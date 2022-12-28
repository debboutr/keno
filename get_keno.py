"""
Sun Aug  7 10:10:45 PM PDT 2022

-- make this a cron job

scrape winning picks from past keno games in OR.
"""

import csv
import click
import sqlite3
import time
from datetime import date, timedelta
import pandas as pd

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def to_iso(t):
    s = 2
    return t[:s] + ":" + t[s:-s] + " " + t[-s:]


def format_time(b, e):
    return [(to_iso(b), to_iso(e))]


def get_times():
    times = []
    early = list(range(2, 13, 2))
    early.insert(0, early.pop())
    late = list(range(1, 12, 2))
    for m in ["AM", "PM"]:
        for x, y in zip(early, late):
            times.append((f"{x}:00 {m}", f"{y}:59 {m}"))
    return times


def get_by_id(name, driver):
    return WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (
                By.ID,
                name,
            )
        )
    )


def start_browser():
    print("starting browser")
    options = Options()
    options.headless = True
    # service = Service("/app/geckodriver")
    service = Service("/home/rick/.local/bin/geckodriver")
    browser = webdriver.Firefox(options=options, service=service)
    kenoURL = "https://www.oregonlottery.org/keno/winning-numbers/"
    browser.get(kenoURL)
    date_input = get_by_id("date-min", browser)
    min_time = get_by_id("time-min", browser)
    max_time = get_by_id("time-max", browser)
    return browser, date_input, min_time, max_time


def rip_time(soup):
    """ """
    rows = soup.find_all("tr")
    winners = [ro.find_all("td") for ro in rows[1:-1]][::-1]
    infos = []
    for win in winners:
        if not win:
            continue
        date_control = win[0].get_attribute_list("class")
        if "datepicker" in date_control[0]:
            # only rows with winner data
            continue
        num_cell = [w.text for w in win]
        if not len(num_cell) > 1:
            # no numbers in the cell???
            continue
        nums = num_cell.pop(3).split("\xa0")
        infos.append(num_cell + nums)
    return infos


@click.command()
@click.option(
    "--begin",
    "-b",
    default=None,
    help="""Hour, minute and meridian of time to end search
        \tExample: `1204AM`""",
)
@click.option(
    "--end",
    "-e",
    default=None,
    help="""Hour, minute and meridian of time to end search
        \tExample: `1159PM`""",
)
@click.option(
    "--start-day",
    "-s",
    default=date.today(),
    help="""Date in ISO 8601 format YYYY-MM-DD
            - Defaults to today""",
)
@click.option(
    "--end-day",
    "-f",
    default=date.today(),
    help="""Number of days to collect from `date` option
            - Defaults to todays date""",
)
# name of file for output / URL to POST
# @click.argument()
def main(begin, end, start_day, end_day):
    if begin:
        # this needs to get split into hours of 2
        times = format_time(begin, end)
    else:
        times = get_times()
    if start_day != end_day:
        start_day = date.fromisoformat(start_day)
        end_day = date.fromisoformat(end_day)
        num = (end_day - start_day).days
        days = [end_day - timedelta(i) for i in  list(range(num+1))]
    else:
        days = [date.fromisoformat(end_day)]
    browser, date_input, min_time, max_time = start_browser()
    records = []
    cols = ["date", "time", "UID", "powerball", "multiplier", "bonus"] + [
        f"num_{x}" for x in range(1, 21)
    ]
    for day in days:
        day = day.strftime("%-m/%-d/%Y")
        print(f"{day=}")
        date_input.clear()
        date_input.send_keys(day)
        date_input.send_keys(Keys.RETURN)
        browser.find_element(By.XPATH, "//body").click()
        for start, end in times:
            min_time.clear()
            min_time.send_keys(start)
            max_time.clear()
            max_time.send_keys(end)
            max_time.send_keys(Keys.RETURN)
            time.sleep(1)
            soup = BeautifulSoup(browser.page_source, "html.parser")
            for record in rip_time(soup):
                records.append(record)
    out = pd.DataFrame(records, columns=cols)

    out["datetime"] = pd.to_datetime(
            out.date + " " + out.time, format="%m/%d/%Y %I:%M %p"
            )
    out["winners"] = out.apply(lambda x: "!".join(x[6:-1].astype(str)), axis=1)
    out["posix"] = out.datetime.apply(lambda x: int(x.timestamp()))
    out = out[["UID", "posix", "winners", "powerball", "multiplier", "bonus"]]

    conn = sqlite3.connect("check.db")
    old = pd.read_sql("select * from keno;", conn, index_col="UID")
    out.set_index("UID", inplace=True)
    out = pd.concat([old, out])
    out = out[~out.index.duplicated()]
    out.to_sql("keno", conn, index=False, index_label="UID", if_exists="replace")
    breakpoint()

    browser.quit()


if __name__ == "__main__":
    main()
