#!/home/rick/miniconda3/envs/keno/bin/python
"""
Sun Aug  7 10:10:45 PM PDT 2022

-- make this a cron job

scrape winning picks from past keno games in OR.
"""

import time
from datetime import date, timedelta

import click
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
    "--start-day",
    "-s",
    default=date.today() - timedelta(days=1),
    help="""Either Date in ISO 8601 format YYYY-MM-DD OR
            Number of days to collect from yesterday.
            - Defaults to yesterday""",
)
@click.option(
    "--end-day",
    "-f",
    default=date.today() - timedelta(days=1),
    help="""Date in ISO 8601 format YYYY-MM-DD
            - Defaults to yesterday's date""",
)
# name of file for output / URL to POST
# @click.argument()
def main(start_day, end_day):
    """Collect keno sessions of past days. without arguments, defaults to
    grabbing all of yesterday's wins."""
    days = None
    times = get_times()
    if start_day != end_day:
        end_day = date.fromisoformat(end_day)
        if "-" in start_day:
            start_day = date.fromisoformat(start_day)
            num = (end_day - start_day).days
        else:
            num = int(start_day)
        days = [end_day - timedelta(i) for i in list(range(num + 1))]
        days.reverse()
    elif not days:
        days = [date.fromisoformat(end_day)]
    browser, date_input, min_time, max_time = start_browser()
    records = []
    cols = ["date", "time", "UID", "powerball", "multiplier", "bonus"] + [
        f"num_{x}" for x in range(1, 21)
    ]
    for day in days:
        print(f"{day.strftime('%-m/%-d/%Y')=}")
        date_input.clear()
        date_input.send_keys(day.strftime("%-m/%-d/%Y"))
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

    out = out[
        ["datetime", "UID", "posix", "powerball", "multiplier", "winners"]
    ]
    d = day.strftime("%-d_%-m_%Y")
    out.sort_values("datetime").to_csv(
        f"~/code/gametheory/daily_data/{d}.csv", index=False
    )
    todo = pd.read_csv("~/code/gametheory/todos.csv")
    todo.datetime = pd.to_datetime(todo.datetime, format="%Y-%m-%d %H:%M:%S")
    tot = pd.concat([todo, out])
    tot.sort_values("datetime", inplace=True)
    assert (len(todo) + len(out)) == len(tot)
    tot.to_csv("~/code/gametheory/todos.csv", index=False)

    browser.quit()


if __name__ == "__main__":
    main()
