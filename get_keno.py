#!/home/rick/code/gametheory/venv/bin/python
"""
Sun Aug  7 10:10:45 PM PDT 2022

xx make this a cron job

scrape winning picks from past keno games in OR.
"""

import os
import time
from datetime import date, datetime, timedelta

import click
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

cwd = os.getcwd()
cols = ["date", "time", "UID", "powerball", "multiplier", "bonus"]
cols += [f"num_{x}" for x in range(1, 21)]

kdate = "%m/%d/%Y %I:%M %p"

yesterday = date.today() - timedelta(days=1)

# lambda v: "!".join(v[6:-1].astype(str))
def bangit(v):
    __import__('pdb').set_trace()
    return "!".join(v[6:-1].astype(str))


def get_times():
    hrs = zip([12, 2, 4, 6, 8, 10], [1, 3, 5, 7, 9, 11])
    return [
        (f"{top}:00 {m}", f"{btm}:59 {m}") for top, btm in hrs for m in ["AM", "PM"]
    ]


def get_last_date(file, how_many_last_lines=2):
    with open(file, 'r') as file:
        end_of_file = file.seek(0,2)
        file.seek(end_of_file)             
        for num in range(end_of_file+1):            
            file.seek(end_of_file - num)    
            last_line = file.read()
            if last_line.count('\n') == how_many_last_lines: 
                iso_date = last_line.strip("\n").split(",")[0].split()[0]
                return date.fromisoformat(iso_date) + timedelta(1)

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
    options.add_argument("-headless")
    browser = webdriver.Firefox(options=options)
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


def format_records(recs: list) -> pd.DataFrame:

    out = pd.DataFrame(recs, columns=cols)
    out["datetime"] = pd.to_datetime((out.date + " " + out.time), format=kdate)
    out["winners"] = out.apply(bangit, axis=1)
    out["posix"] = out.datetime.apply(lambda x: int(x.timestamp()))

    return out[["datetime", "UID", "posix", "powerball", "multiplier", "winners"]]


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
@click.option(
    "--last-day",
    "-l",
    default=False,
    is_flag=True,
    help="""Finds last day of 'todos.csv', starts there""",
)
# name of file for output / URL to POST
# @click.argument()
def main(start_day, end_day, last_day):

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
    elif last_day:
        end_day = date.fromisoformat(end_day)
        start_day = get_last_date(f"{cwd}/todos.csv")
        num = (end_day - start_day).days
        days = [end_day - timedelta(i) for i in list(range(num + 1))]
        days.reverse()
    elif not days:
        days = [date.fromisoformat(end_day)]
    browser, date_input, min_time, max_time = start_browser()
    out = pd.DataFrame()
    for day in days:
        records = []
        dayt = day.strftime("%-m/%-d/%Y")
        date_input.clear()
        date_input.send_keys(dayt)
        date_input.send_keys(Keys.RETURN)
        browser.find_element(By.XPATH, "//body").click()
        time.sleep(1.347)
        for start, end in times:
            min_time.clear()
            min_time.send_keys(start)
            max_time.clear()
            max_time.send_keys(end)
            max_time.send_keys(Keys.RETURN)
            time.sleep(1.347)
            soup = BeautifulSoup(browser.page_source, "html.parser")
            for record in rip_time(soup):
                records.append(record)
        recs = format_records(records)
        print(dayt, f"{len(recs)=}")
        d = day.strftime("%-m_%-d_%Y")
        recs.sort_values("datetime").to_csv(f"{cwd}/daily_data/{d}.csv", index=False)
        out = pd.concat([recs, out])
    todo = pd.read_csv(f"{cwd}/todos.csv")
    todo.datetime = pd.to_datetime(todo.datetime, format="%Y-%m-%d %H:%M:%S")
    tot = pd.concat([todo, out])
    tot.sort_values("datetime", inplace=True)
    tot.drop_duplicates("datetime", inplace=True)
    tot.to_csv(f"{cwd}/todos.csv", index=False)
    browser.quit()


if __name__ == "__main__":
    main()
