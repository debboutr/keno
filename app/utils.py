from datetime import datetime, timedelta
from sqlite3 import PARSE_COLNAMES, PARSE_DECLTYPES, connect

import pandas as pd
import requests

URL = (
    "https://api2.oregonlottery.org/keno/"
    "ByDrawDate?startingDate={}&endingDate={}"
    "&pageSize=600"
)
SUBKEY = "683ab88d339c4b22b2b276e3c2713809"
HEADERS = {"ocp-apim-subscription-key": SUBKEY}


def scrape_keno(day):
    # print(f"URL = {URL.format(day1, day2)}")
    day1 = day.strftime("%m-%d-%Y")
    if day1 == datetime.now().strftime("%m-%d-%Y"):
        day2 = day1
    else:
        day2 = (day + pd.Timedelta(days=1)).strftime("%m-%d-%Y")
    print(f"Collecting : {day1}...{day2}")
    r = requests.get(URL.format(day1, day2), headers=HEADERS)
    out = pd.json_normalize(r.json())
    out.WinningNumbers = out.WinningNumbers.apply(lambda x: "!".join(map(str, x)))
    out = out[
        ["DrawDateTime", "DrawNumber", "BullsEye", "Multiplier", "WinningNumbers"]
    ]
    out.DrawDateTime = pd.to_datetime(out.DrawDateTime, format="%Y-%m-%dT%H:%M:%S")
    return out


def retrieve():
    # recent  = squeal()
    detect_types = PARSE_DECLTYPES | PARSE_COLNAMES
    with connect("mydatabase.db", detect_types=detect_types) as conn:
        recent = pd.read_sql_query(
            """SELECT * FROM keno
                ORDER BY DrawDateTime DESC
                LIMIT 1""",
            conn,
        ).loc[0]
        tbl = scrape_keno(recent.DrawDateTime)
        tbl = tbl[tbl.DrawNumber > recent.DrawNumber]
        tbl.to_sql("keno", conn, if_exists="append", index=False)
        print(f"Records added: {len(tbl)=}")
        conn.commit()
