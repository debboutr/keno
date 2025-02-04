#!/home/rick/code/keno/venv/bin/python
from datetime import datetime, timedelta
from sqlite3 import PARSE_COLNAMES, PARSE_DECLTYPES, connect

import click
import pandas as pd
import requests

URL = "https://api2.oregonlottery.org/keno/\
ByDrawDate?startingDate={}&endingDate={}\
&pageSize=600"

SUBKEY = '683ab88d339c4b22b2b276e3c2713809'
HEADERS = { 'ocp-apim-subscription-key': SUBKEY}

def scrape_keno(day):
    r = requests.get(URL.format(day, day), headers=HEADERS)
    out = pd.json_normalize(r.json())
    out.WinningNumbers = out.WinningNumbers.apply(
            lambda x: "!".join(map(str, x)))
    out = out[['DrawDateTime',
             'DrawNumber',
             'BullsEye',
             'Multiplier',
             'WinningNumbers']]
    out.DrawDateTime = pd.to_datetime(
            out.DrawDateTime,
            format="%Y-%m-%dT%H:%M:%S")
    return out # print(f"{len(out)=}")


def gather_dates(last_recorded_date):
    dates = []
    difference = datetime.today() - last_recorded_date
    for i in list(range(1, difference.days)):
            d = last_recorded_date + timedelta(i)
            date = d.strftime('%m-%d-%Y')
            dates.append(date)
    return dates


@click.command()
def retreive():
    detect_types = PARSE_DECLTYPES | PARSE_COLNAMES
    with connect('mydatabase.db',
                         detect_types=detect_types) as conn:
        df = pd.read_sql_query(
                """SELECT * FROM keno
                ORDER BY DrawDateTime
                DESC LIMIT 1""", conn)
        dates = gather_dates(df.loc[0].DrawDateTime)
        if not dates:
            click.echo("You're all up to date!!!")
            return
        tbl = pd.DataFrame()
        for day in dates:
            records_collected = scrape_keno(day)
            print(f"{day=} || {len(records_collected)=}")
            tbl = pd.concat([tbl, records_collected])
        tbl.to_sql('poopy', conn,
                   if_exists='append', index=False)
        conn.commit()
        sql = """INSERT INTO keno (
                    DrawDateTime,
                    DrawNumber,
                    BullsEye,
                    Multiplier,
                    WinningNumbers)
                SELECT DrawDateTime,
                    DrawNumber,
                    BullsEye,
                    Multiplier,
                    WinningNumbers
                FROM poopy t
                WHERE NOT EXISTS
                    (SELECT 1 FROM keno f
                     WHERE t.DrawNumber = f.DrawNumber)"""
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.execute("""DROP TABLE poopy""")


if __name__ == "__main__":
    retreive()

