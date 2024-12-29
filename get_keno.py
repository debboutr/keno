#!/home/rick/code/gametheory/venv/bin/python
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

# WORK BELOW -- ANALYSIS
# import pandas as pd
# import sqlite3
#
# ## 00
# # GUESSES-lowest: 7, 24, 80
# # GUESSES-highest: 4, 9, 47 win@32  dbl@56@00
#
# ## 01
# # GUESSES-lowest: 16, 17, 78 dbl@12@40 win@52
# # GUESSES-highest: 7, 23, 45 dbl@04@16@20
#
# ## 02
# # GUESSES-lowest: 12, 51, 62 dbl@12
# # GUESSES-highest: 24, 50, 70 db@00
#
# ## 03
# # GUESSES-lowest: 39, 46, 62 dbl@00@16@40@56
# # GUESSES-highest: 10, 37, 52 dbl@00@32@52
#
# """
# @$1 -- 27 + 2 + 2 = 31 paid: $15 DOUBLE!!
# -- strategies: 3 most picked / 3 least picked
# -- maintain guesses for each strategy and track their success rate -> publish
# -- endpoint for each strategie's efficacy
# -- track the indexes of the Counter.most_common() 
# [idx for idx, (val, _) in enumerate(bb) if val in wins]
#
#
# -- go back n days(5) find 3 most common / least common
# -- track the hours that they win
# -- look for common hours
# """
# todo = pd.read_csv("todos.csv")
# todo.drop_duplicates("datetime").to_csv("todos.csv", index=False)
#
# todo.datetime = pd.to_datetime(todo.datetime, format="%Y-%m-%d %H:%M:%S")
# todo = todo.set_index('datetime')
#
#
# aa = todo.groupby(todo.datetime.dt.date).winners.count()
# aa.idxmin()
# aa[aa>222].min()
#
# todo["posix_diff"] = todo["posix"].diff()
# todo.loc[(2000 < todo.posix_diff) & (todo.posix_diff < 9361)].head(30)
# todo.loc[(9359 < todo.posix_diff) & (todo.posix_diff < 9361)]
# todo.iloc[375767:375770]
# todo.iloc[186331:186333]
#
#
# new = pd.read_csv("keep.csv")
# qq = pd.concat([todo, new])
# qq.drop_duplicates("posix")
# qq.drop_duplicates("UID", inplace=True)
# qq.sort_values("posix").to_csv("todos.csv", index=False)
#
# def foo():
#     con = duckdb.connect('db.duckdb')
#     # a=con.execute("SELECT string_agg(winners, '!') as numbers from pulls where date_part('day', datetime) = 2")
#     # a=con.execute("SELECT string_agg(winners, '!') as numbers from pulls LIMIT 300")
#     a=con.execute("SELECT string_agg(winners, '!') FROM (select * from pulls ORDER BY UID DESC LIMIT 20)")
#     # a=con.execute("SELECT string_agg(winners, '!') as numbers from push where date_part('hour', datetime) = 7 and date_part('minute', datetime)=32")
#     b = a.fetchone()
#     d = b[0].split('!')
#     c = Counter(d)
#     con.close()
#     return c
#
#
# from collections import Counter
# from datetime import datetime as dt
# from datetime import timedelta
#
# def deliver_frames(df, num=5):
#     latest_day = df.index.max()
#     for i in range(num, 0, -1):
#         here = latest_day - timedelta(days=i)
#         yield (df.loc[df.index.date < here.date()].copy(),
#                df.loc[df.index.date == here.date()].copy())
#
# helper = deliver_frames(todo)
# for x, y in helper:
#     print(len(x),": ", len(y))
# for x, y in deliver_frames(todo):
#     print(x.index.max(),": ", y.index.min())
#
# def chop(x):
#     return x.split("!")
#
#
# def find_common_indexes(learn, test):
#     for df in bb:
#     # print(df)
#     df["total"] = (df.dbl*2) + (df.bingo*27)
#     print(df.total.max())
#     print(df.total.idxmax())
#     """get most_common / least common of learn
#     -- return what hours the wins are in
#     DataFrame of hours:
#         index bingo double
#         0     0     1
#         1     0     0
#         2     0     0
#         3     ...
#         23
#     """
#     chx = Counter(learn.winners.apply(chop).explode()).most_common()
#     aa = pd.DataFrame([[0,0]], index=range(24), columns=["bingo","dbl"])
#     for idx, group in test.groupby(test.index.hour):
#         # break
#         hr, dow = group.index.hour[0], group.index.dayofweek[0]
#         print(hr)
#         for won in group.winners.apply(chop):
#             # print(won)
#             out = [hit for hit, _ in chx[-3:] if hit in won]
#             if len(out) == 2:
#                 aa.iloc[hr].dbl += 1
#             if len(out) == 3:
#                 aa.iloc[hr].bingo += 1
#     return aa
#
# bb = []
# for x, y in deliver_frames(todo, num=20):
#     bb.append(find_common_indexes(x, y))
#
# for df in bb:
#     # print(df)
#     df["total"] = (df.dbl*2) + (df.bingo*27)
#     print(df.total.max())
#     print(df.total.idxmax())
#
# sum([df.total[9] for df in bb])
#
#     # print(df)
#     df["total"] = (df.dbl*2) + (df.bingo*27)
#     print(df.total.max())
#     print(df.total.idxmax())
#     
#     
# def first_run():
#     """Subset out one day
#     -- aggregate the others to get by hour and day_of_week
#     """
#     tester = dt(2022, 12, 27)
#     c = Counter(new.winners.apply(chop).explode()).most_common()
#     todo.loc[(todo.datetime.dt.dayofweek == 3) & (todo.datetime.dt.hour == 7)]
#     todo.loc[todo.index < "2022-12-26"]
#     test = todo.loc[todo.index > tester]
#     learn = todo.loc[todo.index < tester]
#     # groupby hour of test
#     total = 0
#     res = {}
#     for idx, group in test.groupby(test.index.hour):
#         # break
#         hr, dow = group.index.hour[0], group.index.dayofweek[0]
#         lrn = learn.loc[
#             (learn.index.hour == hr) & (learn.index.day_of_week == dow),
#             "winners"]
#         chx = Counter(lrn.apply(chop).explode()).most_common()
#         # s = []
#         res[hr] = dict(upper=0, downer=0)
#         for won in group.winners.apply(chop):
#             # print(won)
#             # s += [idx for idx, (w, _) in enumerate(chx) if w in won]
#             # Counter(s).most_common()[:3]
#             aa = len(chx)//2
#             tmb = [chx[0],chx[-1],chx[len(chx)//2]]
#             # out = [hit for hit, _ in chx[:3] if hit in won]
#             out = [hit for hit, _ in chx[aa-2:aa+1] if hit in won]
#             out = [hit for hit, _ in tmb if hit in won]
#             if len(out) == 2:
#                 total += 2
#             if len(out) == 3:
#                 total += 27
#                 res[hr]["upper"] += 1
#                 print(out)
#
#
#
#
#
# gg = gg[["UID"] + [f"num_{x}" for x in range(1, 21)]]
# gg = gg[["datetime", "UID"] + [f"num_{x}" for x in range(1, 21)]].head()
#
# tt["winners"] = tt.apply(lambda x: "!".join(x[6:].astype(str)), axis=1)
# tt["datetime"] = pd.to_datetime(
#     tt.date + " " + tt.time, format="%m/%d/%Y %I:%M %p"
# )
# tt["posix"] = tt.datetime.apply(lambda x: int(x.timestamp()))
#
# conn = sqlite3.connect("sample.db")
# tt.to_sql("keno", conn, if_exists="replace")
#
# tt = tt[["posix", "UID", "winners"]]
# tt = tt.sort_values("posix", ascending=True).reset_index(drop=True)
#
#
# gg[[f"num_{x}" for x in range(1, 21)]].value_counts()
# vv = gg[[f"num_{x}" for x in range(1, 21)]].values
# vv.shape
#
# (vv == 8).sum()
#
# from itertools import chain
# import collections
# from collections import Counter
#
# cols = ["date", "time", "UID", "powerball", "multiplier", "bonus"] + [
#     f"num_{x}" for x in range(1, 21)
# ]
#
#
#
# ff = pd.read_csv("junk.csv")
#
#
# tot.loc[tot.UID.isnull()]
# tot.UID.isnull()
# tot.set_index("UID", drop=True)
# tot = tot.set_index("UID", drop=True)
# tot.sort_index()
# tot.sort_index(ascending=False)
# tot = tot.sort_index(ascending=False)
# tot.drop_duplicates()
# tot = tot.drop_duplicates()
# tot.reset_index()
# tot.loc[[f"num_{x}" for x in range(1, 21)]].astype(int)
# tot[[f"num_{x}" for x in range(1, 21)]].astype(int)
# tot.num_1.isnull()
# tot.num_1.isnull().any()
# tot.num_2.isnull().any()
# [tot[f"num_{x}"].isnull().any() for x in range(1, 21)]
#
# tot[[f"num_{x}" for x in range(1, 21)]].astype(int)
# {f"num_{x}": int for x in range(1, 21)}
# tot.astype({f"num_{x}": int for x in range(1, 21)})
# tot = tot.astype({f"num_{x}": int for x in range(1, 21)})
# tot.head()
# tot.to_csv("todos.csv", index=False)
#
# conn = sqlite3.connect("sample.db")
#
# cursor = conn.cursor()
#
# cursor.execute(
#     """
#     select winners, strftime('%w',datetime(posix, 'unixepoch')) as dow
#     from keno where dow = '4';
#     """
# )
#
# cursor.execute(
#     """
#     select winners,
#     strftime('%w',datetime(posix, 'unixepoch')) as dow,
#     strftime('%M',datetime(posix, 'unixepoch')) as min
#     from keno where dow = '4' and min = '24';
#     """
# )
#
#
# fetchedData = cursor.fetchall()
# keep = []
# for row in fetchedData:
#     keep = keep + [int(r) for r in row[0].split("!")]
#
#         """
#         select winners, strftime('%w',datetime(posix, 'unixepoch')) as dow
#         from keno where dow = '4';
#         """
# pp = "datetime(posix, 'unixepoch')"
# def teste():
#     conn = sqlite3.connect("sample.db")
#     df = pd.read_sql_query(
#     """
#     select winners,
#     strftime('%w', {0}) as dow,
#     strftime('%H', {0}) as hour
#     from keno where dow = '4' and hour = '11';
#     """.format(pp)
#         , conn)
#     s = df.winners.apply(lambda x: [int(y) for y in x.split("!")]).explode()
#     return Counter(s)
#
