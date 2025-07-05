"""
@$1 -- 27 + 2 + 2 = 31 paid: $15 DOUBLE!!
-- strategies: 3 most picked / 3 least picked
-- maintain guesses for each strategy and track their success rate -> publish
-- endpoint for each strategie's efficacy
-- track the indexes of the Counter.most_common()
[idx for idx, (val, _) in enumerate(bb) if val in wins]


-- go back n days(5) find 3 most common / least common
-- track the hours that they win
-- look for common hours
"""

from collections import OrderedDict, Counter
from sqlite3 import PARSE_COLNAMES, PARSE_DECLTYPES, connect

import numpy as np
import pandas as pd


def lasso(df, n):
    recs = "!".join(df.head(n).WinningNumbers.tolist()).split("!")
    return dict(Counter(recs).most_common())


def index_keys(df, n):
    hold = dict()
    common = lasso(df, n)
    # index of each number in order 1-80
    for num in range(1, 81):
        s = str(num)
        hold[s] = list(common.keys()).index(s)
    return hold


detect_types = PARSE_DECLTYPES | PARSE_COLNAMES
with connect("mydatabase.db", detect_types=detect_types) as conn:
    df = pd.read_sql_query(
        """SELECT * FROM keno
            ORDER BY DrawDateTime
            DESC LIMIT 1200""",
        conn,
    )
    df = pd.read_sql_query(
        """SELECT * FROM keno
            WHERE DrawDateTime < '2025-04-06'
            ORDER BY DrawDateTime
            DESC LIMIT 1200""",
        conn,
    )


def order_indexes():
    large = index_keys(df, 1200)
    medio = index_keys(df, 600)
    small = index_keys(df, 100)
    # sum the indexes of each subset
    rank = dict()
    for num in range(1, 81):
        s = str(num)
        rank[s] = large[s] + medio[s] + small[s]

    # Creates a sorted dictionary (sorted by key)
    keys = list(rank.keys())
    values = list(rank.values())
    sorted_value_index = np.argsort(values)
    sorted_dict = {keys[i]: values[i] for i in sorted_value_index}
    return sorted_dict

    tbl = pd.read_sql_query(
        """SELECT * FROM keno
            WHERE DrawDateTime BETWEEN '2025-04-06 00:00:00' AND  '2025-04-06 23:59:00'""",
        conn,
    )

win = list(sorted_dict.keys())[-3:]


def squeal(db, limit=1200, order='DrawDateTime', day1=None, day2=None):
order='DrawDateTime'
    detect_types = PARSE_DECLTYPES | PARSE_COLNAMES
    if start:
        pass
    if end:
        pass
    with connect("mydatabase.db",
                 detect_types=detect_types) as conn:
        df = pd.read_sql_query(
            """SELECT * FROM keno
                ORDER BY DrawDateTime DESC
                LIMIT 1200""",
            conn,
        )
        df = pd.read_sql_query(
            """SELECT * FROM keno
                WHERE DrawDateTime < '2025-04-06'
                ORDER BY DrawDateTime DESC
                LIMIT 1200""",
            conn,
        )


s = f"ORDER BY DrawDateTime DESC LIMIT {order}"

count = 0
for idx, row in tbl.iterrows():
    a = row.WinningNumbers
    if sum([x in a for x in win]) == 3:
        count += 27
        # print(row)
    if sum([x in a for x in win]) == 2:
        print("win")
        count += 2

print(count)

# # OLD
# aa = todo.groupby(todo.datetime.dt.date).winners.count()
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
#         df["total"] = (df.dbl*2) + (df.bingo*27)
#         print(df.total.max())
#         print(df.total.idxmax())
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
#     sum([df.total[9] for df in bb])
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
# conn = connect("sample.db")
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
# conn = connect("sample.db")
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
#     conn = connect("sample.db")
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
