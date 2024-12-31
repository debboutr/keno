from sqlalchemy import func
from collections import Counter
from collections import defaultdict
from timeit import default_timer as timer
from datetime import datetime, timedelta
from typing import Annotated, Optional
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select
from .colors import color_ramp

class Keno(SQLModel, table=True):
    DrawNumber: int = Field(default=None, primary_key=True)
    DrawDateTime: Optional[datetime]
    BullsEye: int
    Multiplier: str
    WinningNumbers: str

sqlite_file_name = "mydatabase.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, connect_args=connect_args)

def get_session():
    with Session(engine) as session:
        yield session

SessionDep = Annotated[Session, Depends(get_session)]

app = FastAPI()

app.mount("/static", StaticFiles(directory="app/static"), name="static")

templates = Jinja2Templates(directory="app/templates")


def repare (records):
    subsets = len(records), len(records) // 4, len(records) // 10
    divs = ("HIGH", "MID", "LOW")
    # print(f"{subsets=}")
    tot = {"stats": {}, "numbers": []}
    keeper = {str(num): {j: None for j in divs} for num in range(1, 81)}
    # print(f"{len(data)=}")
    for epoch, subset in zip(divs, subsets):
        free = "!".join([x for x in records[:subset]])
        these = free.split("!")
        todos = dict(Counter(these).most_common())
        l = []
        for rank, key in enumerate(todos.keys()):
            # print(f"{todos[key]=}")
            # print(f"{len(these)=}")
            pct = f"{(todos[key] / len(these)) * 100}"
            good = dict(percent=pct, rank=rank, color=color_ramp[rank], sum=todos[key])
            keeper[key][epoch] = good

    # print(f"{subsets=}")
    tot["numbers"].append(keeper)
    return tot

def prepare (records):
    l = list()
    these = records.split("!")
    todos = dict(Counter(these).most_common())
    for rank, key in enumerate(todos.keys()):
        pct = f"{(todos[key] / len(records)) * 100}"
        l.append({key: dict(percent=pct, rank=rank, color=color_ramp[rank], sum=todos[key])})
    return l


@app.get("/", response_class=HTMLResponse)
def index(request: Request, session: SessionDep):
    t = datetime.now() - timedelta(weeks=4)
    heroes = session.exec(select(Keno).where(Keno.DrawDateTime > t))
    records = [hero.WinningNumbers for hero in heroes]
    data = repare(records)
    return templates.TemplateResponse(
            request=request, name="item.html", context={"charge": data})


@app.get("/heroes/")
def read_heroes(
    session: SessionDep,
    offset: int = 0,
    limit: Annotated[int, Query(le=100)] = 100,
) -> dict:
    """This is REALLY slow when selecting ALL!!"""
    t = datetime.now() - timedelta(weeks=4)
    print(f"{t=}")
    start = timer()
    # you might also just do a limit here on total records descending
    heroes = session.exec(select(Keno).where(Keno.DrawDateTime > t))
    print(f"db query: {(timer() - start)=}")
    start = timer()
    records = [hero.WinningNumbers for hero in heroes]
    print(f"{len(records)=}")
    print(f"{records[0]=}")
    out = "!".join(records)
    subsets = len(records) // 4, len(records) // 10
    print(f"{subsets=}")
    data=prepare(out) # 80 numbers of len
    charge = {"HIGH": dict(records=len(out), data=data)}
    print(f"{len(data)=}")
    for epoch, subset in zip(("MID", "LOW"), subsets):
        print(f"{subset=}")
        print(f"{len(records[:subset])=}")
        data = prepare("!".join([x for x in records[:subset]]))
        print(f"{len(data)=}")
        charge[epoch] = dict(records=subset, data=data)
    print(f"prepare: {(timer() - start)=}")
    print(f"{len(out)=}")
    return charge


@app.get("/items/{id}", response_class=HTMLResponse)
async def read_item(request: Request,
                    session: SessionDep,
                    id: str):
    t = datetime.now() - timedelta(weeks=4)
    print(f"{t=}")
    start = timer()
    heroes = session.exec(select(Keno).where(Keno.DrawDateTime > t).limit(1000))
    records = [hero.WinningNumbers for hero in heroes]
    data = repare(records)
    return data
