from sqlalchemy import Column, DateTime, Integer, String

from .database import Base


class Pull(Base):
    __tablename__ = "pulls"

    datetime = Column(DateTime)
    uid = Column(Integer, unique=True, primary_key=True)
    posix = Column(Integer)
    powerball = Column(Integer)
    multiplier = Column(String)
    winners = Column(String)
