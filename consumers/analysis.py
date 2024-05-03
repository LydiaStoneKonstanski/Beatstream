from sqlalchemy import Column, Integer, String, DECIMAL, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
import os

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    current_song = (String(250))

    def __init__(self, id, current_song):
        self.id = id
        self.current_song = current_song

class Recommendation(Base):
    __tablename__ = 'recommendations'
    id = Column(Integer, primary_key=True)
    userID = Column(Integer)
    modelID = Column(Integer)
    songID = Column(String(250))
    model_score = Column(DECIMAL)

    def __init__(self, userID, modelID, songID, model_score):
        self.userID = userID
        self.modelID = modelID
        self.songID = songID
        self.model_score = model_score

db_path = os.path.realpath('../data/beatstream.sqlite')
engine = create_engine(f'sqlite:///{db_path}')
Base.metadata.create_all(engine)

session = Session(bind=engine)
