import sqlalchemy
from sqlalchemy import Column, Integer, String, DECIMAL, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
import os

Base = sqlalchemy.orm.declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    current_song = Column(String(250))

    def __init__(self, id, current_song):
        self.id = id
        self.current_song = str(current_song)

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

class BeatstreamConnection():
    def __init__(self, local=True):
        if local == True:
            self.host = os.environ["host"]
            self.user = os.environ["user"]
            self.password = os.environ["password"]
            self.engine = create_engine(f'mysql://{self.user}:{self.password}@{self.host}/beatstream')
        else:
            # TODO: set up remote connection
            raise NotImplementedError

        Base.metadata.create_all(self.engine)
        self.session = Session(bind=self.engine)


