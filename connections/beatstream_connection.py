import sqlalchemy
from sqlalchemy import Column, Integer, String, DECIMAL, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
import os

Base = sqlalchemy.orm.declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(String(40), primary_key=True)
    current_song = Column(String(250))

    def __init__(self, id, current_song):
        self.id = id
        self.current_song = str(current_song)

class Recommendation(Base):
    __tablename__ = 'recommendations'
    id = Column(Integer, primary_key=True)
    userID = Column(String(40))
    modelID = Column(String(50))
    trackID = Column(String(25))
    model_score = Column(DECIMAL)

    def __init__(self, userID, modelID, trackID, model_score):
        self.userID = userID
        self.modelID = modelID
        self.trackID = trackID
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

    def reset_database(self):
        self.session.query(User).delete()
        print ("Deleted all records from users table")
        self.session.query(Recommendation).delete()
        print("Deleted all records from recommendations table")

        self.session.commit()


if __name__ == "__main__":
    b = BeatstreamConnection()
    s = b.session

    # Uncomment this and run to reset the database:
    #b.reset_database()
