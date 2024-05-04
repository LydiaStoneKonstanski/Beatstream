import sqlalchemy
from sqlalchemy import Column, Integer, String, DECIMAL, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
import os

Base = sqlalchemy.orm.declarative_base()

class Analysis(Base):
    __tablename__ = 'analysis'
    index = Column(Integer, primary_key=True)
    duration = Column(DECIMAL)
    end_of_fade_in = Column(DECIMAL)
    key = Column(Integer)
    key_confidence = Column(DECIMAL)
    loudness = Column(DECIMAL)
    mode = Column(Integer)
    mode_confidence = Column(DECIMAL)
    start_of_fade_out = Column(DECIMAL)
    tempo = Column(DECIMAL)
    time_signature = Column(Integer)
    time_signature_confidence = Column(DECIMAL)
    track_id = Column(String(25))

    def __init__(self, index, duration, end_of_fade_in, key,
                 key_confidence, loudness, mode, mode_confidence,
                 start_of_fade_out, tempo, time_signature,
                 time_signature_confidence, track_id):
        self.index = index
        self.duration = duration
        self.end_of_fade_in = end_of_fade_in
        self.key = key
        self.key_confidence = key_confidence
        self.loudness = loudness
        self.mode = mode
        self.mode_confidence = mode_confidence
        self.start_of_fade_out = start_of_fade_out
        self.tempo = tempo
        self.time_signature = time_signature
        self.time_signature_confidence = time_signature_confidence
        self.track_id = track_id

class ArtistMbtag(Base):
    __tablename__ = 'artist_mbtag'
    index = Column(Integer, primary_key=True)
    artist_id = Column(String(25))
    mbtag = Column(String(250))

    def __init__(self, index, artist_id, mbtag):
        self.index = index
        self.artist_id = artist_id
        self.mbtag = mbtag


class ArtistTerm(Base):
    __tablename__ = 'artist_term'
    index = Column(Integer, primary_key=True)
    artist_id = Column(String(25))
    term = Column(String(250))

    def __init__(self, index, artist_id, term):
        self.index = index
        self.artist_id = artist_id
        self.term = term

class Artist(Base):
    __tablename__ = 'artists'
    artist_id = Column(String(25), primary_key=True)

    def __init__(self, artist_id):
        self.artist_id = artist_id

class Similarity(Base):
    __tablename__ = 'similarity'
    index = Column(Integer, primary_key=True)
    target = Column(String(25))
    similar = Column(String(25))

    def __init__(self, index, target, similar):
        self.index = index
        self.target = target
        self.similar = similar
"""
('CREATE TABLE tracks(\n  track_id TEXT,\n  title TEXT,\n  
artist_name TEXT,\n  "release" TEXT,\n  year INT,\n  
duration REAL,\n  song_hotttnesss REAL,\n  artist_hotttnesss REAL,\n  
artist_familiarity REAL,\n  artist_location TEXT,\n  artist_latitude REAL,\n  
artist_longitude REAL,\n  song_id TEXT,\n  artist_id TEXT,\n  
track_7digitalid INT,\n  artist_7digitalid INT,\n  release_7digitalid INT,\n  
artist_mbid TEXT\n)',)
"""
class Track(Base):
    __tablename__ = 'tracks'
    index = Column(Integer, primary_key=True)
    track_id = Column(String(25))
    title = Column(String(300))
    artist_name = Column(String(400))
    release = Column(String(200))
    year = Column(Integer)
    duration = Column(DECIMAL)
    song_hotttnesss = Column(DECIMAL)
    artist_hotttnesss = Column(DECIMAL)
    artist_familiarity = Column(DECIMAL)
    artist_location = Column(String(300))
    artist_latitude = Column(DECIMAL)
    artist_longitude = Column(DECIMAL)
    song_id = Column(String(25))
    artist_id = Column(String(25))
    track_7digitalid = Column(Integer)
    artist_7digitalid = Column(Integer)
    release_7digitalid = Column(Integer)
    artist_mbid = Column(String(40))


    def __init__(self, index, track_id, title, artist_name, release, year, duration,
                 song_hotttnesss, artist_hotttnesss, artist_familiarity,
                 artist_location, artist_latitude, artist_longitude,
                 song_id, artist_id, track_7digitalid, artist_7digitalid,
                 release_7digitalid, artist_mbid):
        self.index = index
        self.track_id = track_id
        self.title = title
        self.artist_name = artist_name
        self.release = release
        self.year = year
        self.duration = duration
        self.song_hotttnesss = song_hotttnesss
        self.artist_hotttnesss = artist_hotttnesss
        self.artist_familiarity = artist_familiarity
        self.artist_location = artist_location
        self.artist_latitude = artist_latitude
        self.artist_longitude = artist_longitude
        self.song_id = song_id
        self.artist_id = artist_id
        self.track_7digitalid = track_7digitalid
        self.artist_7digitalid = artist_7digitalid
        self.release_7digitalid = release_7digitalid
        self.artist_mbid = artist_mbid

class MillionConnection():
    def __init__(self, local=True):
        if local == True:
            self.host = os.environ["host"]
            self.user = os.environ["user"]
            self.password = os.environ["password"]
            self.engine = create_engine(f'mysql://{self.user}:{self.password}@{self.host}/million')
        else:
            # TODO: set up remote connection
            raise NotImplementedError

        Base.metadata.create_all(self.engine)
        self.session = Session(bind=self.engine)


if __name__ == "__main__":
    m = MillionConnection()
    s = m.session

