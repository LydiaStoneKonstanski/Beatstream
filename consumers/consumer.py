import sqlite3

from kafka import KafkaConsumer
from json import loads
from connections.million_connection import MillionConnection, Track
from connections.beatstream_connection import BeatstreamConnection, User, Recommendation
import random


class CurrentSongConsumer:

    def __init__(self):
        self.beatstream_connection = BeatstreamConnection(local=True)
        self.beat_session = self.beatstream_connection.session
        self.million_connection = MillionConnection(local=True)
        self.million_session = self.million_connection.session
        self.consumer = KafkaConsumer(
            'user_current_song',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: loads(m.decode('ascii'))
        )

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} found'.format(message))
            self.updateUser(message)
            self.recommendSong(message)

    def updateUser(self, message):
        u = User(
            id=message['userID'],
            current_song=str(message['trackID'])
        )
        self.beat_session.merge(u)

        self.beat_session.commit()

    def get_random_song(self):
        index = random.randint(1, 1000000)
        # query on table Track, filtering down to matching index, and because index is unique there
        # should only be one result so we can take the first result of the query.
        track = self.million_session.query(Track).filter(Track.index == index).first()
        return track.track_id

    def recommendSong(self, message):
        # Delete previous recommendations for this user
        self.beat_session.query(Recommendation).filter(Recommendation.userID == message['userID']).delete()
        # Add new recommendations
        r = Recommendation(
            userID=message['userID'],
            modelID=1,
            trackID=self.get_random_song(),
            model_score=random.randint(1, 100)
        )
        self.beat_session.add(r)

        r = Recommendation(
            userID=message['userID'],
            modelID=2,
            trackID=self.get_random_song(),
            model_score=random.randint(1, 100)
        )
        self.beat_session.add(r)

        r = Recommendation(
            userID=message['userID'],
            modelID=3,
            trackID=self.get_random_song(),
            model_score=random.randint(1, 100)
        )
        self.beat_session.add(r)
        self.beat_session.commit()


if __name__ == "__main__":
    c = CurrentSongConsumer()
    c.handleMessages()
