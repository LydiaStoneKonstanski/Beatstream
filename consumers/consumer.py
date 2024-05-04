import sqlite3

from kafka import KafkaConsumer
from json import loads
from connections.beatstream_connection import BeatstreamConnection, User, Recommendation
import random


class CurrentSongConsumer:

    def __init__(self):
        self.beatstream_connection = BeatstreamConnection(local=True)
        self.beat_session = self.beatstream_connection.session
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
            current_song=str(message['songID'])
        )
        self.beat_session.merge(u)

        self.beat_session.commit()

    def recommendSong(self, message):
        # Delete previous recommendations for this user
        self.beat_session.query(Recommendation).filter(Recommendation.userID == message['userID']).delete()
        # Add new recommendations
        r = Recommendation(
            userID=message['userID'],
            modelID=1,
            songID=random.randint(1, 1000),
            model_score=random.randint(1, 100)
        )
        self.beat_session.add(r)

        r = Recommendation(
            userID=message['userID'],
            modelID=2,
            songID=random.randint(1, 1000),
            model_score=random.randint(1, 100)
        )
        self.beat_session.add(r)

        r = Recommendation(
            userID=message['userID'],
            modelID=3,
            songID=random.randint(1, 1000),
            model_score=random.randint(1, 100)
        )
        self.beat_session.add(r)
        self.beat_session.commit()


if __name__ == "__main__":
    c = CurrentSongConsumer()
    c.handleMessages()
