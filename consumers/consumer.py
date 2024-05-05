import sqlite3

from kafka import KafkaConsumer
from json import loads
from connections.million_connection import MillionConnection, Track
from connections.beatstream_connection import BeatstreamConnection, User, Recommendation
import random

''' Interacts with  both databases.
In total for consumers we need to establish a scoring system, and have a variety of models that use 
the current song to generate a recommendation. 

Example models could be: 
1) pick a random song as we currently are. 
2) pick a random song by the same artist.
3) pick a a random song with a matching mb tag or term (best approximation of genre). 
4) pick a random song from the same decade. 
5) pick a random song from similar artist using similarity table compared to current song. 
6) machine learning model. How do we use cross training to update and get smarter predictions? 
7) AI model. 
8) Large-language model. Though we do not currently have lyric table in our dataset. 

Example scoring system: 
1) 10 points for guessing exact track. 
2) 5 points for guessing correct artist.
3) 2 points for guessing each matching mb tag or term. 
4) 2 points for correct decade. 
5) 3 points for similar artist. 

STRETCH GOAL: 
Raft
'''
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
    '''Currently updates user table and recommends a song using each predictive model'''
    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} found'.format(message))
            self.updateUser(message)
            self.recommendSong(message)

    '''Shows current song for user.'''
    def updateUser(self, message):
        u = User(
            id=message['userID'],
            current_song=str(message['trackID'])
        )
        self.beat_session.merge(u)

        self.beat_session.commit()

    '''Placeholder function. In future will have different functions for each predictive model.         
    Query on table Track, filtering down to matching index, and because index is unique there
    should only be one result so we can take the first result of the query.'''
    def get_random_song(self):
        index = random.randint(1, 1000000)
        track = self.million_session.query(Track).filter(Track.index == index).first()
        return track.track_id

    '''Recommend song adds one row per model for each user with recommended next song and total score for predictive model. 
    right now that score is a random placeholder.'''
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
        '''beat_session is our connection to the beatstream database.'''
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
