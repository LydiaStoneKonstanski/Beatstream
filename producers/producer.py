from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
from connections.million_connection import MillionConnection, Track
import random
'''
There should be functions to pick a current song based on features such as the following:

1) pick a random song as we currently are doing. 
2) pick a random song by the same artist.
3) pick a a random song with a matching mb tag or term (best approximation of genre). 
4) pick a random song from the same decade. 
5) pick a random song from similar artist using similarity table compared to current song. 
6) functions to support using machine learning, large-language, and AI models. 

We may want to have a few functions more than are included in our consumer models so that our 
users are more complex than the models to help us train data sets and give AI something to pick up on. 

Next part of the user is that each user could have different percent preferences for 
different song-pick functions. Those preferences could change from time to time. 
Such as: 

1) Listening in 5 song batches before choosing another tag/artist etc. 
2) Cyclical behavior of things changing over time, yet sped up so you can see it on the dashboard. 
Done in a way where it's not averaged out by having too many users with conflicting behaviors. 
Like oldies and low-fi all day and party music at night for instance. Then you could say Model A
is great in the morning, but Model C is more correct in the afternoon. 

STRETCH GOAL: 
Add a Kafka event for users logging on and logging off so dashboard can show total users and 
currently active users in displays. 
'''
class CurrentSongProducer:

    def __init__(self):
        print("Initializing CurrentSongProducer")
        self.million_connection = MillionConnection(local=True)
        self.million_session = self.million_connection.session
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: dumps(m).encode('ascii'))
        print("Initialization Complete")

    def makeMessages(self):
        count = 0
        for i in range(1000):
            for userID in range(1,11):
                track_id = self.get_random_song()
                data = {'userID': userID, 'trackID': track_id}
                self.producer.send('user_current_song', value=data)
                count += 1
            print(f"Sent {count} total messages")
            sleep(2)

    def get_random_song(self):
        index=random.randint(1,1000000)
        # query on table Track, filtering down to matching index, and because index is unique there
        # should only be one result so we can take the first result of the query.
        track = self.million_session.query(Track).filter(Track.index == index).first()
        return track.track_id



if __name__ == "__main__":
    c = CurrentSongProducer()
    c.makeMessages()










