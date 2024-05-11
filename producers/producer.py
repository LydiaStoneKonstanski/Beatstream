from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
from connections.million_connection import MillionConnection, Track
import random
from user_profile import User, UserProfiles, UserProfile
from timeit import default_timer as timer

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
        self.users = self.get_users()
        print("Initialization Complete")

    # def makeMessages(self):
    #     count = 0
    #     for i in range(1000):
    #         for userID in range(1,11):
    #             track_id = self.get_random_song()
    #             data = {'userID': userID, 'trackID': track_id}
    #             self.producer.send('user_current_song', value=data)
    #             count += 1
    #         print(f"Sent {count} total messages")
    #         sleep(2)


    def get_users(self):
        user_profiles = UserProfiles()
        users = []
        for profile in user_profiles.profiles:
            for i in range(10000):
                user = profile.create_user()
                users.append(user)
        return users

    def makeMessages(self):
        count = 0
        for i in range(1000):
            start = timer()

            for user in self.users:
                track_id = self.get_next_track(user)
                data = {'userID': user.user_id, 'trackID': track_id}
                self.producer.send('user_current_song', value=data)
                count += 1
            sleep(20)
            end = timer()
            print(f"Sent {count} total messages. Took {end - start} seconds.")


    def get_next_track(self, user):
        current_track_id = user.current_track_id
        rand = random.random()
        i = 0
        while user.profile.cumulative[i] <= rand:
            i += 1

        match i:
            case 0:
                return None
            case 1:
                return self.million_connection.get_random_track_id()
            case 2:
                return self.million_connection.get_same_artist_track_id(current_track_id)
            case 3:
                return self.million_connection.get_same_decade_track_id(current_track_id)





if __name__ == "__main__":
    c = CurrentSongProducer()
    c.makeMessages()










