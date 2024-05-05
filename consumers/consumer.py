
import cProfile
import pstats

from kafka import KafkaConsumer
from json import loads
from connections.million_connection import MillionConnection, Track
from connections.beatstream_connection import BeatstreamConnection, User, Recommendation
from score_mechanic import ScoreMechanic
from predictive_models import PredictiveModels

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
        self.score_mechanic = ScoreMechanic(self.million_connection)
        self.predictive_models = PredictiveModels(self.million_connection)
    '''Currently updates user table and recommends a song using each predictive model'''
    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} found'.format(message))
            self.updateUser(message)
            self.scoreModels(message)
            self.recommendSong(message)

            return

    '''Shows current song for user.'''
    def updateUser(self, message):
        u = User(
            id=message['userID'],
            current_song=str(message['trackID'])
        )
        self.beat_session.merge(u)

        self.beat_session.commit()

    def scoreModels(self, message):
        new_track_id = message['trackID']
        user_id = message['userID']
        recommendations = self.beat_session.query(Recommendation).filter(Recommendation.userID == user_id).all()
        for recommendation in recommendations:
            recommended_track_id = recommendation.trackID
            score = self.score_mechanic.get_score(new_track_id, recommended_track_id)
            recommendation.model_score += score
            self.beat_session.add(recommendation)
        self.beat_session.commit()


    '''Recommend song adds one row per model for each user with recommended next song and total score for predictive model. 
    right now that score is a random placeholder.'''
    def recommendSong(self, message):
        new_track_id = message['trackID']
        user_id = message['userID']

        (model_id, track_id) = self.predictive_models.model_a_recommendation(new_track_id)
        self.create_or_update_recommendation(user_id, model_id, track_id)

        (model_id, track_id) = self.predictive_models.model_b_recommendation(new_track_id)
        self.create_or_update_recommendation(user_id, model_id, track_id)

        (model_id, track_id) = self.predictive_models.model_c_recommendation(new_track_id)
        self.create_or_update_recommendation(user_id, model_id, track_id)

        self.beat_session.commit()

    def create_or_update_recommendation(self, user_id, model_id, track_id):
        recommendations = self.beat_session.query(Recommendation).filter(
            Recommendation.userID == user_id, Recommendation.modelID == model_id).all()

        if len(recommendations) > 0:
            recommendation = recommendations[0]
            recommendation.trackID = track_id
        else:
            recommendation = Recommendation(
                userID=user_id,
                modelID=model_id,
                trackID=track_id,
                model_score=0
            )
        self.beat_session.merge(recommendation)


def main():
    c = CurrentSongConsumer()
    c.handleMessages()


if __name__ == "__main__":
    #main()
    #cProfile.run('main()')

    with cProfile.Profile() as profile:
        main()

    profile_result = pstats.Stats(profile)
    profile_result.sort_stats(pstats.SortKey.TIME)
    profile_result.print_stats()
