
import cProfile
import pstats

from kafka import KafkaConsumer
from json import loads
from connections.million_connection import MillionConnection, Track
from connections.beatstream_connection import BeatstreamConnection, User, Recommendation
from score_mechanic import ScoreMechanic
from predictive_models import PredictiveModels
from timeit import default_timer as timer

#TODO: Refactor all 'Get Random Track' queries to have MySQL pick the random track_id, rather than return
# list for us to pick a random one. This should improve performance. "SELECT RANDOM WHERE ..."


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
        print("Initializing CurrentSongConsumer")
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
        print("Initialization Complete")
    '''Currently updates user table and recommends a song using each predictive model'''
    def handleMessages(self):
        count = 0
        start = timer()
        for message in self.consumer:
            message = message.value
            self.updateUser(message)
            existing_recommendations = self.getExistingRecommendations(message)
            self.scoreModels(message, existing_recommendations)
            self.recommendSong(message, existing_recommendations)
            self.beat_session.commit()
            count += 1
            if count % 3000 == 0:
                end = timer()
                print (f"Handled {count} total messages. Took {end - start} seconds.")
                start = timer()

                #return



    '''Shows current song for user.'''
    def updateUser(self, message):
        u = User(
            id=message['userID'],
            current_song=str(message['trackID'])
        )
        self.beat_session.merge(u)

        #self.beat_session.commit()

    def scoreModels(self, message, existing_recommendations):
        new_track_id = message['trackID']
        user_id = message['userID']
        #recommendations = self.beat_session.query(Recommendation).filter(Recommendation.userID == user_id).all()
        for recommendation in existing_recommendations:
            recommended_track_id = recommendation.trackID
            score = self.score_mechanic.get_score(new_track_id, recommended_track_id)
            recommendation.model_score += score
            self.beat_session.add(recommendation)
        #self.beat_session.commit()

    def getExistingRecommendations(self, message):
        user_id = message['userID']
        return self.beat_session.query(Recommendation).filter(Recommendation.userID == user_id).all()


    '''Recommend song adds one row per model for each user with recommended next song and total score for predictive model. 
    right now that score is a random placeholder.'''
    def recommendSong(self, message, existing_recommendations):
        new_track_id = message['trackID']
        if new_track_id == "None":
            new_track_id = None
        user_id = message['userID']

        (model_id, track_id) = self.predictive_models.model_a_recommendation(new_track_id)
        self.create_or_update_recommendation(user_id, model_id, track_id, existing_recommendations)

        (model_id, track_id) = self.predictive_models.model_b_recommendation(new_track_id)
        self.create_or_update_recommendation(user_id, model_id, track_id, existing_recommendations)

        #TODO Reactivate this once performance is better
        # (model_id, track_id) = self.predictive_models.model_c_recommendation(new_track_id)
        # self.create_or_update_recommendation(user_id, model_id, track_id)

        #self.beat_session.commit()

    def create_or_update_recommendation(self, user_id, model_id, track_id, existing_recommendations):
        # recommendations = self.beat_session.query(Recommendation).filter(
        #     Recommendation.userID == user_id, Recommendation.modelID == model_id).all()
        recommendation = None
        for recommendation in existing_recommendations:
            if recommendation.modelID == model_id:
                recommendation = existing_recommendations[0]
                recommendation.trackID = track_id
                break

        if recommendation is None:
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
    main()


    # with cProfile.Profile() as profile:
    #     main()
    #
    # stats = pstats.Stats(profile).sort_stats("cumtime")
    # stats.print_stats(r"\((?!\_).*\)$")  # Exclude private and magic callables.

