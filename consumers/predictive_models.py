import random

from connections.million_connection import MillionConnection, Track, Similarity

class PredictiveModels():
    def __init__(self, million_connection):
        self.million_connection = million_connection
        self.million_session = self.million_connection.session

    def model_a_recommendation(self, current_track_id):
        ''' Model A returns a random track, ignoring the current track'''
        model_id = "Model A"
        track_id = self.million_connection.get_random_track_id()

        return (model_id, track_id)

    def model_b_recommendation(self, current_track_id):
        '''
        Model B returns a random track from the same artist.
        It's possible for the current song to be recommended again.
        '''
        model_id = "Model B"
        track_id = self.million_connection.get_same_artist_track_id(current_track_id)

        return (model_id, track_id)

    def model_c_recommendation(self, current_track_id):
        '''Model C returns a random track from the same decade.'''
        model_id = "Model C"
        track_id = self.million_connection.get_same_decade_track_id(current_track_id)
        return (model_id, track_id)
