import random

from connections.million_connection import MillionConnection, Track, Similarity

class PredictiveModels():
    def __init__(self, million_connection):
        self.million_connection = million_connection
        self.million_session = self.million_connection.session

    def model_a_recommendation(self, current_track_id):
        ''' Model A returns a random track, ignoring the current track'''
        model_id = "Model A"
        index = random.randint(1, 1000000)
        track = self.million_session.query(Track).filter(Track.index == index).first()
        return (model_id, track.track_id)

    def model_b_recommendation(self, track_id):
        '''
        Model B returns a random track from the same artist.
        It's possible for the current song to be recommended again.
        '''
        model_id = "Model B"
        artist_id = self.million_connection.get_artist_id(track_id)
        tracks = self.million_session.query(Track).filter(Track.artist_id == artist_id).all()
        random_choice = random.randint(0, len(tracks))
        track = tracks[random_choice]
        return (model_id, track.track_id)

    def model_c_recommendation(self, track_id):
        '''Model C returns a random track from the same decade.'''
        model_id = "Model C"
        year = self.million_connection.get_year(track_id)
        decade = self.million_connection.get_decade(year)
        tracks = self.million_session.query(Track).filter(Track.year >= decade, Track.year < decade + 10).all()
        random_choice = random.randint(0, len(tracks))
        track = tracks[random_choice]
        return (model_id, track.track_id)
