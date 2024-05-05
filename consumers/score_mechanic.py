import math
from connections.million_connection import MillionConnection, Track, Similarity
class ScoreMechanic():
    def __init__(self, million_connection):
        self.million_connection = million_connection
        self.million_session = self.million_connection.session

#TODO add additional scoring such as mb tag and term.
    def get_score(self, new_track_id, recommended_track_id):
        score=0
        if recommended_track_id==new_track_id:
            score+=10
        new_year = self.get_year(new_track_id)
        recommended_year = self.get_year(recommended_track_id)

        if abs(new_year - recommended_year) <=5:
            score += 2

        if math.floor(new_year/10)*10 == math.floor(recommended_year/10)*10:
            score += 2

        new_artist_id = self.get_artist_id(new_track_id)
        recommended_artist_id = self.get_artist_id(recommended_track_id)

        if new_artist_id==recommended_artist_id:
            score += 5

        similar_artist_ids = self.get_similar_artist_ids(new_artist_id)

        if recommended_artist_id in similar_artist_ids:
            score += 4

        return score

    # TODO flesh out these functions
    def get_year(self, track_id):
        track = self.million_session.query(Track).filter(Track.track_id == track_id).first()
        year = track.year

        return year

    def get_artist_id(self, track_id):
        track = self.million_session.query(Track).filter(Track.track_id == track_id).first()
        artist_id = track.artist_id

        return artist_id

    def get_similar_artist_ids(self, artist_id):
        artist_ids = []
        similar_match = self.million_session.query(Similarity).filter(Similarity.similar == artist_id).all()

        for artist in similar_match:
            artist_ids.append(artist.target)

        target_match = self.million_session.query(Similarity).filter(Similarity.target == artist_id).all()

        for artist in target_match:
            artist_ids.append(artist.similar)

        return artist_ids

