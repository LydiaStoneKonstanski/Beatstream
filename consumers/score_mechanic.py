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
        new_year = self.million_connection.get_year(new_track_id)
        recommended_year = self.million_connection.get_year(recommended_track_id)

        if abs(new_year - recommended_year) <=5:
            score += 2

        if self.million_connection.get_decade(new_year) == self.million_connection.get_decade(recommended_year):
            score += 2

        new_artist_id = self.million_connection.get_artist_id(new_track_id)
        recommended_artist_id = self.million_connection.get_artist_id(recommended_track_id)

        if new_artist_id==recommended_artist_id:
            score += 5


        # TODO: This is too slow to keep up. We need a more efficient way to query to work at this scale.
        # similar_artist_ids = self.million_connection.get_similar_artist_ids(new_artist_id)
        #
        # if recommended_artist_id in similar_artist_ids:
        #     score += 4

        return score



