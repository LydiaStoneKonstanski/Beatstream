import math
class ScoreMechanic():
    def __init__(self):
        pass

#TODO add additional scoring such as mb tag and term.
    def get_score(self, old_track_id, new_track_id, recommended_track_id):
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

        similar_artist_ids = self.get_similar_artist_ids(new_track_id)

        if recommended_artist_id in similar_artist_ids:
            score += 4

        return score

    # TODO flesh out these functions
    def get_year(self, track_id):
        year= None
        return year

    def get_artist_id(self, track_id):
        artist_id= None
        return artist_id

    def get_similar_artist_ids(self, track_id):
        artist_ids=[]

        return artist_ids

