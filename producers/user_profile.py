import uuid
from itertools import accumulate
class User():

    def __init__(self, profile):
        self.profile = profile
        self.user_id = uuid.uuid1()
        self.current_track_id = None


class UserProfile():

    def __init__(self, name, weights, stability):
        self.name = name
        self.weights = weights
        self.stability = stability
        total_weights = sum(weights)
        self.probabilities = [x/total_weights for x in self.weights]
        self.cumulative = list(accumulate(self.probabilities))

    def create_user(self):
        return User(self)

class UserProfiles():

    def __init__(self):
        self.profiles = [
            UserProfile("profile_1", [1, 3, 5, 1], 5),
            UserProfile("profile_2", [0, 0, 3, 2], 10),
            UserProfile("profile_3", [1, 4, 3, 5], -1)
        ]


