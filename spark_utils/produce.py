from topics import Topic
listen = Topic('listen-events', 9092, '/Users/lydia/Projects/Beatstream/data/sample/listen_events')
analysis = Topic('analysis', 9092, '/Users/lydia/Projects/Beatstream/data/analysis.csv')
tracks = Topic('tracks', 9092, '/Users/lydia/Projects/Beatstream/data/tracks.csv')

# listen.j_produce()
# analysis.c_produce()
tracks.c_produce()
