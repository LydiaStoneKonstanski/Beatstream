from topics import Topic
listen = Topic('listen-events', 9092, '/Users/chris/Downloads/sample/listen_events.json')
analysis = Topic('analysis', 9092, '/Users/chris/Downloads/mill_songs/analysis copy.csv')
tracks = Topic('tracks', 9092, '/Users/chris/Downloads/mill_songs/tracks copy.csv')

# listen.j_produce()
# analysis.c_produce()
tracks.c_produce()
