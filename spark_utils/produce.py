from topics import Topic
listen = Topic('listen-events', 9092, '/Users/deepa/Documents/Projects/Beatstream/spark_utils/streaming_parquet')
# analysis = Topic('analysis', 9092, '/Users/chris/Downloads/mill_songs/analysis copy.csv')
# tracks = Topic('tracks', 9092, '/Users/chris/Downloads/mill_songs/tracks copy.csv')
listen.p_produce()
