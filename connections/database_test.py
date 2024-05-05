from beatstream_connection import BeatstreamConnection, User, Recommendation
from million_connection import MillionConnection, Analysis, ArtistMbtag, ArtistTerm, Artist, Similarity, Track

def printresult(result):
    first = True
    output = ''
    for row in result:
        output = ''
        if first == True:
            for c in row.__table__.columns:
                output += str(c.name) + ', '
            print (output)
            first = False
            output = ''
        for c in row.__table__.columns:
            output += str(getattr(row, c.name)) + ', '
        print (output)

if __name__ == "__main__":
    million_connection = MillionConnection()
    million_session = million_connection.session

    print("\nMillion Songs Analysis - Last 5")
    query = million_session.query(Analysis).order_by(Analysis.index.desc()).limit(5)
    result = query.all()
    printresult(result)

    print("\nMillion Songs Artist MBTag - Last 5")
    query = million_session.query(ArtistMbtag).order_by(ArtistMbtag.index.desc()).limit(5)
    result = query.all()
    printresult(result)

    print("\nMillion Songs Artist Term - Last 5")
    query = million_session.query(ArtistTerm).order_by(ArtistTerm.index.desc()).limit(5)
    result = query.all()
    printresult(result)

    print("\nMillion Songs Artists - Last 5")
    query = million_session.query(Artist).order_by(Artist.artist_id.desc()).limit(5)
    result = query.all()
    printresult(result)

    print("\nMillion Songs Similarity - Last 5")
    query = million_session.query(Similarity).order_by(Similarity.index.desc()).limit(5)
    result = query.all()
    printresult(result)

    print("\nMillion Songs Tracks - Last 5")
    query = million_session.query(Track).order_by(Track.index.desc()).limit(5)
    result = query.all()
    printresult(result)

    beatstream_connection = BeatstreamConnection()
    beatstream_session = beatstream_connection.session

    print("\nBeatstream Users - Last 5")
    query = beatstream_session.query(User).order_by(User.id.desc()).limit(5)
    result = query.all()
    printresult(result)

    print("\nBeatstream Recommendations - Last 5")
    query = beatstream_session.query(Recommendation).order_by(Recommendation.id.desc()).limit(5)
    result = query.all()
    printresult(result)