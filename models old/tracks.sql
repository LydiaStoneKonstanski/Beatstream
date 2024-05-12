

with final as (
    select
id	as	id	,
track_id	as	track_id	,
title	as	title	,
artist_name	as	artist_name	,
release	as	release	,
year	as	year	,
duration	as	duration	,
song_hotttnesss	as	song_hotttnesss	,
artist_hotttnesss	as	artist_hotttnesss	,
artist_familiarity	as	artist_familiarity	,
artist_location	as	artist_location	,
artist_latitude	as	artist_latitude	,
artist_longitude	as	artist_longitude	,
song_id	as	song_id	,
artist_id	as	artist_id	,
track_7digitalid	as	track_7digitalid	,
artist_7digitalid	as	artist_7digitalid	,
release_7digitalid	as	release_7digitalid	,
artist_mbid	as	artist_mbid	
    from dbt_pk.tracks_raw
)
select * from final