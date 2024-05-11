{{ config(materialized='table',
          unique_key=['userId', 'song_id', 'ts']) }}

    select
	ts as ts,
	state as state,
	city as city,
	zip as zip,
	userId as userId,
	song_id as song_id,
	artist_id as artist_id
	from dbt_pk.event_stage
