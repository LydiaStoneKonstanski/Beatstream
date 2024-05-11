{{ config(materialized='table') }}

    select
	song_id as song_id,
	song as song,
	release as release,
	year as year,
	tempo as tempo,
	time_signature as time_signature,
	loudness as loudness,
	key as key,
	mode as mode,
	artist_id as artist_id
	from dbt_pk.song_stage