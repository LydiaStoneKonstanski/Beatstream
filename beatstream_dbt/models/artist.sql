{{ config(materialized='table') }}

    select
	artist_id as artist_id,
	artist as artist_name
    from dbt_pk.artist_stage
