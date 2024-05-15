{{ config(materialized='view') }}

WITH score AS (
  SELECT
    TIMESTAMP_MILLIS(ts) AS event_time,
    userId,
    song_id,
    score
  FROM
    {{ source('dbt_pk', 'rec') }}
)

SELECT
  *
FROM
  score
ORDER BY
  event_time