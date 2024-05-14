{{ config(materialized='view') }}

WITH song_counts AS (
    SELECT
        song_id,
        COUNT(*) AS play_count
    FROM {{ source('dbt_pk', 'event') }}
    GROUP BY song_id
)

SELECT
    s.song_id,
    s.song,
    a.artist,
    sc.play_count
FROM song_counts sc
JOIN {{ source('dbt_pk', 'song') }} s ON sc.song_id = s.song_id
JOIN {{ source('dbt_pk', 'artist') }} a ON s.artist_id = a.artist_id
ORDER BY sc.play_count DESC
LIMIT 5