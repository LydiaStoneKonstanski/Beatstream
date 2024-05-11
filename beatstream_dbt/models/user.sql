{{ config(materialized='table') }}

    select
	userId as userId,
	firstName as firstName,
	lastName as lastName,
	registration as registration
    from dbt_pk.user_stage
