{{
    config(
        materialized='table'
    )
}}

SELECT
    user_id,
    MAX(first_name) AS first_name,
    MAX(last_name) AS last_name,
    MAX(email) AS email,
    MAX(date_of_birth) AS date_of_birth,
    MAX(country) AS country,
    MAX(company) AS company,
    MAX(job_title) AS job_title,
    MAX(is_active) AS is_active
FROM {{ source('raw_data', 'user_activity') }}
WHERE user_id IS NOT NULL
GROUP BY user_id