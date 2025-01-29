{{ config(
    materialized='table'
) }}

SELECT
    user_id,
    product_name,
    TRY_CAST(login_time AS TIMESTAMP) as login_time,
    TRY_CAST(logout_time AS TIMESTAMP) as logout_time,
    session_duration_minutes::DOUBLE as session_duration_minutes,
    price::DECIMAL(10,2) as price,
    purchase_status,
    user_agent,
    is_active,
    TRY_CAST(account_created AS TIMESTAMP) as account_created,
    TRY_CAST(account_updated AS TIMESTAMP) as account_updated,
    TRY_CAST(account_deleted AS TIMESTAMP) as account_deleted,
    ip_address,
    regexp_extract(user_agent, '(\(.*?\))', 1) as device_type,
    regexp_extract(user_agent, '([^;]+)(?=;)', 1) as os,
    regexp_extract(user_agent, '(?<=\s)[^/]+(?=/)', 1) as browser,
    {{ dbt_utils.generate_surrogate_key(['user_id', 'login_time']) }} as session_id,
    {{ dbt_utils.generate_surrogate_key(['user_id', 'login_time', 'product_name']) }} as transact_id
FROM {{ source('raw_data', 'user_activity') }}