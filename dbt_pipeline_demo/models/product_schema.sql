{{
    config(
        materialized='table'
    )
}}

WITH dim_product AS (
    SELECT 
        product_name,
        MAX(price) as price,
        MAX(CASE 
            WHEN price < 500 THEN 'Budget'
            WHEN price < 1000 THEN 'Standard'
            WHEN price < 2500 THEN 'Premium'
            ELSE 'Luxury'
        END) as price_tier
    FROM {{ ref('stg_product_schema') }}
    WHERE product_name IS NOT NULL
    GROUP BY product_name
),

dim_platform AS (
    SELECT DISTINCT
        user_agent,
        FIRST_VALUE(device_type) OVER (PARTITION BY user_agent ORDER BY login_time DESC) as device_type,
        FIRST_VALUE(os) OVER (PARTITION BY user_agent ORDER BY login_time DESC) as os,
        FIRST_VALUE(browser) OVER (PARTITION BY user_agent ORDER BY login_time DESC) as browser
    FROM {{ ref('stg_product_schema') }}
    WHERE user_agent IS NOT NULL
)

SELECT DISTINCT
    s.session_id,
    s.transact_id,
    s.login_time,
    s.logout_time,
    s.session_duration_minutes,
    s.purchase_status,
    s.price,
    s.is_active,
    s.account_created,
    s.account_updated,
    s.account_deleted,
    s.ip_address,
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    p.product_name,
    p.price_tier,
    pl.device_type,
    pl.os,
    pl.browser
FROM {{ ref('stg_product_schema') }} s
LEFT JOIN {{ ref('dim_user') }} u 
    ON s.user_id = u.user_id
LEFT JOIN dim_product p 
    ON s.product_name = p.product_name
LEFT JOIN dim_platform pl 
    ON s.user_agent = pl.user_agent