"""
Analytics queries for the product data pipeline.
All queries run against dbt-created tables in the main schema.
"""

import logging
from pathlib import Path
import pandas as pd
from duckdb import DuckDBPyConnection

log = logging.getLogger(__name__)

def run_lifecycle_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user lifecycle metrics."""
    try:
        return conn.sql("""
            SELECT 
                COUNT(DISTINCT user_id) as total_users,
                COUNT(DISTINCT CASE WHEN is_active = 'yes' THEN user_id END) as active_users,
                ROUND(
                    AVG(
                        DATEDIFF(
                            'day',
                            TRY_CAST(account_created AS TIMESTAMP),
                            COALESCE(
                                TRY_CAST(account_deleted AS TIMESTAMP),
                                CURRENT_TIMESTAMP
                            )
                        )
                    ),
                2) as avg_account_lifetime_days
            FROM product_schema;
        """).fetchdf()
    except Exception as e:
        log.error("Error in lifecycle analysis: %s", str(e))
        raise

def run_purchase_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze purchase patterns."""
    try:
        return conn.sql("""
            SELECT 
                product_name,
                COUNT(*) as total_purchases,
                ROUND(SUM(price), 2) as total_revenue,
                ROUND(AVG(price), 2) as avg_price,
                ROUND(
                    COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) * 100.0 / 
                    COUNT(*),
                2) as conversion_rate
            FROM product_schema
            GROUP BY product_name
            ORDER BY total_revenue DESC;
        """).fetchdf()
    except Exception as e:
        log.error("Error in purchase analysis: %s", str(e))
        raise

def run_demographics_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user demographics and platform usage."""
    try:
        return conn.sql("""
            SELECT 
                device_type,
                os,
                browser,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                ROUND(AVG(CASE 
                    WHEN purchase_status = 'completed' THEN price 
                    ELSE 0 
                END), 2) as avg_purchase_value
            FROM product_schema
            GROUP BY device_type, os, browser
            HAVING COUNT(*) > 10
            ORDER BY unique_users DESC;
        """).fetchdf()
    except Exception as e:
        log.error("Error in demographics analysis: %s", str(e))
        raise

def run_business_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze business metrics by job title."""
    try:
        return conn.sql("""
            SELECT 
                job_title,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                ROUND(
                    COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) * 100.0 / 
                    COUNT(*),
                2) as conversion_rate
            FROM product_schema
            GROUP BY job_title
            HAVING COUNT(DISTINCT user_id) > 5
            ORDER BY unique_users DESC;
        """).fetchdf()
    except Exception as e:
        log.error("Error in business analysis: %s", str(e))
        raise

def run_engagement_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user engagement patterns."""
    try:
        return conn.sql("""
            SELECT 
                DATE_TRUNC('hour', TRY_CAST(login_time AS TIMESTAMP)) as hour,
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                ROUND(SUM(CASE 
                    WHEN purchase_status = 'completed' THEN price 
                    ELSE 0 
                END), 2) as revenue
            FROM product_schema
            GROUP BY DATE_TRUNC('hour', TRY_CAST(login_time AS TIMESTAMP))
            ORDER BY total_sessions DESC;
        """).fetchdf()
    except Exception as e:
        log.error("Error in engagement analysis: %s", str(e))
        raise

def run_churn_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user churn patterns."""
    try:
        return conn.sql("""
            SELECT 
                DATE_TRUNC('month', TRY_CAST(account_created AS TIMESTAMP)) as cohort_month,
                COUNT(DISTINCT user_id) as cohort_size,
                ROUND(
                    COUNT(CASE WHEN account_deleted IS NOT NULL THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(*), 0),
                2) as churn_rate,
                ROUND(
                    AVG(CASE 
                        WHEN account_deleted IS NOT NULL THEN 
                            DATEDIFF('day', account_created, account_deleted)
                        ELSE NULL 
                    END),
                2) as avg_days_to_churn
            FROM product_schema
            GROUP BY DATE_TRUNC('month', TRY_CAST(account_created AS TIMESTAMP))
            ORDER BY cohort_month;
        """).fetchdf()
    except Exception as e:
        log.error("Error in churn analysis: %s", str(e))
        raise

def run_session_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze session patterns."""
    try:
        return conn.sql("""
            SELECT 
                user_id,
                first_name,
                last_name,
                COUNT(*) as total_sessions,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                ROUND(DATEDIFF('hour', 
                    MIN(TRY_CAST(login_time AS TIMESTAMP)),
                    MAX(TRY_CAST(login_time AS TIMESTAMP))
                ) / 3600.0, 1) AS session_duration_hours
            FROM product_schema
            WHERE TRY_CAST(login_time AS TIMESTAMP) IS NOT NULL
            GROUP BY user_id, first_name, last_name
            ORDER BY total_sessions DESC
            LIMIT 100;
        """).fetchdf()
    except Exception as e:
        log.error("Error in session analysis: %s", str(e))
        raise

def run_funnel_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze conversion funnel metrics."""
    try:
        return conn.sql("""
            SELECT 
                product_name,
                COUNT(*) as total_views,
                COUNT(DISTINCT user_id) as unique_viewers,
                ROUND(
                    COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(*), 0),
                2) as conversion_rate
            FROM product_schema
            GROUP BY product_name
            ORDER BY conversion_rate DESC;
        """).fetchdf()
    except Exception as e:
        log.error("Error in funnel analysis: %s", str(e))
        raise

def save_analysis_results(results: dict, reports_dir: Path) -> None:
    """Save analysis results to CSV files."""
    try:
        for name, df in results.items():
            output_path = reports_dir / f"{name}.csv"
            df.to_csv(output_path, index=False)
            log.info(f"Saved {name} to {output_path}")
    except Exception as e:
        log.error("Error saving analysis results: %s", str(e))
        raise
