import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


#########################################################
#
#   Load Environment Variables
#
#########################################################
snowflake_conn_id = "snowflake_conn_id"


########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'AkshayaSarathy',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='DAG4',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################


query_refresh_ext_table_raw_airbnb = f"""
ALTER EXTERNAL TABLE raw.raw_airbnb REFRESH;
"""

query_refresh_ext_table_raw_nswlgacode = f""" 
ALTER EXTERNAL TABLE raw.raw_code REFRESH;
"""

query_refresh_ext_table_raw_nswlgasuburb =f"""
ALTER EXTERNAL TABLE raw.raw_suburb REFRESH;
"""

query_refresh_ext_table_raw_censusg01 = f"""
ALTER EXTERNAL TABLE raw.raw_g01census REFRESH;
"""

query_refresh_ext_table_raw_censusg02 =f"""
ALTER EXTERNAL TABLE raw.raw_g02census REFRESH;
"""

query_refresh_staging_listing = f"""
CREATE OR REPLACE TABLE staging.staging_listing AS
SELECT
value:c1::varchar as listing_id,
value:c4::varchar as host_id,
value:c5::varchar as host_name,
value:c6::varchar as host_since,
value:c7::char as host_superhost,
value:c8::varchar as host_neighbourhood,
value:c9::varchar as listing_neighbourhood,
value:c10::varchar as property_type,
value:c11::varchar as room_type,
value:c12::int as accommodates,
value:c13::int as price,
value:c14::char as has_availability,
value:c15::int as availability_30,
value:c16::int as number_of_reviews,
value:c17::int as review_scores_rating,
value:c18::int as review_scores_accuracy,
value:c19::int as review_scores_cleanliness,
value:c20::int as review_scores_checkin,
value:c21::int as review_scores_communication,
value:c22::int as review_scores_value,
SUBSTR(metadata$filename, 6, 7) as month_year
FROM raw.raw_airbnb;
"""

query_refresh_staging_code = f"""
CREATE OR REPLACE TABLE staging.staging_code AS
SELECT 
value:c1::varchar AS lga_code,
value:c2::varchar AS lga_name
FROM raw.raw_code;
"""

query_refresh_staging_suburb = f"""
CREATE OR REPLACE TABLE staging.staging_suburb AS
SELECT
value:c1::varchar AS lga_name,
value:c2::varchar AS suburb_name
FROM raw.raw_suburb;
"""

query_refresh_staging_g01 = f"""
CREATE OR REPLACE TABLE staging.staging_g01 AS
SELECT
value:c1::varchar AS lga_code,
value:c2::int AS total_pop_m,
value:c3::int AS total_pop_f,
value:c4::int total_pop_p,
value:c5::int age_0_4_yr_m,
value:c6::int age_0_4_yr_f,
value:c7::int age_0_4_yr_p,
value:c8::int age_5_14_yr_m,
value:c9::int age_5_14_yr_f,
value:c10::int age_5_14_yr_p,
value:c11::int age_15_19_yr_m,
value:c12::int age_15_19_yr_f,
value:c13::int age_15_19_yr_p,
value:c14::int age_20_24_yr_m,
value:c15::int age_20_24_yr_f,
value:c16::int age_20_24_yr_p,
value:c17::int age_25_34_yr_m,
value:c18::int age_25_34_yr_f,
value:c19::int age_25_34_yr_p,
value:c20::int age_35_44_yr_m,
value:c21::int age_35_44_yr_f,
value:c22::int age_35_44_yr_p,
value:c23::int age_45_54_yr_m,
value:c24::int age_45_54_yr_f,
value:c25::int age_45_54_yr_p,
value:c26::int age_55_64_yr_m,
value:c27::int age_55_64_yr_f,
value:c28::int age_55_64_yr_p,
value:c29::int age_65_74_yr_m,
value:c30::int age_65_74_yr_f,
value:c31::int age_65_74_yr_p,
value:c32::int age_75_84_yr_m,
value:c33::int age_75_84_yr_f,
value:c34::int age_75_84_yr_p,
value:c35::int age_85ov_m,
value:c36::int age_85ov_f,
value:c37::int age_85ov_p
FROM raw.raw_g01census;
"""

query_refresh_staging_g02 = f"""
CREATE OR REPLACE TABLE staging.staging_g02 AS
SELECT
value:c1::varchar AS lga_code,
value:c2::int AS median_age,
value:c3::int AS median_mortgage_repay_monthly,
value:c4::int AS median_tot_prsnl_inc_weekly,
value:c5::int AS median_rent_weekly,
value:c6::int AS median_tot_fam_inc_weekly,
value:c7::int AS avg_num_psns_per_bedroom,
value:c8::int AS median_tot_hhd_inc_weekly,
value:c9::int AS avg_household_size
FROM raw.raw_g02census;
"""

query_refresh_datawarehouse_dim_listing = f"""
CREATE OR REPLACE TABLE datawarehouse.dim_listing AS 
SELECT 
listing_id,
host_id,
lower(listing_neighbourhood) as listing_neighbourhood,
property_type,
room_type,
accommodates,
price,
has_availability,
availability_30,
number_of_reviews,
review_scores_rating,
month_year
FROM staging.staging_listing;
"""

query_refresh_datawarehouse_dim_hosting = f"""
CREATE OR REPLACE TABLE datawarehouse.dim_hosting AS 
SELECT
host_id,
host_name,
TO_DATE(host_since,'DD/MM/YYYY') AS host_since,
host_superhost,
lower(host_neighbourhood) AS host_neighbourhood
FROM staging.staging_listing;
"""

query_refresh_datawarehouse_dim_lga = f"""
CREATE OR REPLACE TABLE datawarehouse.dim_lga AS 
SELECT 
a.lga_code,
lower(a.lga_name) AS lga_name,
lower(b.suburb_name) AS suburb_name
FROM staging.staging_code as a 
LEFT JOIN staging.staging_suburb as b 
ON lower(a.lga_name) = lower(b.lga_name)
"""

query_refresh_datawarehouse_dim_censusg01 = f"""
CREATE OR REPLACE TABLE datawarehouse.dim_censusg01 AS 
SELECT 
SUBSTR(lga_code, 4,5) AS clean_lga_code,*
FROM staging.staging_g01;
"""

query_refresh_datawarehouse_dim_censusg02 = f"""
CREATE OR REPLACE TABLE datawarehouse.dim_censusg02 AS 
SELECT 
SUBSTR(lga_code, 4,5) AS clean_lga_code,*
FROM staging.staging_g02;
"""

query_refresh_datawarehouse_fact_table = f"""
CREATE OR REPLACE TABLE datawarehouse.fact_table AS 
SELECT
DISTINCT a.listing_id,
a.host_id,
b.lga_code
FROM staging.staging_listing AS a 
LEFT JOIN datawarehouse.dim_lga AS b 
ON lower(a.listing_neighbourhood) = lower(b.lga_name)
LEFT JOIN datawarehouse.dim_censusg02 AS c 
ON b.lga_code = c.clean_lga_code
;
"""

query_refresh_datamart_dm_superhost_price = f"""
CREATE OR REPLACE TABLE datamart.dm_superhost_price AS
WITH cte1 AS (
SELECT
    a.listing_neighbourhood,
    a.month_year,
    COUNT(DISTINCT a.listing_id) AS total_listings,
    COUNT(DISTINCT b.host_id) AS total_distinct_hosts,
    COUNT(DISTINCT(CASE WHEN b.host_superhost = 't'
    THEN b.host_id
    ELSE 0
    END
   )) AS total_superhost_count,
    
MIN(CASE WHEN a.has_availability = 't' THEN a.price ELSE null END) as true_min_price_active_listings,
MAX(CASE WHEN a.has_availability = 't' THEN a.price ELSE null END) as true_max_price_active_listings,
AVG(CASE WHEN a.has_availability = 't' THEN a.price ELSE null END) as true_avg_price_active_listings,
MEDIAN(CASE WHEN a.has_availability = 't' THEN a.price ELSE null END) as true_median_price_active_listings,
AVG(CASE WHEN a.has_availability = 't' THEN a.review_scores_rating ELSE null END) as true_avg_review_score_rating
    
FROM datawarehouse.fact_table AS f
LEFT JOIN datawarehouse.dim_listing AS a
ON f.listing_id = a.listing_id
LEFT JOIN datawarehouse.dim_hosting AS b
ON a.host_id = b.host_id
GROUP BY a.listing_neighbourhood, a.month_year
ORDER BY a.listing_neighbourhood, a.month_year
)

SELECT ROUND((total_superhost_count/total_distinct_hosts)*100,2) as superhost_rate, * FROM cte1;

"""

query_refresh_datamart_total_stays = f"""
CREATE OR REPLACE TABLE datamart.total_stays AS
SELECT 
listing_neighbourhood, 
month_year,
COUNT(1) as total_listings, 
SUM(CASE WHEN has_availability='t' THEN 30-availability_30 ELSE null END) as total_number_of_stays
FROM datawarehouse.dim_listing a
GROUP BY listing_neighbourhood, month_year
ORDER BY listing_neighbourhood, month_year
;
"""

query_refresh_datamart_percent_change = f"""
CREATE OR REPLACE TABLE datamart.percent_change AS 
SELECT 
listing_neighbourhood,
month_year, 
COUNT(CASE WHEN has_availability = 't' THEN has_availability ELSE null END) as total_active_listings,
COUNT(CASE WHEN has_availability = 'f' THEN has_availability ELSE null END) as total_inactive_listings,
LAG(total_active_listings) OVER (
PARTITION BY listing_neighbourhood
ORDER BY month_year) previous_total_active_listings,
LAG(total_inactive_listings) OVER (
PARTITION BY listing_neighbourhood
ORDER BY month_year) previous_total_inactive_listings,
    
CONCAT(ROUND((total_active_listings - previous_total_active_listings ) * 100 /NULLIF(previous_total_active_listings,0),0),'%')  percentage_change_active,
CONCAT(ROUND((total_inactive_listings - previous_total_inactive_listings ) * 100/NULLIF(previous_total_inactive_listings,0),0),'%')  percentage_change_inactive
FROM datawarehouse.dim_listing
GROUP BY listing_neighbourhood, month_year
;
"""

query_refresh_datamart_est_rev = f"""
CREATE OR REPLACE TABLE datamart.est_rev AS
SELECT 
listing_neighbourhood, 
month_year,
COUNT(DISTINCT listing_id) AS total_listings,
SUM(price*(30-availability_30)) as rev,
rev/total_listings as avg_rev
FROM datawarehouse.dim_listing
GROUP BY listing_neighbourhood, month_year
ORDER BY listing_neighbourhood, month_year
;
"""

query_refresh_datamart_dm_listing_neighbourhood = f"""
CREATE OR REPLACE TABLE datamart.dm_listing_neighbourhood AS 
SELECT
A.listing_neighbourhood,
A.month_year,
A.total_listings,
(C.total_active_listings*100/A.total_listings) AS active_listings_rate,
true_min_price_active_listings,
true_max_price_active_listings,
true_avg_price_active_listings,
true_median_price_active_listings,
total_distinct_hosts,
total_superhost_count,
true_avg_review_score_rating,
C.percentage_change_active,
C.percentage_change_inactive,
B.total_number_of_stays,
D.avg_rev
FROM datamart.dm_superhost_price AS A
LEFT JOIN datamart.total_stays AS B ON A.listing_neighbourhood = B.listing_neighbourhood AND A.month_year = B.month_year
LEFT JOIN datamart.percent_change AS C ON A.listing_neighbourhood = C.listing_neighbourhood AND A.month_year = C.month_year
LEFT JOIN datamart.est_rev AS D on A.listing_neighbourhood = D.listing_neighbourhood AND A.month_year = D.month_year
;

"""

query_refresh_datamart_dm_superhost_score = f"""
CREATE OR REPLACE TABLE datamart.dm_superhost_score AS
WITH cte1 AS (
SELECT
    
    a.month_year,
    a.property_type,
    a.room_type,
    a.accommodates,
    COUNT(DISTINCT a.listing_id) AS total_listings,
    COUNT(DISTINCT b.host_id) AS total_distinct_hosts,
    COUNT(DISTINCT(CASE WHEN b.host_superhost = 't'
    THEN b.host_id
    ELSE 0
    END
   )) AS total_superhost_count,
    
MIN(CASE WHEN a.has_availability = 't' THEN a.price ELSE null END) as true_min_price_active_listings,
MAX(CASE WHEN a.has_availability = 't' THEN a.price ELSE null END) as true_max_price_active_listings,
AVG(CASE WHEN a.has_availability = 't' THEN a.price ELSE null END) as true_avg_price_active_listings,
MEDIAN(CASE WHEN a.has_availability = 't' THEN a.price ELSE null END) as true_median_price_active_listings,
AVG(CASE WHEN a.has_availability = 't' THEN a.review_scores_rating ELSE null END) as true_avg_review_score_rating
    
FROM datawarehouse.fact_table AS f
LEFT JOIN datawarehouse.dim_listing AS a
ON f.listing_id = a.listing_id
LEFT JOIN datawarehouse.dim_hosting AS b
ON a.host_id = b.host_id
GROUP BY a.property_type, a.room_type, a.accommodates, a.month_year
)
SELECT ROUND((total_superhost_count/total_distinct_hosts)*100,2) as superhost_rate, * FROM cte1;
"""

query_refresh_datamart_property_total_stays = f"""
CREATE OR REPLACE TABLE datamart.property_total_stays AS
SELECT  
A.property_type,
A.room_type,
A.accommodates,
A.month_year,
COUNT(1) as total_listings, 
SUM(CASE WHEN has_availability='t' THEN 30-availability_30 ELSE null END) as total_number_of_stays
FROM datawarehouse.dim_listing as A
GROUP BY A.property_type, A.room_type, A.accommodates, A.month_year;

"""

query_refresh_datamart_property_percent_change = f"""
CREATE OR REPLACE TABLE datamart.property_percent_change AS 
SELECT 
property_type,
room_type,
accommodates,
month_year, 
COUNT(CASE WHEN has_availability = 't' THEN has_availability ELSE null END) as total_active_listings,
COUNT(CASE WHEN has_availability = 'f' THEN has_availability ELSE null END) as total_inactive_listings,
LAG(total_active_listings) OVER (
PARTITION BY property_type, room_type, accommodates
ORDER BY month_year)AS previous_total_active_listings,
LAG(total_inactive_listings) OVER (
PARTITION BY property_type, room_type, accommodates
ORDER BY month_year)AS previous_total_inactive_listings,
    
CONCAT(ROUND((total_active_listings - previous_total_active_listings ) * 100 /NULLIF(previous_total_active_listings,0),0),'%')  AS percentage_change_active,
CONCAT(ROUND((total_inactive_listings - previous_total_inactive_listings ) * 100/NULLIF(previous_total_inactive_listings,0),0),'%') AS percentage_change_inactive
FROM datawarehouse.dim_listing
GROUP BY property_type, room_type, accommodates, month_year
;
"""

query_refresh_datamart_property_est_rev = f"""
CREATE OR REPLACE TABLE datamart.property_est_rev AS
SELECT  
month_year,
property_type,
room_type,
accommodates,
COUNT(DISTINCT listing_id) AS total_listings,
SUM(price*(30-availability_30)) as rev,
rev/total_listings as avg_rev
FROM datawarehouse.dim_listing
GROUP BY property_type, room_type, accommodates, month_year
;
"""

query_refresh_datamart_dm_property_type = f"""
CREATE OR REPLACE TABLE datamart.dm_property_type AS 
SELECT
A.month_year,
A.total_listings,
A.property_type,
A.room_type,
A.accommodates,
(C.total_active_listings*100/A.total_listings) AS active_listings_rate,
true_min_price_active_listings,
true_max_price_active_listings,
true_avg_price_active_listings,
true_median_price_active_listings,
total_distinct_hosts,
total_superhost_count,
true_avg_review_score_rating,
C.percentage_change_active,
C.percentage_change_inactive,
B.total_number_of_stays,
D.avg_rev
FROM datamart.dm_superhost_score AS A
LEFT JOIN datamart.property_total_stays AS B ON 
A.property_type = B.property_type AND 
A.room_type = B.room_type AND 
A.accommodates = B.accommodates AND 
A.month_year = B.month_year
LEFT JOIN datamart.property_percent_change AS C 
ON A.property_type = C.property_type
AND A.room_type = C.room_type
AND A.accommodates = C.accommodates
AND A.month_year = C.month_year
LEFT JOIN datamart.property_est_rev AS D 
ON A.property_type = D.property_type
AND A.room_type = D.room_type
AND A.accommodates = D.accommodates
AND A.month_year = D.month_year
;
"""

query_refresh_datamart_dm_host_neighbourhood = f"""
CREATE OR REPLACE TABLE datamart.dm_host_neighbourhood AS 
WITH cte1 AS (

    SELECT 
    a.month_year as month_year,
    c.lga_name as lga_name,
    b.host_id as host_id,
    a.price as price,
    IFF(a.has_availability = 't', 30-a.availability_30, null) as total_number_stays
    FROM datawarehouse.dim_listing AS a 
    LEFT JOIN datawarehouse.dim_hosting AS b 
    ON a.host_id = b.host_id
    LEFT JOIN datawarehouse.dim_lga AS c 
    ON lower(b.host_neighbourhood) = lower(c.lga_name)

)
SELECT month_year, lga_name, 
COUNT(DISTINCT host_id) AS unique_hosts,
SUM(price * total_number_stays) AS estimated_revenue,
ROUND((SUM(price * total_number_stays)/COUNT(DISTINCT host_id)),2) AS estimated_revenue_per_host
FROM cte1
GROUP BY month_year, lga_name
ORDER BY month_year, lga_name
;
"""


#########################################################
#
#   DAG Operator Setup
#
#########################################################

refresh_ext_table_raw_airbnb = SnowflakeOperator(
    task_id = 'refresh_ext_table_raw_airbnb',
    sql = query_refresh_ext_table_raw_airbnb,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_ext_table_raw_nswlgacode = SnowflakeOperator(
    task_id = 'refresh_ext_table_raw_nswlgacode',
    sql = query_refresh_ext_table_raw_nswlgacode,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_ext_table_raw_nswlgasuburb = SnowflakeOperator(
    task_id = 'refresh_ext_table_raw_nswlgasuburb',
    sql = query_refresh_ext_table_raw_nswlgasuburb,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_ext_table_raw_censusg01 = SnowflakeOperator(
    task_id = 'refresh_ext_table_raw_censusg01',
    sql = query_refresh_ext_table_raw_censusg01,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_ext_table_raw_censusg02 = SnowflakeOperator(
    task_id = 'refresh_ext_table_raw_censusg02',
    sql = query_refresh_ext_table_raw_censusg02,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

########################################################

refresh_staging_listing = SnowflakeOperator(
    task_id = 'refresh_staging_listing',
    sql = query_refresh_staging_listing,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_staging_nswlgacode = SnowflakeOperator(
    task_id = 'refresh_staging_code',
    sql = query_refresh_staging_code,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_staging_nswlgasuburb = SnowflakeOperator(
    task_id = 'refresh_staging_suburb',
    sql = query_refresh_staging_suburb,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_staging_censusg01 = SnowflakeOperator(
    task_id = 'refresh_staging_censusg01',
    sql = query_refresh_staging_g01,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_staging_censusg02 = SnowflakeOperator(
    task_id = 'refresh_staging_censusg02',
    sql = query_refresh_staging_g02,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

########################################################

refresh_datawarehouse_dim_listing = SnowflakeOperator(
    task_id = 'refresh_datawarehouse_dim_listing',
    sql = query_refresh_datawarehouse_dim_listing,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datawarehouse_dim_hosting = SnowflakeOperator(
    task_id = 'refresh_datawarehouse_dim_hosting',
    sql = query_refresh_datawarehouse_dim_hosting,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datawarehouse_dim_lga = SnowflakeOperator(
    task_id = 'refresh_datawarehouse_dim_lga',
    sql = query_refresh_datawarehouse_dim_lga,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datawarehouse_dim_censusg01 = SnowflakeOperator(
    task_id = 'refresh_datawarehouse_dim_censusg01',
    sql = query_refresh_datawarehouse_dim_censusg01,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datawarehouse_dim_censusg02 = SnowflakeOperator(
    task_id = 'refresh_datawarehouse_dim_censusg02',
    sql = query_refresh_datawarehouse_dim_censusg02,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datawarehouse_fact_table = SnowflakeOperator(
    task_id = 'refresh_datawarehouse_fact_table',
    sql = query_refresh_datawarehouse_fact_table,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

########################################################

refresh_datamart_dm_superhost_price = SnowflakeOperator(
    task_id = 'refresh_datamart_dm_superhost_price',
    sql = query_refresh_datamart_dm_superhost_price,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datamart_total_stays = SnowflakeOperator(
    task_id = 'refresh_datamart_total_stays',
    sql = query_refresh_datamart_total_stays,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datamart_percent_change = SnowflakeOperator(
    task_id = 'refresh_datamart_percent_change',
    sql = query_refresh_datamart_percent_change,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datamart_est_rev = SnowflakeOperator(
    task_id = 'refresh_datamart_est_rev',
    sql = query_refresh_datamart_est_rev,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datamart_dm_listing_neighbourhood = SnowflakeOperator(
    task_id = 'refresh_datamart_dm_listing_neighbourhood',
    sql = query_refresh_datamart_dm_listing_neighbourhood,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

###################################################################

refresh_datamart_dm_superhost_score= SnowflakeOperator(
    task_id = 'refresh_datamart_dm_superhost_score',
    sql = query_refresh_datamart_dm_superhost_score,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datamart_property_total_stays = SnowflakeOperator(
    task_id = 'refresh_datamart_property_total_stays',
    sql = query_refresh_datamart_property_total_stays,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datamart_property_percent_change = SnowflakeOperator(
    task_id = 'refresh_datamart_property_percent_change',
    sql = query_refresh_datamart_property_percent_change,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datamart_property_est_rev = SnowflakeOperator(
    task_id = 'refresh_datamart_property_est_rev',
    sql = query_refresh_datamart_property_est_rev,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datamart_dm_property_type = SnowflakeOperator(
    task_id = 'refresh_datamart_dm_property_type',
    sql = query_refresh_datamart_dm_property_type,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

refresh_datamart_dm_host_neighbourhood = SnowflakeOperator(
    task_id = 'refresh_datamart_dm_host_neighbourhood',
    sql = query_refresh_datamart_dm_host_neighbourhood,
    snowflake_conn_id = snowflake_conn_id,
    dag=dag 
)

########################################################

# Order of operations 

refresh_ext_table_raw_airbnb >> refresh_staging_listing
refresh_ext_table_raw_nswlgacode >> refresh_staging_nswlgacode
refresh_ext_table_raw_nswlgasuburb >> refresh_staging_nswlgasuburb
refresh_ext_table_raw_censusg01 >> refresh_staging_censusg01
refresh_ext_table_raw_censusg02 >> refresh_staging_censusg02

refresh_staging_listing >> refresh_datawarehouse_dim_listing
refresh_staging_listing >> refresh_datawarehouse_dim_hosting

refresh_staging_nswlgacode >> refresh_datawarehouse_dim_lga
refresh_staging_nswlgasuburb >> refresh_datawarehouse_dim_lga

refresh_staging_censusg01 >> refresh_datawarehouse_dim_censusg01
refresh_staging_censusg02 >> refresh_datawarehouse_dim_censusg02

refresh_staging_listing >> refresh_datawarehouse_fact_table
refresh_datawarehouse_dim_lga >> refresh_datawarehouse_fact_table
refresh_datawarehouse_dim_censusg02 >> refresh_datawarehouse_fact_table



refresh_datawarehouse_fact_table >> refresh_datamart_dm_superhost_price
refresh_datawarehouse_dim_listing >> refresh_datamart_dm_superhost_price
refresh_datawarehouse_dim_hosting >> refresh_datamart_dm_superhost_price

refresh_datawarehouse_dim_listing >> refresh_datamart_total_stays
refresh_datawarehouse_dim_listing >> refresh_datamart_percent_change
refresh_datawarehouse_dim_listing >> refresh_datamart_est_rev

refresh_datamart_dm_superhost_price >> refresh_datamart_dm_listing_neighbourhood
refresh_datamart_total_stays >> refresh_datamart_dm_listing_neighbourhood
refresh_datamart_percent_change >> refresh_datamart_dm_listing_neighbourhood
refresh_datamart_est_rev >> refresh_datamart_dm_listing_neighbourhood

refresh_datawarehouse_fact_table >> refresh_datamart_dm_superhost_score
refresh_datawarehouse_dim_listing >> refresh_datamart_dm_superhost_score
refresh_datawarehouse_dim_hosting >> refresh_datamart_dm_superhost_score

refresh_datawarehouse_dim_listing >> refresh_datamart_property_total_stays
refresh_datawarehouse_dim_listing >> refresh_datamart_property_percent_change
refresh_datawarehouse_dim_listing >> refresh_datamart_property_est_rev

refresh_datamart_dm_superhost_score >> refresh_datamart_dm_property_type
refresh_datamart_property_total_stays >> refresh_datamart_dm_property_type
refresh_datamart_property_percent_change >> refresh_datamart_dm_property_type
refresh_datamart_property_est_rev>> refresh_datamart_dm_property_type

refresh_datawarehouse_dim_listing >> refresh_datamart_dm_host_neighbourhood
refresh_datawarehouse_dim_hosting >> refresh_datamart_dm_host_neighbourhood
refresh_datawarehouse_dim_lga >> refresh_datamart_dm_host_neighbourhood



