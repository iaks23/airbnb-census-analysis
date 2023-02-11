CREATE DATABASE bde_at3;


USE bde_at3;

CREATE SCHEMA raw;

USE bde_at3.raw;


CREATE OR REPLACE STORAGE INTEGRATION GCP
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('');

DESCRIBE INTEGRATION GCP;


create or replace stage stage_gcp
storage_integration = GCP
url=''
;


create or replace file format file_format_csv 
type = 'CSV' 
field_delimiter = ',' 
skip_header = 1
NULL_IF = ('\\N', 'NULL', 'NUL', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;

list @stage_gcp;

//Creating all raw tables

create or replace external table raw.raw_airbnb
with location = @stage_gcp 
file_format = file_format_csv
pattern = '.*[0-9]_[0-9]*.csv';

create or replace external table raw.raw_g01census
with location = @stage_gcp 
file_format = file_format_csv
pattern = '.*2016Census_G01_NSW_LGA.csv';

create or replace external table raw.raw_g02census
with location = @stage_gcp 
file_format = file_format_csv
pattern = '.*2016Census_G02_NSW_LGA.csv';

create or replace external table raw.raw_suburb
with location = @stage_gcp 
file_format = file_format_csv
pattern = '.*NSW_LGA_SUBURB.csv';

create or replace external table raw.raw_code
with location = @stage_gcp 
file_format = file_format_csv
pattern = '.*NSW_LGA_CODE.csv';


Create schema staging;

create schema datawarehouse;
create schema datamart;




select * FROM raw_airbnb LIMIT 1;
--SELECT 
--SUBSTR(metadata$filename, 6, 7)
--FROM raw.raw_airbnb;

//Staging
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


CREATE OR REPLACE TABLE staging.staging_code AS
SELECT 
value:c1::varchar AS lga_code,
value:c2::varchar AS lga_name
FROM raw.raw_code;


CREATE OR REPLACE TABLE staging.staging_suburb AS
SELECT
value:c1::varchar AS lga_name,
value:c2::varchar AS suburb_name
FROM raw.raw_suburb;

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

//Verifying table creation
DESCRIBE TABLE staging.staging_listing;

//Data Warehousing & Dimension Table Creation using STAR SCHEMA WITH

//DIMENSION LISTING

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


//DIMENSION HOSTING

CREATE OR REPLACE TABLE datawarehouse.dim_hosting AS 
SELECT
host_id,
host_name,
TO_DATE(host_since,'DD/MM/YYYY') AS host_since,
host_superhost,
lower(host_neighbourhood) AS host_neighbourhood
FROM staging.staging_listing;


// DIMENSION LGA PK = LGA_CODE
CREATE OR REPLACE TABLE datawarehouse.dim_lga AS 
SELECT 
a.lga_code,
lower(a.lga_name) AS lga_name,
lower(b.suburb_name) AS suburb_name
FROM staging.staging_code as a 
LEFT JOIN staging.staging_suburb as b 
ON lower(a.lga_name) = lower(b.lga_name)
;

//DIMENSION CENSUS G01 PK = CLEAN_LGA_CODE
CREATE OR REPLACE TABLE datawarehouse.dim_censusg01 AS 
SELECT 
SUBSTR(lga_code, 4,5) AS clean_lga_code,*
FROM staging.staging_g01;

//DIMENSION CENSUS G02 PK = CLEAN_LGA_CODE

CREATE OR REPLACE TABLE datawarehouse.dim_censusg02 AS 
SELECT 
SUBSTR(lga_code, 4,5) AS clean_lga_code,*
FROM staging.staging_g02;





//FACT TABLE
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

SELECT * FROM datawarehouse.fact_table LIMIT 5;


//DATA MART CREATIONS

--Data Mart 1 Creation

--dm_listing_neighbourhood

--create temp table to calculate super_hsot rate

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



CREATE OR REPLACE TABLE datamart.total_stays AS
SELECT 
listing_neighbourhood, 
month_year,
COUNT(1) as total_listings, 
SUM(CASE WHEN has_availability='t' THEN 30-availability_30 ELSE null END) as total_number_of_stays
FROM datawarehouse.dim_listing a
GROUP BY listing_neighbourhood, month_year
ORDER BY listing_neighbourhood, month_year;



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
GROUP BY listing_neighbourhood, month_year;


CREATE OR REPLACE TABLE datamart.est_rev AS
SELECT 
listing_neighbourhood, 
month_year,
COUNT(DISTINCT listing_id) AS total_listings,
SUM(price*(30-availability_30)) as rev,
rev/total_listings as avg_rev
FROM datawarehouse.dim_listing
GROUP BY listing_neighbourhood, month_year
ORDER BY listing_neighbourhood, month_year;


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


--SELECT * FROM datamart.dm_listing_neighbourhood LIMIT 10;


--Data Mart 2 Creation

--dm_property_type

--first temp table to get superhost rate along with other info

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

--second temp view to get total stays

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


---temp table to calculate percent change

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
GROUP BY property_type, room_type, accommodates, month_year;

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
AND A.month_year = D.month_year;


SELECT * FROM datamart.dm_property_type
ORDER BY property_type, room_type, accommodates, month_year;

//DATA MART 3

--dim.hosting joined with dim.lgasuburb to get lga_name
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


select * from datamart.dm_host_neighbourhood limit 1;