WITH date_range AS (
    SELECT
        DATE('2017-10-30') + INTERVAL n DAY AS date
    FROM
        UNNEST(GENERATE_ARRAY(0, 1226)) AS n
)
SELECT
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(MONTH FROM date) AS month,
    FORMAT_DATE('%B', date) AS month_name,
    EXTRACT(WEEK FROM date) AS week_of_year,
    EXTRACT(DAY FROM date) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    FORMAT_DATE('%A', date) AS day_name,
    EXTRACT(DAYOFYEAR FROM date) AS day_of_year,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS weekday_weekend,
    DATE_TRUNC(date, WEEK(MONDAY)) AS week_start_date,
    DATE_TRUNC(date, MONTH) AS month_start_date,
    DATE_TRUNC(date, QUARTER) AS quarter_start_date,
    DATE_TRUNC(date, YEAR) AS year_start_date#,
    #CONCAT(EXTRACT(YEAR FROM date), '-', LPAD(EXTRACT(MONTH FROM date), 2, '0')) AS year_month
FROM
    date_range