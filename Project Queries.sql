------TASK 1------
SELECT 
	COUNT(country_code) AS total_stores,
	country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_stores DESC;


------TASK 2------
SELECT 
	COUNT(locality) AS total_stores,
	locality
FROM dim_store_details
GROUP BY locality
ORDER BY total_stores DESC
LIMIT 7;


------TASK 3------
SELECT dim_date_times.month, SUM(orders_table.product_quantity * dim_products.product_price_£) AS total_sales
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY dim_date_times.month
ORDER BY total_sales DESC
LIMIT 6

SELECT *
FROM dim_store_details

CREATE TABLE dim_store_details_test AS
SELECT * FROM dim_store_details;

SELECT *
FROM dim_store_details_test


ALTER TABLE dim_store_details_test
ADD COLUMN online_offline VARCHAR(255);
UPDATE dim_store_details_test
SET online_offline = CASE 
                        WHEN store_type = 'Web Portal' THEN 'Web'
                        ELSE 'Offline'
                     END;
				
				
------TASK 4------					 
SELECT dim_store_details_test.online_offline, SUM(orders_table.product_quantity) AS product_quantity_count
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details_test ON orders_table.store_code = dim_store_details_test.store_code
GROUP BY dim_store_details_test.online_offline
ORDER BY product_quantity_count DESC


------TASK 5------
WITH TotalSalesPerStore AS (
	SELECT dim_store_details.store_type,
	SUM(orders_table.product_quantity * dim_products.product_price_£) AS total_sales
	FROM orders_table
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
	JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
	GROUP BY dim_store_details.store_type
)
SELECT store_type,
		ROUND(total_sales::numeric, 2) AS total_sales,
		ROUND((total_sales::numeric / CAST(SUM(total_sales) OVER() AS numeric)) * 100,2) AS percentage_total
FROM TotalSalesPerStore
ORDER BY total_sales DESC


------TASK 6------

SELECT dim_date_times.month,
		dim_date_times.year,
		SUM(orders_table.product_quantity * dim_products.product_price_£) AS total_sales
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY dim_date_times.month, dim_date_times.year
ORDER BY total_sales DESC
LIMIT 10


------TASK 7------
SELECT SUM(staff_numbers) AS total_staff,
		country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff DESC


------TASK 8------
SELECT dim_store_details.country_code,
		dim_store_details.store_type,
		SUM(orders_table.product_quantity * dim_products.product_price_£) AS total_sales
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.country_code, dim_store_details.store_type
ORDER BY total_sales DESC


------TASK 9------
WITH CombinedTimestamp AS (
    SELECT 
		year,
        CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'), ' ', timestamp)::timestamp AS combined_timestamp
    FROM 
        dim_date_times
	),
	NextTimeStamp AS (
	SELECT
		year,
		combined_timestamp,
		LEAD(combined_timestamp) OVER (ORDER BY combined_timestamp) AS next_day_timestamp
	FROM 
		CombinedTimestamp
	),
	DifferenceTimeStamps AS (
	SELECT
		year,
		next_day_timestamp - combined_timestamp AS time_stamp_diff
	FROM
		NextTimeStamp
)
SELECT
	year,
	AVG(time_stamp_diff) AS actual_time_taken
FROM
	DifferenceTimeStamps
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5;
		
