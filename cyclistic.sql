-- Create table to import csv
CREATE TABLE tripdata_2022 (
	ride_id VARCHAR(100) PRIMARY KEY,
	rideable_type VARCHAR(50),
	started_at TIMESTAMP,
	ended_at TIMESTAMP,
	start_station_name VARCHAR(255),
	start_station_id VARCHAR(100),
	end_station_name VARCHAR(255),
	end_station_id VARCHAR(100),
	start_lat DOUBLE PRECISION,
	start_lng DOUBLE PRECISION,
	end_lat DOUBLE PRECISION,
	end_lng DOUBLE PRECISION,
	member_casual VARCHAR(50)
);

-- import data from csv into postgresql
COPY tripdata_2022(
	ride_id,
	rideable_type,
	started_at,
	ended_at,
	start_station_name,
	start_station_id,
	end_station_name,
	end_station_id,
	start_lat,
	start_lng,
	end_lat,
	end_lng,
	member_casual
)
FROM 'C:\Users\Public\SQL\202201-divvy-tripdata.csv'
DELIMITER ','
CSV HEADER;

-------------------Clean Data------------------------

-- look at data
SELECT *
FROM tripdata_2022
LIMIT 100;

-- Check whether each ride_id is unique and distinct
SELECT COUNT(*) AS total_trips, COUNT(DISTINCT ride_id)
FROM tripdata_2022;

-- Check for nulls
SELECT 
	SUM(CASE WHEN ride_id IS NULL THEN 1 ELSE 0 END) ride_id,
	SUM(CASE WHEN rideable_type IS NULL THEN 1 ELSE 0 END) rideable_type,
	SUM(CASE WHEN started_at IS NULL THEN 1 ELSE 0 END) started_at,
	SUM(CASE WHEN ended_at IS NULL THEN 1 ELSE 0 END) ended_at,
	SUM(CASE WHEN start_station_name IS NULL THEN 1 ELSE 0 END) start_station_name,
	SUM(CASE WHEN start_station_id IS NULL THEN 1 ELSE 0 END) start_station_id,
	SUM(CASE WHEN end_station_name IS NULL THEN 1 ELSE 0 END) end_station_name,
	SUM(CASE WHEN end_station_id IS NULL THEN 1 ELSE 0 END) end_station_id,
	SUM(CASE WHEN start_lat IS NULL THEN 1 ELSE 0 END) start_lat,
	SUM(CASE WHEN start_lng IS NULL THEN 1 ELSE 0 END) start_lng,
	SUM(CASE WHEN end_lat IS NULL THEN 1 ELSE 0 END) end_lat,
	SUM(CASE WHEN end_lng IS NULL THEN 1 ELSE 0 END) end_lng,
	SUM(CASE WHEN member_casual IS NULL THEN 1 ELSE 0 END) member_casual
FROM tripdata_2022

/* deal with nulls in sql start_station_name, start_station_id, end_station_name, 
end_station_id, end_lat, end_lng

Change nulls to 'non-station' for nulls in station names/id due to electric bikes 
are allowed to be dropped off at non-station locations,
drop nulls for classic and docked bikes */

WITH null_cte AS (
	SELECT *
	FROM tripdata_2022
	WHERE start_station_name IS NULL
	OR end_station_name IS NULL
)
SELECT rideable_type, COUNT(*)
FROM null_cte
GROUP BY rideable_type;

-- drop non-electric bike station nulls
DELETE FROM tripdata_2022
WHERE (
	start_station_name IS NULL
	OR start_station_id IS NULL
	OR end_station_name IS NULL
	OR end_station_id IS NULL)
AND rideable_type IN ('classic_bike', 'docked_bike')

UPDATE tripdata_2022
SET start_station_name = 'Non-station'
WHERE start_station_name IS NULL;

UPDATE tripdata_2022
SET start_station_id = 'Non-station'
WHERE start_station_id IS NULL;

UPDATE tripdata_2022
SET end_station_name = 'Non-station'
WHERE end_station_name IS NULL;

UPDATE tripdata_2022
SET end_station_id = 'Non-station'
WHERE end_station_id IS NULL;

-- Add columns: day_of_week, month, hour_of_day, ride_duration
SELECT 
	EXTRACT(dow FROM started_at) AS day_of_week,
	EXTRACT(hour FROM started_at) AS hour_of_day,
	EXTRACT(month FROM started_at) AS month,
	(ended_at - started_at) AS ride_duration
FROM tripdata_2022

-----------------Explore Data------------------

-- Most popular hour, day, month, include percentage
-- hour
SELECT 
	member_casual, 
	EXTRACT(hour FROM started_at) AS hour_of_day, 
	COUNT(*) AS num_of_rides,
	COUNT(*)/SUM(COUNT(*)) OVER(PARTITION BY member_casual) AS pct_hour
FROM tripdata_2022
GROUP BY member_casual, hour_of_day
ORDER BY member_casual, pct_hour DESC;

-- day of week
SELECT 
	member_casual, 
	EXTRACT(dow FROM started_at) AS day_of_week, 
	COUNT(*) AS num_of_rides,
	COUNT(*)/SUM(COUNT(*)) OVER(PARTITION BY member_casual) AS pct_dow
FROM tripdata_2022
GROUP BY member_casual, day_of_week
ORDER BY member_casual, pct_dow DESC;

-- month
SELECT 
	member_casual, 
	EXTRACT(month FROM started_at) AS month, 
	COUNT(*) AS num_of_rides,
	COUNT(*)/SUM(COUNT(*)) OVER(PARTITION BY member_casual) AS pct_month
FROM tripdata_2022
GROUP BY member_casual, month
ORDER BY member_casual, pct_month DESC;

-- types of bikes
SELECT member_casual, rideable_type, COUNT(*) AS num_of_rides,
	COUNT(*)/SUM(COUNT(*)) OVER(PARTITION BY member_casual) AS pct_rides
FROM tripdata_2022
GROUP BY member_casual, rideable_type

-- how often are the return location same as the starting location
SELECT
	member_casual,
	CASE WHEN start_station_name = end_station_name THEN 1 ELSE 0 END AS same_spot, 
	COUNT(*) AS num_of_rides,
	SUM(COUNT(*)) OVER(PARTITION BY member_casual) AS total_rides,
	COUNT(*)/SUM(COUNT(*)) OVER(PARTITION BY member_casual) AS pct_rides	
FROM tripdata_2022
WHERE start_station_name != 'Non-station'
GROUP BY member_casual, same_spot

-- avg ride duration
WITH time_cte AS (
SELECT member_casual,
	   started_at,
	   ended_at,
	   ended_at - started_at AS ride_duration
FROM tripdata_2022
)
SELECT member_casual, AVG(ride_duration) AS avg_duration
FROM time_cte
GROUP BY member_casual

-- total rides
SELECT member_casual, COUNT(ride_id)
FROM tripdata_2022
GROUP BY member_casual

-- daily weekday rides
WITH weekday_cte AS (
	SELECT member_casual, EXTRACT(dow FROM started_at) AS day_of_week
	FROM tripdata_2022
)

SELECT member_casual, COUNT(*) AS total_weekday_rides,
	SUM(COUNT(*)) OVER (PARTITION BY member_casual)/
	(
		SELECT COUNT(*)
		FROM (SELECT DISTINCT(DATE(started_at))
		FROM tripdata_2022) AS test
		WHERE EXTRACT(dow FROM date) BETWEEN 1 AND 5
	) AS avg_daily_weekday_rides
FROM weekday_cte
WHERE day_of_week BETWEEN 1 AND 5
GROUP BY member_casual;


-- daily weekend rides
WITH weekend_cte AS (
	SELECT member_casual, EXTRACT(dow FROM started_at) AS day_of_week
	FROM tripdata_2022
)

SELECT member_casual, COUNT(*) AS total_weekend_rides,
	SUM(COUNT(*)) OVER (PARTITION BY member_casual)/
	(
		SELECT COUNT(*)
		FROM (SELECT DISTINCT(DATE(started_at))
		FROM tripdata_2022) AS test
		WHERE EXTRACT(dow FROM date) IN (0,6)
	) AS avg_daily_weekend_rides
FROM weekend_cte
WHERE day_of_week IN (0,6)
GROUP BY member_casual

-- pct of weekend rides
WITH pct_week_rides AS (
	SELECT 
		member_casual, 
		EXTRACT(dow FROM started_at) AS day_of_week, 
		COUNT(*) AS num_of_rides,
		COUNT(*)/SUM(COUNT(*)) OVER(PARTITION BY member_casual) AS pct_dow
	FROM tripdata_2022
	GROUP BY member_casual, day_of_week
)
SELECT member_casual, SUM(pct_dow) AS pct_weekend_rides
FROM pct_week_rides
WHERE day_of_week IN (0,6)
GROUP BY member_casual