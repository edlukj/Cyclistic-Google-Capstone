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

-- look at data

SELECT *
FROM tripdata_2022
LIMIT 100;

--