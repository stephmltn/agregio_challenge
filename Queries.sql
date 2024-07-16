
------ Raw tables

CREATE TABLE raw_production (
	production_id SERIAL PRIMARY KEY, 
	site_id INT NOT NULL,
	sensor_id INT NOT NULL, 
	production_date DATE, 
	energy_generated DECIMAL(10, 2), 
	energy_type VARCHAR(50) 
);

CREATE TABLE raw_sites (
	site_id SERIAL PRIMARY KEY, 
	site_name VARCHAR(100), 
	location VARCHAR(100),
	installation_date DATE, 
	total_capacity DECIMAL(10, 2) 
);

CREATE TABLE raw_sensors ( 
	sensor_id SERIAL PRIMARY KEY, 
	site_id INT NOT NULL, 
	sensor_type VARCHAR(50), 
	installation_date DATE, 
	status VARCHAR(50) 
);


-- Validate tables

SELECT *
FROM raw_production;

SELECT *
FROM raw_sites;

SELECT *
FROM raw_sensors;


------ SRC tables

---- Production table

-- Create src production table

CREATE TABLE src_production AS (
	SELECT * FROM raw_production
);

SELECT COUNT(1) FROM src_production;


-- Study duplicates

SELECT site_id, sensor_id, production_date, COUNT(1)
FROM raw_production
GROUP BY 1,2,3 ORDER BY 4 DESC;

'''
3	7	"2023-01-09"	2
16	72	"2023-03-03"	2
14	68	"2023-06-09"	2
'''

SELECT *
FROM raw_production
WHERE site_id IN (3,16,14)
AND sensor_id IN (7,72,68);


-- Remove duplicates

WITH duplicates AS (
	SELECT production_id 
	FROM (
		SELECT production_id, 
		ROW_NUMBER() OVER (PARTITION BY site_id, sensor_id, production_date ORDER BY production_id DESC) AS row_num 
		FROM raw_production
	) subquery 
	WHERE subquery.row_num > 1
) 
DELETE FROM src_production 
WHERE production_id IN (SELECT production_id FROM duplicates);

SELECT COUNT(1) FROM src_production;

-- Remove rows where one id is missing

SELECT * FROM src_production
WHERE site_id IS NULL
OR sensor_id IS NULL
OR production_date IS NULL;

DELETE FROM src_production 
WHERE site_id IS NULL
OR sensor_id IS NULL
OR production_date IS NULL;

-- Validate energy type values
SELECT energy_type, COUNT(1) FROM src_production
GROUP BY 1

-- Validate energy generated values
SELECT * FROM src_production
ORDER BY energy_generated

-- Final src production table: 199 rows

SELECT * FROM src_production;


---- Sites table

SELECT * FROM raw_sites;

SELECT site_name, COUNT(1) FROM raw_sites 
GROUP BY 1 ORDER BY 2 DESC;
-- Site5 count 2

-- TODO clean table
WITH duplicate_names AS (
	SELECT site_name
	FROM (
		SELECT site_name, COUNT(1) AS nb
		FROM raw_sites 
		GROUP BY 1 ORDER BY 2 DESC
	)
	WHERE nb > 1
),
duplicates AS (
	SELECT *
	FROM raw_sites
	WHERE site_name in (SELECT * FROM duplicate_names)
),
oldest_records AS (
	SELECT *
	FROM duplicates	
)


-- Create src sites table
-- TODO needs to be cleaned, remove duplicates

CREATE TABLE src_sites AS (
	SELECT * FROM raw_sites
);



---- Sensors table

SELECT * FROM raw_sensors

-- Sensor type
SELECT sensor_type, COUNT(1) 
FROM raw_sensors
GROUP BY 1

-- Installation date
-- Could check for irrealistic dates
SELECT installation_date IS NULL, COUNT(1) 
FROM raw_sensors
GROUP BY 1

-- Status
SELECT status, COUNT(1) 
FROM raw_sensors
GROUP BY 1

-- Create src sensors table

CREATE TABLE src_sensors AS (
	SELECT * FROM raw_sensors
	WHERE installation_date IS NOT NULL
);

SELECT COUNT(1) FROM raw_sensors -- 100 rows

SELECT COUNT(1) FROM src_sensors -- 99 rows


------ Tables DIM

---- Table dim_sites

CREATE TABLE dim_sites AS (
	-- CTE with sensor counts
	WITH sensors_per_site AS (
		SELECT sites.site_id,
			COUNT(sensors.sensor_id) AS nb_sensors,
			SUM(CASE WHEN sensors.status = 'active' THEN 1 ELSE 0 END) AS nb_active
		FROM src_sites AS sites
		LEFT JOIN src_sensors AS sensors ON 1=1
			AND sites.site_id = sensors.site_id
		GROUP BY sites.site_id
	)
	SELECT sites.*,
		CASE WHEN nb_sensors = nb_active THEN 'Operational'
			WHEN nb_sensors > nb_active AND nb_active > 0 THEN 'Partially Operational'
			WHEN nb_active = 0 THEN 'Non Operational'
		END
		AS site_status
	FROM src_sites AS sites
	LEFT JOIN sensors_per_site AS sensors ON 1=1
		AND sites.site_id = sensors.site_id
);

SELECT * FROM dim_sites;


---- Table dim_sensors

CREATE TABLE dim_sensors AS (
	SELECT sensors.*,
		EXTRACT(DAY FROM NOW() - installation_date) AS sensor_age
	FROM src_sensors AS sensors
)


------ FCT tables

-- fct_production table

CREATE TABLE fct_production AS (
	SELECT prod.*,
		sites.total_capacity,
		ROUND(energy_generated / total_capacity,2) AS energy_efficiency,
		SUM(energy_generated) OVER (PARTITION BY sensor_id, energy_type) AS cumulative_energy
	FROM src_production prod
	LEFT JOIN dim_sites sites ON 1=1
		AND sites.site_id = prod.site_id
)

SELECT COUNT(1) FROM fct_production; -- 199 rows
SELECT * FROM fct_production;


------ MART tables

---- mart_production_summary table

-- Selectionner les attributs nécessaires
-- Somme pour total_energy_generated
-- Window function Average pour average_daily_energy (en scannant les datapoints quotidients)

---- mart_site_performance

-- Attribut, somme et quotient. Semble straightforward.


------ Analyses

-- Attributs, puis Average sur 10 latest records (option de filtrer avec Window function)





