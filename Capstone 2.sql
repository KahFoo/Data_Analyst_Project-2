-- Import data into SQL

-- 1. Create table covid_data

	DROP TABLE IF EXISTS covid_data;
	CREATE TABLE covid_data(
		iso_code text,
		continent text,	
		location text,
		date date,
		total_cases bigint,
		new_cases bigint,
		new_cases_smoothed text,
		total_deaths numeric,
		new_deaths numeric,
		new_deaths_smoothed numeric,
		total_cases_per_million numeric,
		new_cases_per_million text,
		new_cases_smoothed_per_million numeric,
		total_deaths_per_million numeric,
		new_deaths_per_million numeric,
		new_deaths_smoothed_per_million numeric,
		reproduction_rate numeric,
		icu_patients numeric,
		icu_patients_per_million numeric,
		hosp_patients numeric,
		hosp_patients_per_million numeric,
		weekly_icu_admissions numeric,
		weekly_icu_admissions_per_million numeric,
		weekly_hosp_admissions text,
		weekly_hosp_admissions_per_million numeric,
		total_tests numeric,	
		new_tests text,
		total_tests_per_thousand text,
		new_tests_per_thousand text,
		new_tests_smoothed text,
		new_tests_smoothed_per_thousand text,
		positive_rate text,
		tests_per_case text,
		tests_units text,	
		total_vaccinations float,
		people_vaccinated float,
		people_fully_vaccinated float,
		total_boosters float,
		new_vaccinations float,
		new_vaccinations_smoothed text,
		total_vaccinations_per_hundred text,
		people_vaccinated_per_hundred text,
		people_fully_vaccinated_per_hundred text,
		total_boosters_per_hundred text,
		new_vaccinations_smoothed_per_million text,
		new_people_vaccinated_smoothed text, 
		new_people_vaccinated_smoothed_per_hundred text,
		stringency_index text,
		population_density text,
		median_age text,
		aged_65_older text,
		aged_70_older text,
		gdp_per_capita numeric,
		extreme_poverty text,
		cardiovasc_death_rate text,
		diabetes_prevalence text,
		female_smokers text,	
		male_smokers text,	
		handwashing_facilities text,	
		hospital_beds_per_thousand text,	
		life_expectancy text,	
		human_development_index text,	
		population bigint,	
		excess_mortality_cumulative_absolute text,	
		excess_mortality_cumulative	text,
		excess_mortality text,	
		excess_mortality_cumulative_per_million text
	);
	
-- 2. Copy Covid_Data into table------------------------------------------------------------------
	
	COPY covid_data
	FROM 'C:\Users\Chafu\Downloads\SCPT-Data Analyst Course Progress\03 Analyze Data With SQL\capstone 2\Option 2 COVID 19 Data Set\owid-covid-data_refined.csv' 
	WITH DELIMITER ',' HEADER CSV;
	
-- 3. Create lookup table, table (3NF)------------------------------------------------------------- 

-- i) Create table continent
	DROP TABLE IF EXISTS Continent CASCADE;
	CREATE TABLE continent 
		(
		continent_id serial PRIMARY KEY,
		continent VARCHAR(255),
		continent_population BIGINT NOT NULL
		);

	-- populate table for population by continent from country table
	INSERT INTO Continent (continent, continent_population)
	SELECT c.continent, SUM(c.population) AS population
	FROM covid_data c
	WHERE continent IS NOT NULL
	GROUP BY c.continent;

-- ii) Create the Country table
	DROP TABLE IF EXISTS Country CASCADE;
	CREATE TABLE Country 
		(
		country_id TEXT PRIMARY KEY,
		country_name VARCHAR(100),
		continent_id INT NOT NULL REFERENCES Continent(continent_id),
		population BIGINT NOT NULL,
		gdp_per_capita NUMERIC(10, 2)
		);

	-- Insert data into the Country table
	INSERT INTO Country (country_id, country_name, continent_id, population, gdp_per_capita)
	SELECT DISTINCT iso_code, location, c.continent_id, MAX(population), MAX(gdp_per_capita)
	FROM covid_data
	JOIN Continent c ON covid_data.continent = c.continent
	GROUP BY iso_code, location, c.continent_id;
	
-- iii) Create the Cases table
	DROP TABLE IF EXISTS Covid_Cases CASCADE;
	CREATE TABLE Covid_Cases 
		(
		cases_id SERIAL PRIMARY KEY,
		country_id TEXT NOT NULL REFERENCES Country(country_id),
		date DATE NOT NULL,
		total_cases BIGINT,
		new_cases INT
		);

	-- Insert data into the Cases table
	INSERT INTO Covid_Cases (country_id, date, total_cases, new_cases)
	SELECT DISTINCT iso_code, date, MAX(total_cases), MAX(new_cases)
	FROM covid_data
	WHERE iso_code NOT LIKE 'OWID%'
	GROUP BY iso_code, date;

-- iv) Create the Death table
	DROP TABLE IF EXISTS Covid_Death CASCADE;
	CREATE TABLE Covid_Death 
		(
		death_id SERIAL PRIMARY KEY,
		country_id TEXT NOT NULL REFERENCES Country(country_id),
		date DATE NOT NULL,
		total_deaths INT,
		new_deaths INT
		);

	-- Insert data into the Death table
	INSERT INTO Covid_Death (country_id, date, total_deaths, new_deaths)
	SELECT DISTINCT iso_code, date, MAX(total_deaths), MAX(new_deaths)
	FROM covid_data
	WHERE iso_code NOT LIKE 'OWID%'
	GROUP BY iso_code, date;

-- V) Create the Vaccination table
	DROP TABLE IF EXISTS Covid_vaccination CASCADE;
	CREATE TABLE Covid_vaccination 
		(
		vaccination_id SERIAL PRIMARY KEY,
		country_id TEXT NOT NULL REFERENCES Country(country_id),
		date DATE NOT NULL,
		total_vaccinations BIGINT,
		people_vaccinated BIGINT,
		people_fully_vaccinated BIGINT,
		total_boosters BIGINT,
		new_vaccinations BIGINT
		);

	-- Insert data into the Vaccination table
	INSERT INTO Covid_vaccination (country_id, date, total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters, new_vaccinations)
	SELECT DISTINCT iso_code, date, MAX(total_vaccinations), MAX(people_vaccinated), MAX(people_fully_vaccinated), MAX(total_boosters), MAX(new_vaccinations)
	FROM covid_data
	WHERE iso_code NOT LIKE 'OWID%'
	GROUP BY iso_code, date;

-- 4. Analyze Data

-- i) TOTAL Cases VS Population By Country

	WITH global_infection_by_country_daily (country_id, country_name, date, population, total_cases, percent_population_infected) AS
		(
		SELECT a.country_id, a.country_name, b.date, a.population, b.total_cases, (b.total_cases*1.0/a.population*1.0)*100 AS percent_population_infected 
		FROM Country a
		LEFT JOIN Covid_Cases b
		ON a.country_id = b.country_id
		ORDER BY 2,3 DESC
		)
	SELECT country_id, country_name, MAX(population) population, MAX(total_cases) total_cases, MAX(percent_population_infected) percent_of_population_infected 
	FROM global_infection_by_country_daily
	WHERE percent_population_infected IS NOT NULL --AND country LIKE 'Singapore%'
	GROUP BY 1,2
	ORDER BY 4 DESC
	LIMIT 10;
	
-- ii) TOTAL Death VS Population By Country
	
	WITH global_death_by_country_daily (country_id, country_name, date, population, total_deaths, percent_population_death) AS
		(
		SELECT a.country_id, a.country_name, b.date, a.population, b.total_deaths, (b.total_deaths*1.0/a.population*1.0)*100.0 AS percent_population_death 
		FROM Country a
		LEFT JOIN Covid_Death b
		ON a.country_id = b.country_id
		ORDER BY 1,2 DESC
		)
	SELECT country_id, country_name, MAX(population) population, MAX(total_deaths) total_deaths, MAX(percent_population_death) percent_population_death 
	FROM global_death_by_country_daily
	WHERE percent_population_death IS NOT NULL
	GROUP BY 1,2
	ORDER BY 4 DESC
	LIMIT 10;

-- iii) Death Percentage From Covid By Country

	WITH global_infection_by_country_daily (country_id, country_name, date, population, total_cases, percent_population_infected) AS
		(
		SELECT a.country_id, a.country_name, b.date, a.population, b.total_cases, (b.total_cases*1.0/a.population*1.0)*100 AS percent_population_infected 
		FROM Country a
		LEFT JOIN Covid_Cases b
		ON a.country_id = b.country_id
		ORDER BY 2,3 DESC
		)
		,
		global_infection_by_country_total AS
		(
		SELECT country_id, country_name, MAX(population) population, MAX(total_cases) total_cases, MAX(percent_population_infected) percent_of_population_infected 
		FROM global_infection_by_country_daily
		WHERE percent_population_infected IS NOT NULL 
		GROUP BY 1,2
		ORDER BY 4 DESC
		)
		,
		global_death_by_country_daily (country_id, country_name, date, population, total_deaths, percent_population_death) AS
		(
		SELECT a.country_id, a.country_name, b.date, a.population, b.total_deaths, (b.total_deaths*1.0/a.population*1.0)*100 AS percent_population_death 
		FROM Country a
		LEFT JOIN Covid_Death b
		ON a.country_id = b.country_id
		ORDER BY 1,2 DESC
		)
		,
		global_death_by_country_total AS
		(
		SELECT country_id, country_name, MAX(population) population, MAX(total_deaths) total_deaths, MAX(percent_population_death) percent_population_death 
		FROM global_death_by_country_daily
		WHERE percent_population_death IS NOT NULL
		GROUP BY 1,2
		ORDER BY 4 DESC
		)
	SELECT a.country_id, a.country_name, a.total_cases, b.total_deaths, (b.total_deaths*1.0/a.total_cases*1.0)*100 as DeathPercentage
	From global_infection_by_country_total a
	LEFT JOIN global_death_by_country_total b
	ON a.country_id = b.country_id
	WHERE total_deaths IS NOT NULL
	ORDER BY 4 DESC
	LIMIT 10;

-- iv) Total Population vs Vaccinations

	WITH global_vaccination_by_country_daily (country_id, country_name, date, population, total_vaccinated, percent_population_vaccinated) AS
		(
		SELECT a.country_id, a.country_name, b.date, a.population, b.people_fully_vaccinated, (b.people_fully_vaccinated*1.0/a.population*1.0)*100 AS percent_population_vaccinated 
		FROM Country a
		LEFT JOIN Covid_vaccination b
		ON a.country_id = b.country_id
		WHERE people_fully_vaccinated IS NOT NULL
		ORDER BY people_fully_vaccinated DESC
		)
	SELECT country_id, country_name, MAX(population) population, MAX(total_vaccinated) total_vaccinated, MAX(percent_population_vaccinated) percent_population_vaccinated 
	FROM global_vaccination_by_country_daily
	WHERE global_vaccination_by_country_daily IS NOT NULL
	GROUP BY 1,2
	ORDER BY 4 DESC
	LIMIT 10;


