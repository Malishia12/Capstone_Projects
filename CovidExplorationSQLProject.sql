CREATE DATABASE projects;

USE projects;
# CSV file was imported so "day_date" data type had to be uploaded as string, will transform data type after upload
CREATE TABLE covid_vaccinations(
	iso_code VARCHAR(100),
    continent VARCHAR(100),
    location VARCHAR(100),
    day_date VARCHAR(100),
    total_tests BIGINT,
    new_tests BIGINT,
    total_tests_per_thousand DOUBLE,
    new_tests_per_thousand DOUBLE,
    new_tests_smoothed INT,
    new_tests_smoothed_per_thousand DOUBLE,
    positive_rate DOUBLE,
    tests_per_case DOUBLE,
    tests_units VARCHAR(100),
    total_vaccinations BIGINT,
    people_vaccinated BIGINT,
    people_fully_vaccinated BIGINT,
    total_boosters BIGINT,
    new_vaccinations BIGINT,
    new_vaccinations_smoothed INT,
    total_vaccinations_per_hundred DOUBLE,
    people_vaccinated_per_hundred DOUBLE,
    people_fully_vaccinated_per_hundred DOUBLE,
    total_boosters_per_hundred DOUBLE,
    new_vaccinations_smoothed_per_million INT,
    new_people_vaccinated_smoothed INT,
    new_people_vaccinated_smoothed_per_hundred DOUBLE,
    stringency_index DOUBLE,
    population_density DOUBLE,
    median_age DOUBLE,
    aged_65_older DOUBLE,
    aged_70_older DOUBLE,
    gdp_per_capita DOUBLE,
    extreme_poverty DOUBLE,
    cardiovasc_death_rate DOUBLE,
    diabetes_prevalence DOUBLE,
    female_smokers DOUBLE,
    male_smokers DOUBLE,
    handwashing_facilities DOUBLE,
    hospital_beds_per_thousand DOUBLE,
    life_expectancy DOUBLE,
    human_development_index DOUBLE,
    excess_mortality_cumulative_absolute DOUBLE,
    excess_mortality_cumulative DOUBLE,
    excess_mortality DOUBLE,
    excess_mortality_cumulative_per_million DOUBLE
    );

CREATE TABLE covid_deaths(
	iso_code VARCHAR(100),
    continent VARCHAR(100),
    location VARCHAR(100),
    day_date VARCHAR(100),
    population BIGINT,
    total_cases BIGINT,
    new_cases INT,
    new_cases_smoothed DOUBLE,
    total_deaths INT,
    new_deaths INT,
    new_deaths_smoothed DOUBLE,
    total_cases_per_million DOUBLE,
    new_cases_per_million DOUBLE,
    new_cases_smoothed_per_million DOUBLE,
    total_deaths_per_million DOUBLE,
    new_deaths_per_million DOUBLE,
    new_deaths_smoothed_per_million DOUBLE,
    reproduction_rate DOUBLE,
    icu_patients INT,
    icu_patients_per_million DOUBLE,
    hosp_patients INT,
    hosp_patients_per_million DOUBLE,
    weekly_icu_admissions INT,
    weekly_icu_admissions_per_million DOUBLE,
    weekly_hosp_admissions INT,
    weekly_hosp_admissions_per_million DOUBLE
    );
# Display tables to make sure data was imported correctly
SELECT * 
FROM covid_vaccinations;
    
SELECT * 
FROM covid_deaths;
    
# Convert data types for "day_date" columns
UPDATE covid_vaccinations
SET day_date = str_to_date(day_date, "%m/%d/%Y");

ALTER TABLE covid_vaccinations
MODIFY day_date DATE;

UPDATE covid_deaths
SET day_date = str_to_date(day_date, "%m/%d/%Y");

ALTER TABLE covid_deaths
MODIFY day_date DATE;

# Preview data we will be working with in this section
SELECT location, day_date, total_cases, new_cases, total_deaths, population
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY location, day_date;

# Total Cases vs Total Deaths by location and dates
# Shows likelyhood of death if contracted in these countries
SELECT day_date, location, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_rate
FROM covid_deaths
WHERE continent IS NOT NULL
#WHERE location LIKE "%states"
ORDER BY location, day_date;

# Total Cases vs Population 
SELECT day_date, continent, population, total_cases, (total_cases/population)*100 AS infection_rate
FROM covid_deaths
WHERE continent IS NOT NULL
#WHERE location LIKE "%states"
ORDER BY location, day_date;

# Countries with highest infection rates compared to population
SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 AS highest_infection_rate
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY highest_infection_rate desc;
 
# Countries with highest death count per population
SELECT location, MAX(total_deaths) AS total_death_count
FROM covid_deaths
#WHERE location LIKE "%states"
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY total_death_count desc;
 
# Broken down by continent
SELECT continent, SUM(total_deaths) AS total_death_count
FROM covid_deaths
WHERE continent IS NOT NULL AND location NOT LIKE "%income%"
GROUP BY continent
ORDER BY total_death_count desc;

#Global death rate by day
SELECT day_date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 AS death_rate
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY day_date
ORDER BY day_date, SUM(new_cases);

#Global death rate
SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 AS global_death_rate
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY day_date, SUM(new_cases);

# Looking at total population vs total vaccinations
SELECT dea.continent, dea.location, dea.day_date, dea.population, vac.new_vaccinations, 
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.day_date) AS cumulative_vaccinations
FROM covid_deaths AS dea 
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
    AND dea.day_date = vac.day_date
WHERE dea.continent IS NOT NULL
ORDER BY dea.location, dea.day_date;

# Create a CTE to use the "cumulative_vaccinations" column in an operation
WITH WorldVacRate (continent, location, day_date, population, new_vaccinations, cumulative_vaccinations)
AS
(
SELECT dea.continent, dea.location, dea.day_date, dea.population, vac.new_vaccinations
, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.day_date) AS cumulative_vaccinations
FROM covid_deaths AS dea 
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
    AND dea.day_date = vac.day_date
WHERE dea.continent IS NOT NULL
)
SELECT *, (cumulative_vaccinations/population)*100 AS percent_vaccinated
FROM WorldVacRate;

# Same data fetch but using temp table
DROP TABLE IF EXISTS world_vac_rate;
CREATE TEMPORARY TABLE world_vac_rate( 
continent VARCHAR(100),
location VARCHAR(100),
day_date DATE,
population BIGINT,
new_vaccinations INT,
cumulative_vaccinations BIGINT
);
INSERT INTO world_vac_rate
SELECT dea.continent, dea.location, dea.day_date, dea.population, vac.new_vaccinations
, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.day_date) AS cumulative_vaccinations
FROM covid_deaths AS dea 
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
    AND dea.day_date = vac.day_date
WHERE dea.continent IS NOT NULL;

SELECT *, (cumulative_vaccinations/population)*100 AS percent_vaccinated
FROM world_vac_rate;

# Creating Views to store data for visualizations

CREATE VIEW Death_Rate AS
SELECT day_date, location, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_rate
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY location, day_date;

CREATE VIEW Infection_Rate AS
SELECT day_date, continent, population, total_cases, (total_cases/population)*100 AS infection_rate
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY location, day_date;

CREATE VIEW Highest_Infection_Rate AS
SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 AS highest_infection_rate
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY highest_infection_rate desc;

CREATE VIEW Highest_Death_Count AS
SELECT location, MAX(total_deaths) AS total_death_count
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY total_death_count desc;

CREATE VIEW Total_Deaths_by_Continent AS
SELECT continent, SUM(total_deaths) AS total_death_count
FROM covid_deaths
WHERE continent IS NOT NULL AND location NOT LIKE "%income%"
GROUP BY continent
ORDER BY total_death_count desc;

CREATE VIEW Global_DeathRate_by_Day AS
SELECT day_date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 AS death_rate
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY day_date
ORDER BY day_date, SUM(new_cases);

CREATE VIEW Global_DeathRate AS
SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 AS global_death_rate
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY day_date, SUM(new_cases);

CREATE VIEW World_Vac_Rate AS
SELECT dea.continent, dea.location, dea.day_date, dea.population, vac.new_vaccinations, 
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.day_date) AS cumulative_vaccinations
FROM covid_deaths AS dea 
JOIN covid_vaccinations AS vac
	ON dea.location = vac.location
    AND dea.day_date = vac.day_date
WHERE dea.continent IS NOT NULL;
