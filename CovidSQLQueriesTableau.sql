USE projects;

# Queries used for Data visualization of COVID 19 Dataset Dashboard

# 1. Global death rate
SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 AS global_death_rate
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY day_date, SUM(new_cases);

# 2. Total Death Count per continent
SELECT location, SUM(new_deaths) as total_death_count
FROM covid_deaths
WHERE continent IS NULL
AND location NOT IN ('World', 'European Union', 'International')
AND location NOT LIKE '%income'
GROUP BY location
ORDER BY total_death_count DESC;

# 3. Countries with highest infection rates compared to population
SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 AS population_infection_rate
FROM covid_deaths
GROUP BY location, population
ORDER BY population_infection_rate desc;

# 4. World Vaccination Rate
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

# 5 Total Cases Vs Population
SELECT day_date, location, population, total_cases, (total_cases/population)*100 AS infection_rate
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY location, day_date, population, total_cases;
