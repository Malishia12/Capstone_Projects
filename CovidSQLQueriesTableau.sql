USE projects;

# Queries used for Data visualization of COVID 19 Dataset

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

#Total Death Count per continent
SELECT location, SUM(new_deaths) as total_death_count
FROM covid_deaths
WHERE continent IS NULL
AND location NOT IN ('World', 'European Union', 'International')
AND location NOT LIKE '%income'
GROUP BY location
ORDER BY total_death_count DESC;

# Countries with highest infection rates compared to population
SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 AS population_infection_rate
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY population_infection_rate desc;

#Global infection rate by day
SELECT location, population, day_date, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population))*100 AS daily_population_infection_rate
FROM covid_deaths
WHERE continent IS NOT NULL
GROUP BY location, population, day_date
ORDER BY daily_population_infection_rate desc;

# World Vaccination Rate
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


