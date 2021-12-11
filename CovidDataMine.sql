/*
Covid 19 DataSet Exploration 
Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
Original data from: https://ourworldindata.org/covid-deaths
*/

-- Select Data To Identify Target Columns
select*
from PortfolioProject..CovidDeaths
where continent is not null
order by 3,4


select location, date, total_cases, new_cases, total_deaths,population
from PortfolioProject..CovidDeaths
order by 1,2


-- Total Cases vs Total Deaths
-- Shows Percentage Of Death When Virus Contracted

select location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as Death_Percentage
from PortfolioProject..CovidDeaths
where location like '%kingdom%'
order by 1,2


-- Running Total Of New Cases Partitioned By Location

select location, population, date, cast(new_cases as int) as new_cases, sum(cast(new_cases as int)) OVER (partition by location order by date) as Cumulative_Cases
from PortfolioProject..CovidDeaths
where continent is not null
order by 1,2,3

-- -- Running Total Of New_Deaths Partitioned By Location

select location, population, date, cast(new_deaths as int) as new_deaths, sum(convert(int, new_deaths)) OVER (partition by location order by date) as Cumulative_Deaths
from PortfolioProject..CovidDeaths
where continent is not null
order by 1,2,3


-- Using Temp Table For Calculation (On New_Deaths & New_Cases) Partitioned By Location


drop table if exists #Percent_of_Death
create table #Percent_of_Death
(
location nvarchar(255),
population numeric,
date datetime,
new_cases numeric,
new_deaths numeric,
Cumulative_Cases numeric,
Cumulative_Deaths numeric
)

insert into #Percent_of_Death
select location, population, date, cast(new_cases as int) as new_cases, cast(new_deaths as int) as new_deaths, 
sum(convert(float, new_cases)) OVER (partition by location order by date) as Cumulative_Cases, 
sum(convert(float, new_deaths)) OVER (partition by location order by date) as Cumulative_Deaths
from PortfolioProject..CovidDeaths
where continent is not null


select *, (Cumulative_Deaths/Cumulative_Cases)*100 as Rolling_Death_Percentage
from #Percent_of_Death
order by 1,2


-- Total Cases vs Population
-- Shows What Percentage Of Population Is Infected

select location, date, total_cases, population, (total_cases/population)*100 as Population_Infection_Percentage
from PortfolioProject..CovidDeaths
--where location like '%kingdom%' 
order by 1,2


-- Countries With The Highest Infection Rate With Regards To Population

select location, population, MAX(total_cases) as Highest_Infection, MAX((total_cases/population)*100) as Population_Infection_Percentage
from PortfolioProject..CovidDeaths
group by location, population
order by Population_Infection_Percentage desc



-- Countries With The Highest Deaths By Location

select location, MAX(cast(total_Deaths as int)) as Highest_Total_Deaths
from PortfolioProject..CovidDeaths
where continent is not null
group by location
order by Highest_Total_Deaths desc


-- Highest Deaths Query By Continents

select continent, MAX(cast(total_Deaths as int)) as Highest_Total_Deaths
from PortfolioProject..CovidDeaths
where continent is not null
group by continent
order by Highest_Total_Deaths desc


-- Global Values

select sum(new_cases) as Total_Cases, sum(cast(new_deaths as int)) as Total_Deaths, sum(cast(new_deaths as int))/sum(new_cases)*100 as Death_Percentage
from PortfolioProject..CovidDeaths
where Location = 'World'
order by 1,2


-- Total Population vs Vaccinations
-- Shows The Percentage Of The Population That Has Recieved One Covid Vaccine

select cd.continent, cd.location, cd.date, cd.population, cast(cv.new_vaccinations as int) as new_vaccinations
, sum(convert(BIGINT,cv.new_vaccinations)) OVER (partition by cd.location order by cd.location, cd.date) as Cumulative_Vaccinations
from PortfolioProject..CovidDeaths cd
join PortfolioProject..CovidVaccinations cv
	on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null
order by 1,2,3


-- Using CTE For Calculation (On Population Vaccinated) With Partition By


with pvac (continent, location, date, population, new_vacination, Cumulative_Vaccinations)
as
(
select cd.continent, cd.location, cd.date, cd.population, cast(cv.new_vaccinations as int) as new_vaccinations
, sum(convert(BIGINT,cv.new_vaccinations)) OVER (partition by cd.location order by cd.location, cd.date) as Cumulative_Vaccinations
from PortfolioProject..CovidDeaths cd
join PortfolioProject..CovidVaccinations cv
	on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null
)
select *, (Cumulative_Vaccinations/population)*100 as percentage_pop_vaccinations
from pvac


-- Using Temp Table for Calculation (On Population Vaccinated) With Partition By


drop table if exists #Percent_of_PopVac
create table #Percent_of_PopVac
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
Cumulative_Vaccinations numeric
)

insert into #Percent_of_PopVac
select cd.continent, cd.location, cd.date, cd.population, cast(cv.new_vaccinations as int) as new_vaccinations
, sum(convert(BIGINT,cv.new_vaccinations)) OVER (partition by cd.location order by cd.location, cd.date) as Cumulative_Vaccinations
from PortfolioProject..CovidDeaths cd
join PortfolioProject..CovidVaccinations cv
	on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null


select *, (Cumulative_Vaccinations/population)*100 as percentage_pop_vaccinations
from #Percent_of_PopVac
--where location like '%kingdom%'



-- Temp Table Of Percentage Of Population That Have Not Had One Vaccination

drop table if exists #Precent_Vaccination
create table #Precent_Vaccination
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
deaths numeric,
vaccinations numeric,
)

insert into #Precent_Vaccination
select cd.continent, cd.location, cd.date, cd.population, cast(cd.total_deaths as float) deaths, cast(cv.total_vaccinations as float) as vaccinations
from PortfolioProject..CovidDeaths cd
join PortfolioProject..CovidVaccinations cv
	on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null
order by 1,2,3


select *, (vaccinations/population)*100 as "% 1st shot"
from #Precent_Vaccination
order by 1,2



-- Percentage Of Population That Have Not Had Two Vaccinations

select cd.continent, cd.location, cd.date, cd.population, cast(cd.total_deaths as int) deaths, cast(cv.total_boosters as int) boosters 
from PortfolioProject..CovidDeaths cd
join PortfolioProject..CovidVaccinations cv
	on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null
order by 1,2,3

-- Temp Table Of Percentage Of Population That Have Not Had Two Vaccinations

drop table if exists #Precent_booster
create table #Precent_booster
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
deaths numeric,
boosters numeric,
)

insert into #Precent_booster
select cd.continent, cd.location, cd.date, cd.population, cast(cd.total_deaths as int) deaths, cast(cv.total_boosters as int) boosters
from PortfolioProject..CovidDeaths cd
join PortfolioProject..CovidVaccinations cv
	on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null
order by 1,2,3


select *, (boosters/population)*100 as "% 2nd shot"
from #Precent_booster
order by 1,2



-- Comparing The Percentage Of Population That Have Not Had One Vaccination With Those Not Having Two

select a.continent, a.location, a.date, a.population, a.deaths, a.vaccinations, b.boosters, (1-(a.vaccinations/a.population))*100 as none_vac_percent, (1-(b.boosters/a.population))*100 as none_booster_percent
from #Precent_Vaccination a
join #Precent_booster b
	on a.location = b.location and a.date = b.date
where a.continent is not null
order by 2,3 


-- Temp Table Of The None Vaccinated Population By Location

drop table if exists #Non_vac_pop
create table #Non_vac_pop
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
deaths numeric,
boosters numeric,
vaccinations numeric,
)

insert into #Non_vac_pop
select a.continent, a.location, a.date, a.population, a.deaths, a.vaccinations, b.boosters
from #Precent_Vaccination a
join #Precent_booster b
	on a.location = b.location and a.date = b.date
where a.continent is not null
order by 1,2,3 

select*, (1-(vaccinations/population))*100 as none_vac_percent, (1-(boosters/population))*100 as none_booster_percent
from #Non_vac_pop




-- Creating Views For Visualisation

--	1) Percentage Of Popultion That Have Been Vaccinated
create view Percent_of_PopVac as
select cd.continent, cd.location, cd.date, cd.population, cast(cv.new_vaccinations as int) as new_vaccinations
, sum(convert(BIGINT,cv.new_vaccinations)) OVER (partition by cd.location order by cd.location, cd.date) as Cumulative_Vaccinations
from PortfolioProject..CovidDeaths cd
join PortfolioProject..CovidVaccinations cv
	on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null
order by Location, Population, date

-- 2) Percentage Of Death When Virus Is Contracted (Global)
create view Percent_of_PopVac as
select sum(new_cases) as Total_Cases, sum(cast(new_deaths as int)) as Total_Deaths, sum(cast(new_deaths as int))/sum(new_cases)*100 as Death_Percentage
from PortfolioProject..CovidDeaths
where Location = 'World'


-- 3) Death Count When Virus Is Contracted
create view Percent_of_Death as
Select location, sum(cast(new_deaths as int)) as Death_Count
from PortfolioProject..CovidDeaths
where continent is null
and location not in ('World','European Union', 'International', 'High income', 'Upper middle income', 'Lower middle income', 'Low income')
group by location
order by Death_Count desc

-- 4) Cumulative Deaths When Virus Is Contracted By Locations
create view Percent_of_Death as
select location, population, date, cast(new_cases as int) as new_cases, cast(new_deaths as int) as new_deaths, 
sum(convert(float, new_cases)) OVER (partition by location order by date) as Cumulative_Cases, 
sum(convert(float, new_deaths)) OVER (partition by location order by date) as Cumulative_Deaths
from PortfolioProject..CovidDeaths
where continent is not null
and location not in ('World','European Union', 'International')
order by Cumulative_Deaths desc

-- 5) Percentage Of Population Infected
create view Percent_of_Death as
select location, date, total_cases, population, (total_cases/population)*100 as Population_Infection_Percentage
from PortfolioProject..CovidDeaths
--where location like '%kingdom%'
Group by Location, Population, date
order by Population_Infection_Percentage desc

-- 6) Percentage Of Population Recieved Booster
create view Precent_booster as
select cd.continent, cd.location, cd.date, cd.population, cast(cd.total_deaths as int) deaths, cast(cv.total_boosters as int) boosters
from PortfolioProject..CovidDeaths cd
join PortfolioProject..CovidVaccinations cv
	on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null
order by cd.location desc

-- 7) Percentage Of None Vaccinated Population 
create view Non_vac_population as
select cd.continent, cd.location, cd.date, cd.population, cd.total_deaths, cv.total_vaccinations, cv.total_boosters, (1-(cv.total_vaccinations/cd.population))*100 as none_vac_percent, (1-(cv.total_boosters/cd.population))*100 as none_booster_percent
from PortfolioProject..CovidDeaths cd
join PortfolioProject..CovidVaccinations cv
	on cd.location = cv.location and cd.date = cv.date
where cd.continent is not null
order by cd.location desc