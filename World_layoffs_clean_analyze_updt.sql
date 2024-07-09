DROP DATABASE IF EXISTS world_layoffs;
CREATE DATABASE world_layoffs;
USE world_layoffs;

SELECT * FROM layoffs;

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging 
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;

--  data cleaning steps
-- 1. check for duplicates and remove if any
-- 2. standardize data and fix errors
-- 3. Look at null values & remove
-- 4. remove any columns and rows that are not necessary

-- Creating another table to add extra column row_num to check for duplicates
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2;

-- delete the records where row_num value is 2 , because those records are duplicated.alter

SELECT * FROM layoffs_staging2
WHERE row_num > 1;

DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- 2. Standardize Data
SELECT * FROM layoffs_staging2;

SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY industry;
-- here I find some null & blank values

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

SET SQL_SAFE_UPDATES = 0;

-- setting blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
ORDER BY industry;
-- now I'll populate those nulls 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
-- Now the null values are populated with the help of company field

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
ORDER BY industry;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;
-- Crypto has multiple different variations. I'll standardize that - (all to crypto)

SELECT distinct country
FROM layoffs_staging2
ORDER BY country; 
-- In country field i have two value i.e "United States" and some "United States."
-- I'll standardize this

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Now I'll fix date column
-- can use str_to _ate to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now I can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3) Check for Null values
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions 
-- all look normal.  I don't think I want to change that I like having them null because
--  it makes it easier for calculations during the EDA phase

-- 4. remove any columns and rows which are not necessary

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data I can't really use , where both total_laid_off &
-- percentage_laid_off are null
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

-- Now ill delete row_num as I dont need it now
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM layoffs_staging2;
-- This is my cleaned dataset

-- Exploratory Data Analysis
-- exploring the data and find trends or patterns

SELECT * FROM layoffs_staging2;

SELECT MAX(total_laid_off)
FROM layoffs_staging2;


-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;

-- if I order by funds_raised_millions we can see how big some of these companies were
SELECT * FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt looks like an EV company, Quibi! I recognize that company - wow raised like 2 billion dollars and went under - ouch

-- Companies with the biggest single Layoff
SELECT company, total_laid_off
FROM layoffs_staging
ORDER BY 2 DESC
LIMIT 5;

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- layoffs by location
SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- countries with highest layoffs
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

--  total layoffs in each year 
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`);

-- layoffs grouped by industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

--  layoffs for each company partitioned by each year & ranking them
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- 
WITH rolling_total AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates,total_off, SUM(total_off) OVER (ORDER BY dates ) as rolling_total_layoffs
FROM rolling_total
ORDER BY dates ASC;