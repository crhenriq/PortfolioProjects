-- Exploratory Data Analysis

-- In this project, we study the economic slowdown experienced by tech firms around the globe.
-- We analyze the recent tech turmoil and discover useful insights using the following data set: https://www.kaggle.com/datasets/swaptr/layoffs-2022/data
-- This project displays the exploratory data analysis of the table created in the Data Cleaning Project.

-- this shows table from Data Cleaning Project
SELECT *
FROM layoffs_staging2;

-- This shows us the max amount of people laid off in a company in one day and the % of that company
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- if % laid off  = 1, it means 100% of the company was laid off
-- we're looking to see which companies lost all their employees
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised DESC;

-- this shows the total # of ppl laid off per company for the table
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- we're looking at the date range of the layoffs here
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- to see what industry got hit the most do this
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- this query shows which country got hit the most
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


SELECT *
FROM layoffs_staging2;


-- This query shows us the total laid off for each year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- this query shows total laid of by stage of company
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- To look at progression of layoffs (rolling sum), start at earliest of layoffs and go to the end we do the below
-- start at position 6 and take 2 to pull out the month (it starts at 6 and occupies 2 spots)
-- this query shows us total for each month across all years
SELECT SUBSTRING(`date`, 6, 2) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `MONTH`;

-- this shows sum of layoffs from the beginning till the end for each month
-- we're going to do a rolling sum of this
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- we don't need to do a partition because we already did group by
-- what we'll see here is the month, how many were laid off, and a month by month progression of layoffs all the way down
-- rolling totals are great for visualization
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,
SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- here we're looking at the company by the year and how many people they laid off
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

-- to rank the years companies laid off ppl we do this:
-- we create our first CTE Company Year and change the columns
-- we created a rank and we wanted to filter on that rank, so we hit off our Company Year CTE to make our second CTE
-- lastly, we queried off of the final CTE
-- this query shows the top 5 companies with the most amount of layoffs per year
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;