-- Data Cleaning

-- In this project, we study the economic slowdown experienced by tech firms around the globe.
-- We analyze the recent tech turmoil and discover useful insights using the following data set: https://www.kaggle.com/datasets/swaptr/layoffs-2022/data
-- This project displays the data cleaning aspect of the data.

SELECT *
FROM layoffs;

-- Guide for Data Cleaning:
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Eliminate Null values or blank values
-- 4. Remove Any Columns 

-- Removing column from raw data set is a big problem, so we create staging or raw data set

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging; -- this gives us all of the columns; so now we must insert the data

INSERT layoffs_staging -- this selects all data form layoffs and inserts to layoffs_staging
SELECT *
FROM layoffs;

-- You don't want to work on the raw data; not best practice

-- Now, let's try to identify dupes
-- When we have a column that gives unique row ID, it's easier to remove dupes
-- we don't have that here, so its more difficult; we'll do a row # that'll match against all these columns

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num -- date is a keyword in MySQL so use backticks
FROM layoffs_staging; -- the row_num colum should be unique; if it has 2 or above, that means there's dupes (there's an issue)

-- Create a CTE or subquery for the above; we're doing a CTE
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) AS row_num 
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1; -- everything showing when we run this provides duplicates, so we want to get rid of what appears here

-- to confirm these are dupes,

SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo'; -- this shows some that aren't dupes because we didn't partition by every column

-- In MSSQL you can identify the row numbers in the CTE and delete them; can't do them in MySQL
-- so something like the below
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) AS row_num 
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;
-- doing the above will prompt "The target table duplicate_cte of the DELETE is not updatable
-- so we're going to create another table that has an extra row, and deleting it where the row = 2

-- so we're creating our table below using copy to clipboard create statement; we're creating the copy of the table

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL,
  `row_num` INT -- this was the additional row created
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2 -- this alone gives us an empty table, so we want to insert info in partition into this
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised) AS row_num 
FROM layoffs_staging;

SET SQL_SAFE_UPDATES = 0; -- I did this just so I could update/delete; I could also uncheck the box in preferences

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2; -- the row_num col showing when this runs is redundant; we'll get rid of it at the end
-- we are good to go; that is how we remove duplicates
-- if we had a unique col on the left it would've been easier, but we didn't have that


-- STEP 2 Standardizing Data
-- it's finding issues in your data and then fixing it

-- if there were white spaces in the beginning of the company name, we would do the below to fix
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company); -- TRIM just removes whitespace off the ends (left or right hand side)


-- in a previous version of the data set, there was Crypto, Cryptocurrency, and Crypto Currency as different industries
-- to fix this issue we would do the below
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto' -- this assigns the name 'Crypto' to industries that begin with 'Crypto%'
WHERE industry LIKE 'Crypto%';

-- if there was a dot after a dupe united states, we could do the below to address and fix
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) -- Trim alone won't fix, you must do trailing; we're specifying looking for a period here not just whitespace
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- to do time series (exploratory data analysis), we must change date because right now it's text; we want to change it to a date column
-- prev data set shows date column info like this: 12/16/2022;
-- to change that, do the below; it would change from 12/16/2022 format to 2022-12-16 format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') -- capital Y stands for 4 number long year
FROM layoffs_staging2;

-- if we needed to actually update the format we would do the below
-- UPDATE layoffs_staging2
-- SET `date` = STR_TO_DATE(`date`, '%m/$d/%Y')

-- the above only changes the date format; to actually convert to date column do the below
-- only do this on a staging table not a raw table
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- STEP 3 Working with NULL and blank values
-- you're going to have NULLS and blank values, so you must figure out how to handle them

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- actual issue
SELECT *
FROM layoffs_staging2
WHERE company = 'Appsmith';

-- if there were several blanks like 'AirBnb', do the below
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'; -- if this shows one dupe with blank industry and one dupe with industry filled, we can populate data for airbnb (we want them to be the same
-- the row without industry would not be helpful, so we must update

-- do the below to join the tables with info from one to the other dupe
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; 

UPDATE layoffs_staging t1
JOIN layoffs_staging2 t2
	ON t1.copany = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL ;

SELECT *
FROM layoffs_staging2;

-- STEP 4 Remove any columns and rows that must be removed

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- we'll delete info in above because it's not useful right now (data can't be trusted)

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- delete the row_num col because we no longer need it
ALTER TABLE layoffs_staging2
DROP COLUMN row_num