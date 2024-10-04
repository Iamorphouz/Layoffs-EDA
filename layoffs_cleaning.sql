-- Data Cleaning

SELECT * 
FROM layoffs;

-- 1. Remove Duplicate
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove Any Columns

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT * 
FROM layoffs;

-- DROP TABLE layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE layoffs_staging2
LIKE layoffs;

ALTER TABLE layoffs_staging2
ADD COLUMN row_num INT;

SELECT * 
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2
;

-- Standardizing data

SELECT company ,TRIM(company)
FROM layoffs_staging2
-- WHERE company <> TRIM(company)
;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT company
FROM layoffs_staging2
;

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT COUNT(*)
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

-- United States United States.

SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

SELECT * 
FROM layoffs_staging2
;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE
;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;


SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';


SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb'
;

UPDATE layoffs_staging2
SET industry = 'Travel'
WHERE company = 'Airbnb' 
AND industry = ''
;

SELECT * 
FROM layoffs_staging2
WHERE company = "Carvana"
-- WHERE company = "Juul"
;

SELECT t1.company, t1.location, t1.industry,
t2.company, t2.location, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND (t2.industry IS NOT NULL AND t2.industry <> '')
;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND (t2.industry IS NOT NULL AND t2.industry <> '')
;


SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

DELETE 
FROM layoffs_staging2
WHERE company = "Bally's Interactive"
;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num
;