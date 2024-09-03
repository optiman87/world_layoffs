USE world_layoffs;

# Count the number of records in the table
SELECT 
    COUNT(*)
FROM
    worldLayoffs;

# Add id column to table
ALTER TABLE worldLayoffs
ADD COLUMN (id INT PRIMARY KEY AUTO_INCREMENT);

# Create a staging table to store a copy of the data
CREATE TABLE layoffs_staging LIKE worldLayoffs;

# Insert data into the staging table
INSERT INTO layoffs_staging
SELECT * FROM worldLayoffs;

# Look for duplicate records in the staging table
WITH duplicate_cte AS 
( SELECT id, company, ROW_NUMBER() OVER w AS row_num FROM layoffs_staging
WINDOW w AS (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised))
SELECT * FROM duplicate_cte
WHERE row_num > 1;

COMMIT;

# Delete duplicate records in the staging table
WITH duplicate_cte AS 
( SELECT id, ROW_NUMBER() OVER w AS row_num FROM layoffs_staging
WINDOW w AS (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised))
DELETE FROM layoffs_staging
WHERE id IN (SELECT id FROM duplicate_cte WHERE row_num > 1);

# Trim data in the company column
SELECT 
    company, TRIM(company)
FROM
    layoffs_staging;

UPDATE layoffs_staging
SET 
    company = TRIM(company);

# Inspect industry values
SELECT DISTINCT
    industry
FROM
    layoffs_staging
ORDER BY 1;

# Inspect country values
SELECT DISTINCT
    country
FROM
    layoffs_staging
ORDER BY 1;

# Set date values to date format
SELECT 
    `date`
FROM
    layoffs_staging;
    
UPDATE layoffs_staging
SET 
    date = STR_TO_DATE(`date`, '%Y-%m-%d');

# Change date column to date format
ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;

# Rename funds_raised column
ALTER TABLE layoffs_staging
RENAME COLUMN funds_raised to funds_raised_millions;

# Change funds_raised_millions values to integer
SELECT 
    funds_raised_millions
FROM
    layoffs_staging;
    
UPDATE layoffs_staging
SET 
    funds_raised_millions = ROUND(funds_raised_millions,0);

ALTER TABLE layoffs_staging
MODIFY COLUMN funds_raised_millions INT;
    
# Select companies whose industries are null  
SELECT * FROM layoffs_staging WHERE industry IS NULL;

# Look for companies who have both null and non null industry value
SELECT 
    t1.industry, t2.industry
FROM
    layoffs_staging t1
        JOIN
    layoffs_staging t2 ON t1.company = t2.company
        AND t1.industry IS NULL
        AND t2.industry IS NOT NULL;
        
# Populate the industry value with the non null value 
UPDATE layoffs_staging t1
        JOIN
    layoffs_staging t2 ON t1.company = t2.company 
SET 
    t1.industry = t2.industry
WHERE
    t1.industry IS NULL
        AND t2.industry IS NOT NULL;

# Delete records with null total_laid_off and percentage_laid_off values
SELECT 
    *
FROM
    layoffs_staging
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;
        
COMMIT;

DELETE FROM layoffs_staging
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

# Calculate the total laid off employees grouped by company
SELECT 
    company, SUM(total_laid_off) AS total
FROM
    layoffs_staging
GROUP BY company
ORDER BY 2 DESC;

# Calculate the total laid off employees grouped by month (Year-month)
SELECT 
    SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off)
FROM
    layoffs_staging
WHERE
    `date` IS NOT NULL
GROUP BY `month`
ORDER BY `month`;

# Calculate the rolling total of laid off employees by month
with rolling_total_cte as 
(select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_layoffs
from layoffs_staging
where `date` is not null
group by `month`
order by `month`)
select `month`, total_layoffs, sum(total_layoffs) over (order by `month`) as rolling_total
from rolling_total_cte;

# Calculate the total laid off employees grouped by company each year
SELECT 
    company, YEAR(`date`) AS `year`, SUM(total_laid_off) as total_layoffs
FROM
    layoffs_staging
GROUP BY company , `year`
ORDER BY `year`, total_layoffs DESC;

# Rank the top 5 companies with the most laid off employees in each year
WITH layoff_by_year_cte AS (
SELECT company, YEAR(`date`) AS `year`, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
GROUP BY company, `year`
ORDER BY `year`),
ranking_cte AS (
SELECT company, `year`, total_layoffs, DENSE_RANK() OVER (PARTITION BY  `year` ORDER BY  total_layoffs DESC) AS ranking
FROM layoff_by_year_cte 
WHERE `year` IS NOT NULL)
SELECT * FROM ranking_cte 
WHERE ranking <= 5;

# Drop the id column as it is no longer required
ALTER TABLE layoffs_staging
DROP COLUMN  id;

# Add current timestamp to the processed_timestamp column for audit purposes
ALTER TABLE layoffs_staging
ADD COLUMN (processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

