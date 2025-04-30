# ______________________________________________________________________________

# ---- Load Packages ----
# ______________________________________________________________________________

library(DBI)
library(arrow)
library(duckdb) 
library(tidyverse)
library(lubridate)
library(dbplyr)

#_______________________________________________________________________________

# ---- Clean code.gov data ----
#_______________________________________________________________________________

## View a sample to look at column names ----
codegov_users <- open_dataset("user_data_country_sectors_2025_04_09_codegov.parquet") %>%
  collect()

codegov_repos <- open_dataset("repo_data_codegov_4_9_25.parquet") %>%
  collect()

## Clean code.gov user data ----

### Create new_agencies column ----

govern_agencies_codegov <- codegov_users %>%
  filter((!is.na(organization_company_government)) & (country_government == 'United States') | ((country_government == 'United States of America'))) %>% #rerun  w/ United States of America
  mutate(new_agencies = case_when(
    grepl('USDA|Agriculture', organization_company_government, ignore.case = TRUE) ~ 'Department of Agriculture',
    grepl('US National Institute of Standards and Technology|US NOAA Fisheries|US National Weather Service|Department of Commerce|US Bureau of Economic Analysis|US International Trade Administration|US National Ocean Service|US Patent and Trademark Office', organization_company_government, ignore.case = TRUE) ~ 'Department of Commerce',
    grepl('USGS|Interior|National Park Service|Bureau of Land Management|Bureau of Reclamation', organization_company_government, ignore.case = TRUE) ~ 'Department of the Interior',
    grepl('Department of Defense', organization_company_government, ignore.case = TRUE) ~ 'Department of Defense',
    grepl('Lab(oratory|oratories)|Energy|Oak Ridge|Princeton Plasma', organization_company_government, ignore.case = TRUE) ~ 'Department of Energy',
    grepl('National Institutes of Health|Centers for Disease Control and Prevention|National Cancer Institute|National Library of Medicine|Health and Human Services|National Institute on Aging|National Institute on Drug Abuse', organization_company_government) ~ 'Department of Health and Human Services',
    grepl('Department of Homeland Security|USCIS|FEMA', organization_company_government) ~ 'Department of Homeland Security', 
    grepl('Investigation|Justice|Unicor', organization_company_government, ignore.case = TRUE) ~ 'Department of Justice',
    grepl('\bLabor\b', organization_company_government, ignore.case = TRUE) ~ 'Department of Labor',
    grepl('Transportation|Enterprise|Aviation|NHTSA', organization_company_government) ~ 'Department of Transportation',
    grepl('Department of Veterans Affairs', organization_company_government , ignore.case = TRUE)~ 'Department of Veterans Affairs',
    grepl('Environmental Protection Agency', organization_company_government, ignore.case = TRUE)~ 'Environmental Protection Agency',
    grepl('Federal Election Commission', organization_company_government, ignore.case = TRUE)~ 'Federal Election Commission',
    grepl('General Services Administration|GSA|FAI', organization_company_government)~ 'General Services Administration',
    grepl('National Aeronautics and Space Administration', organization_company_government)~ 'National Aeronautics and Space Administration',
    grepl('US National Science Foundation', organization_company_government)~ 'National Science Foundation',
    grepl('OPM', organization_company_government)~ 'Office of Personnel Management',
    grepl('Social Security Administration', organization_company_government)~ 'Social Security Administration',
    grepl('Dept of State|US Department of State', organization_company_government)~ 'Department of State',
    TRUE ~ NA_character_
  )) %>%
  mutate(org_in_agency = !is.na(new_agencies)) %>%
  select(login, new_agencies, org_in_agency)

# join back to codegov users file

codegov_users_gov <- codegov_users %>%
  filter(government == 1) %>%
  left_join(govern_agencies_codegov, by = "login")


# write results to parquet file
write_parquet(codegov_users_gov,"user_data_country_sectors_governmentClassification_codegov_2025_04_10.parquet")


### Cleaning users query ----

# Connect to DuckDB 
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

query <- "
COPY (
  SELECT 
    id,
    login,
    -- cleaning country
    array_to_string(
      array_distinct(
        split(
          CONCAT_WS(', ',
            REPLACE(country_location, '|', ', '),  
            REPLACE(country_email, '|', ', '),
            REPLACE(country_government, '|', ', '),
            REPLACE(country_academic, '|', ', ')
          )
        , ', ')
      )
    , ', ') AS country_cleaned,
    
    -- cleaning organization, with Inc. removed
    REPLACE(
      array_to_string(
        array_distinct(
          split(
            CONCAT_WS(', ',
              REPLACE(organization_company_business, '|', ', '),
              REPLACE(organization_email_business, '|', ', '),
              REPLACE(organization_company_government, '|', ', '),
              REPLACE(organization_email_government, '|', ', '),
              REPLACE(organization_company_academic, '|', ', '),
              REPLACE(organization_email_academic, '|', ', '),
              REPLACE(organization_company_nonprofit, '|', ', '),
              REPLACE(organization_email_nonprofit, '|', ', '),
              REPLACE(new_agencies, '|', ', ')
            )
          , ', ')
        )
      , ', '),
      'Inc.', ''
    ) AS organization_cleaned,
    
    -- creating sector column
    array_to_string(
      array_distinct(
        split(
          CONCAT_WS(', ',
            CASE WHEN nonprofit = 1 THEN 'nonprofit' END,
            CASE WHEN government = 1 THEN 'government' END,
            CASE WHEN business = 1 THEN 'business' END,
            CASE WHEN academic = 1 THEN 'academic' END
          )
        , ', ')
      )
    , ', ') AS sector
  FROM read_parquet('user_data_country_sectors_governmentClassification_codegov_2025_04_10.parquet')
) TO 'user_data_country_sectors_cleaned_codegov.parquet'
(FORMAT PARQUET);
"
dbExecute(con, query)

### View cleaned code.gov users data ----

codegov_users_cleaned <- open_dataset("user_data_country_sectors_cleaned_codegov.parquet") %>%
  collect()


## Clean code.gov commit data ----

# Connect to DuckDB in-memory
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

### Cleaning query for combined commits file ----
query <- "
COPY (
  SELECT 
    userid AS author_id,
    repoid AS branch,
    MIN(EXTRACT(YEAR FROM commityear)) AS min_commit_year
  FROM read_parquet('repo_data_codegov_4_9_25.parquet')
  GROUP BY userid, repoid
  HAVING MIN(EXTRACT(YEAR FROM commityear)) >= 2009
)
TO 'unique_commits_codegov.parquet'
(FORMAT PARQUET);
"

dbExecute(con, query)
dbDisconnect(con, shutdown = TRUE)


#_______________________________________________________________________________

# ---- Combine code.gov data with GitHub data ----
#_______________________________________________________________________________


## Users file combined ----

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

query <- "
COPY (
  SELECT *
  FROM read_parquet('user_data_country_sectors_cleaned.parquet')
  UNION ALL
  SELECT *
  FROM read_parquet('user_data_country_sectors_cleaned_codegov.parquet')
  WHERE id NOT IN (
    SELECT id FROM read_parquet('user_data_country_sectors_cleaned.parquet')
  )
)
TO 'user_data_country_sectors_cleaned_codegov_merged.parquet'
(FORMAT PARQUET);
"

dbExecute(con, query)
dbDisconnect(con, shutdown = TRUE)

## Commits file combined ----

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

query <- "
COPY (
  SELECT *
  FROM read_parquet('unique_commits_2009_2023.parquet')
  UNION ALL
  SELECT *
  FROM read_parquet('unique_commits_codegov.parquet') AS c
  WHERE NOT EXISTS (
    SELECT 1
    FROM read_parquet('unique_commits_2009_2023.parquet') AS m
    WHERE m.author_id = c.author_id
      AND m.branch = c.branch
  )
)
TO 'unique_commits_2009_2023_codegov_merged.parquet'
(FORMAT PARQUET);
"

dbExecute(con, query)
dbDisconnect(con, shutdown = TRUE)