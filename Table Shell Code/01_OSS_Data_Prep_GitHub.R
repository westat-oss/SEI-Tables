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

# ---- Clean User Data ----
#_______________________________________________________________________________

## View a sample to look at column names ----
users <- open_dataset("user_data_country_sectors_governmentClassification_2025_03_24.parquet")
print(users$schema)

users_raw_subset <- users %>% 
  select(id, author_id, everything()) %>%
  head(10000) %>%
    collect()

## Cleaning query ----

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
              REPLACE(organization, '|', ', '), 
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
  FROM read_parquet('user_data_country_sectors_governmentClassification_2025_03_24.parquet')
) TO 'user_data_country_sectors_cleaned.parquet'
(FORMAT PARQUET);
"
dbExecute(con, query)

#_______________________________________________________________________________

# ---- EDA on cleaned user data ----
#_______________________________________________________________________________

## View cleaned dataset ----

users_clean <- open_dataset("user_data_country_sectors_cleaned.parquet")

users_clean_subset <- users_clean %>% 
  collect()


#_______________________________________________________________________________
  
# ---- Clean commits data ----
#_______________________________________________________________________________

## View a sample to look at column names ----
commits_2009_raw <- open_dataset("commits_raw_2009.0.parquet")

commits_2009_raw_subset <- commits_2009_raw %>% 
  head(100) %>% 
  collect()

## Create commit files for each year ----

# List files in the zip archive and filter for .parquet files
zipfile <- "C:/Users/askew_n/Downloads/commit_archive.zip"  # zip folder containing all parquet commit files
file_list <- unzip(zipfile, list = TRUE)
parquet_files <- file_list$Name[grepl("\\.parquet$", file_list$Name)]

# Define folders of interest (e.g., "2009.0", "2010.0", "2023.0")
folders_of_interest <- paste0(2009:2023, ".0")

# Filter to keep only files from these folders
filtered_files <- parquet_files[sapply(parquet_files, function(x) {
  folder <- sub("^([^/]+)/.*", "\\1", x)
  folder %in% folders_of_interest
})]

# Extract all filtered files to a temporary directory (preserving folder structure)
temp_dir <- file.path(tempdir(), "commit_archive_extracted")
dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
unzip(zipfile, files = filtered_files, exdir = temp_dir)

# Connect to DuckDB 
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

# For each year, run a query reading all the extracted parquet files
for (year in 2009:2023) {
  query <- sprintf("
    COPY (
      SELECT DISTINCT branch, unnest(authors_id) AS author_id, EXTRACT(YEAR FROM committedat) AS commit_year -- had to unnest authors_id b/c it is stored as a list item
      FROM read_parquet('%s/*/*.parquet')
      WHERE EXTRACT(YEAR FROM committedat) = %d
    )
    TO 'unique_commits_%d.parquet'
    (FORMAT PARQUET);
  ", temp_dir, year, year)
  
  dbExecute(con, query)
  message(sprintf("Exported unique commits for %d to unique_commits_%d.parquet", year, year))
}

# disconnect from DuckDB
dbDisconnect(con, shutdown = TRUE)


## ---- Create a single commit file with the minimum commit year by author id and branch combination ----

# Connect to DuckDB (if not already connected)
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

# Create the final aggregated file combining all yearly files
query <- "
COPY (
  SELECT author_id, branch, MIN(commit_year) AS min_commit_year
  FROM (
    SELECT * FROM read_parquet('unique_commits_2009.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2010.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2011.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2012.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2013.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2014.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2015.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2016.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2017.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2018.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2019.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2020.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2021.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2022.parquet')
    UNION ALL
    SELECT * FROM read_parquet('unique_commits_2023.parquet')
  ) t
  GROUP BY author_id, branch
)
TO 'unique_commits_2009_2023.parquet'
(FORMAT PARQUET);
"

dbExecute(con, query)
dbDisconnect(con, shutdown = TRUE)

#______________________________________________________________________________

# ----- EDA on cleaned commit data -----
#______________________________________________________________________________

## View cleaned datasets ----

# 2009 yearly
commits_clean_2009 <- open_dataset("unique_commits_2009.parquet")
print(commits_clean_2009$schema)

commits_clean_2009_subset <- commits_clean_2009 %>% 
  head(100000) %>% 
  collect()

# aggregated commits
commits_clean_full <- open_dataset("unique_commits_2009_2023.parquet")
print(commits_clean_full$schema)

commits_clean_full_subset <- commits_clean_full %>% 
  head(100000) %>% 
  collect()
