library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)
library(openxlsx)
library(purrr)   # for map_chr()
library(tidyr) # for unnest

#_______________________________________________________________________________

# ---- Function: generate_co_dev_repos ----
#
# This function uses DuckDB to join users and commits, then computes a table summarizing
# the number of co-developed branches (using branch as the identifier) between each unique
# pair of countries for a given year. The final summary is written to an Excel sheet.
#_______________________________________________________________________________
generate_co_dev_repos <- function(
    con,
    users_file,     # Full path to the users file (e.g., "user_data_country_sectors_cleaned.parquet")
    commits_file,   # Full path to the commits file (e.g., "unique_commits_YYYY.parquet")
    year,           # The commit year to filter on
    output_file,    # Excel template file to write results to
    sheet_name,     # Sheet name in the Excel template where results will be written
    start_row       # Starting row number in the sheet to write the data
) {
  ## 1) Read and process the users file ----
  users <- tbl(con, sql(paste0(
    "SELECT *, 
     FROM read_parquet('", 
    users_file, 
    "')"
  )))
  users_expanded <- users %>%
    mutate(countries = string_split(country_cleaned, ',')) %>% # split unique values for users on ","
    select(id, countries) %>%                # "id" from users file
    mutate(country = sql("unnest(countries)")) %>% # create separate rows for each country value
    mutate(country = trimws(country)) %>%  # remove extra whitespace
    mutate(country = ifelse(is.na(country) | country == "", "Missing Country", country))
  
  ## 2) Read the commits file ----
  commits <- tbl(con, sql(paste0(
    "SELECT * FROM read_parquet('", 
    commits_file, 
    "')"
  )))
  
  ## 3) Join commits with users, filter by year, and select branch & country ----
  joined_data <- commits %>%
    filter(commit_year == year) %>%
    inner_join(users_expanded, by = c("author_id" = "id")) %>%
    select(branch, country)
  
  ## 4) Collect the joined data into R ----
  # distinct() ensures that duplicate branch-country combinations are removed.
  co_dev_data <- joined_data %>% distinct() %>% collect()
  
  ## 5) For each branch, generate all unique unordered country pairs ----
  country_pairs <- co_dev_data %>%
    group_by(branch) %>%
    summarise(country_list = list(unique(country))) %>%
    mutate(country_pair = map(country_list, function(x) {
      if(length(x) >= 2) combn(sort(x), 2, simplify = FALSE) else list()
    })) %>%
    unnest(country_pair) %>%
    mutate(
      Country1 = map_chr(country_pair, ~ .x[1]),
      Country2 = map_chr(country_pair, ~ .x[2])
    ) %>%
    select(branch, Country1, Country2) %>%
    ungroup()
  
  ## 6) Summarize co-developed branches per country pair ----
  co_dev_summary <- country_pairs %>%
    filter(Country1 != "Missing Country", Country2 != "Missing Country") %>%
    group_by(Country1, Country2) %>%
    summarise(Co_developed_branches = n_distinct(branch), .groups = "drop") %>%
    arrange(desc(Co_developed_branches)) %>%
    rename("Country 1" = Country1,
           "Country 2" = Country2,
           "Co-developed repositories" = Co_developed_branches)
  
  ## 7) Write the result to the excel workbook ----
  if (!is.null(output_file)) {
    if (file.exists(output_file)) {
      wb <- loadWorkbook(output_file)
    } else {
      wb <- createWorkbook()
    }
    # Add the worksheet with the specified sheet_name (if it doesn't already exist)
    if (!(sheet_name %in% names(wb))) {
      addWorksheet(wb, sheet_name)
    }
    writeData(wb, sheet = sheet_name, x = co_dev_summary, startRow = 4, colNames = TRUE)
    saveWorkbook(wb, output_file, overwrite = TRUE)
  }
  
  return(co_dev_summary)
}

#_______________________________________________________________________________

# ---- Production: Run on the year 2023, run on 2024 when available ----
#_______________________________________________________________________________

# define connection
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)

# Define file paths 
users_file_path <- "user_data_country_sectors_cleaned.parquet"
commits_file_path <- "unique_commits_2023.parquet"

# production year
production_year <- 2023

# Define output Excel file and sheet name
output_excel <- "\\\\westat.com\\DFS\\DVSTAT\\Individual Directories\\Askew\\sectoring\\Code\\Production\\Output_codegov\\SEI_2026_Shells_Output_Preliminary_codegov.xlsx"
sheet <- "Data for Figure YYY-Y"

# Run the test function for a single year (2024)
co_dev_repos_table <- generate_co_dev_repos(
  con = con,
  users_file = users_file_path,
  commits_file = commits_file_path,
  year = production_year,
  output_file = output_excel,
  sheet_name = sheet,
  start_row = 4
)

print(co_dev_repos_table)
