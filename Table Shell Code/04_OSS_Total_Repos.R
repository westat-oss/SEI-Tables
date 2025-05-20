library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)
library(openxlsx)
library(tidyr)
library(purrr)

# ______________________________________________________________________________
# ---- Function: generate_total_repos_db ----
#
# This function uses DuckDB to join users and commits, then computes a table
# where each branch’s credit is fractionally aggregated to each country represented.
# after computing fractiona credit at the user level.
# The final summary (grouped by country) is computed in DuckDB and then collected,
# so that the sum across all countries equals the distinct number of branches.
# The output is written to a specified sheet (e.g., "Total Repos") within an Excel workbook.
# ______________________________________________________________________________

generate_total_repos <- function(
    con,
    users_file,    # Full path to the users file (e.g., "user_data_country_sectors_cleaned.parquet")
    commits_file,  # Full path to the commits file (e.g., "unique_commits_YYYY.parquet")
    year,          # Commit year to filter on
    output_file,   # Excel workbook file to write results to
    sheet_name,    # Sheet name in the Excel workbook where results will be written
    start_row      # Starting row number in the sheet to write the data
) {
  ## 1) Read and process the users file ----
  users <- tbl(con, sql(paste0(
    "SELECT * FROM read_parquet('", users_file, "')"
  )))
  # Expand the comma-separated country list into individual rows.
  users_expanded <- users %>%
    mutate(countries = string_split(country_cleaned, ',')) %>%
    select(id, countries) %>%                
    mutate(country = sql("unnest(countries)")) %>%
    mutate(country = trimws(country)) %>%  # remove extra whitespace
    mutate(country = ifelse(is.na(country) | country == "", "Missing Country", country))
  
  ## 2) Read the commits file ----
  commits <- tbl(con, sql(paste0(
    "SELECT * FROM read_parquet('", commits_file, "')"
  )))
  
  ## 3) Join commits with users, filter by year, and select branch & country ----
  # - This type of joining method is done to account for fractional credit to missing author_ids 
  # in the commits file. We still end up with same total repositories as an inner join, but a larger
  # weight of the credit is shifted towards "Missing Country"
    
  # Step 1: Left join commits to users
  expanded_commits <- commits %>%
    filter(commit_year == year) %>%
    left_join(users_expanded, by = c("author_id" = "id"))
  
  # Step 2: Find branches with at least one matched user
  branches_with_valid_id <- expanded_commits %>%
    filter(sql("country IS NOT NULL")) %>%   # ← country comes from users
    distinct(branch)
  
  # Step 3: Filter to only those branches
  joined_data <- expanded_commits %>%
    semi_join(branches_with_valid_id, by = "branch") %>%
    mutate(country = ifelse(is.na(country) | country == "", "Missing Country", country)) %>%
    select(branch, author_id, country)
  
  ## 4) Compute per-user credit per branch ----
  user_credit <- joined_data %>%
    group_by(branch) %>%
    mutate(n_users = n_distinct(author_id),
           user_credit = 1.0 / n_users) %>%
    ungroup()
  
  ## 5) Split user credit across countries ----
  country_frac <- user_credit %>%
    group_by(branch, author_id) %>%
    mutate(n_countries = n_distinct(country),
           fraction = user_credit / n_countries) %>%
    ungroup()
  
  ## 6) Aggregate to total fractional credit per country ----
  total_repos_summary <- country_frac %>%
    distinct(branch, author_id, country, fraction) %>%
    group_by(country) %>%
    summarise(Total_Repos = sum(fraction), .groups = "drop") %>%
    arrange(desc(Total_Repos))
  
  ## 7) Pull the final summary into R memory ----
  total_repos_summary_collected <- total_repos_summary %>% collect() %>%
    rename("Country" = country,
           "Total repositories" = Total_Repos)
  
  ## 8) Write the result to the Excel workbook ----
  if (!is.null(output_file)) {
    if (file.exists(output_file)) {
      wb <- loadWorkbook(output_file)
    } else {
      wb <- createWorkbook()
    }
    if (!(sheet_name %in% names(wb))) {
      addWorksheet(wb, sheet_name)
    }
    writeData(wb, sheet = sheet_name, x = total_repos_summary_collected, startRow = start_row, colNames = TRUE)
    saveWorkbook(wb, output_file, overwrite = TRUE)
  }
  
  return(total_repos_summary_collected)
}

# ______________________________________________________________________________

# ---- Production: Run on the desired year (e.g., 2023) ----
# ______________________________________________________________________________

# define connection
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)

# Define file paths
users_file_path   <- "user_data_country_sectors_cleaned.parquet"
commits_file_path <- "unique_commits_2023.parquet"
production_year   <- 2023

# Define output Excel file and sheet name for Total Repos
output_excel <- "\\\\westat.com\\DFS\\DVSTAT\\Individual Directories\\Askew\\sectoring\\Code\\Production\\Deliverable\\SEI_2026_Shells_Output_v4.xlsx"
sheet <- "Total Repos"


# Run the function
total_repos_table <- generate_total_repos(
  con = con,
  users_file = users_file_path,
  commits_file = commits_file_path,
  year = production_year,
  output_file = output_excel,
  sheet_name = sheet,
  start_row = 4
)

print(total_repos_table)

# Disconnect when finished
dbDisconnect(con)

