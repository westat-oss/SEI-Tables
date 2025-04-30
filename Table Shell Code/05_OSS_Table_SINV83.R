library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)
library(openxlsx)
library(tidyr)
library(purrr)

# ______________________________________________________________________________
# ---- Function: generate_table_SINV83 ----
#
# This function uses DuckDB to join users and aggregated commits data.
# It unpacks the comma‐separated fields (organization, country, sector) from the users file
# and filters for U.S. government users.
#
# It then groups by branch and organization to determine each branch’s earliest commit year.
# Two aggregations are performed:
#   1. Detailed Agency Counts: only for rows where the organization is in a specified list.
#   2. Federal Total: overall distinct branch counts (regardless of organization).
#
# Finally, the function pivots the results so that there is one row per agency (and one row for "Federal Total")
# with a column for each year (2009-2023) indicating the number of new branches.
#
# The final table is written to an Excel sheet.
# ______________________________________________________________________________

generate_table_SINV83 <- function(
    con,
    users_file,    # e.g., "user_data_country_sectors_cleaned.parquet"
    commits_file,  # e.g., "unique_commits_2009_2023.parquet"
    output_file,   # full path to Excel workbook to write results to
    sheet_name,    # sheet name in the Excel workbook where results will be written
    start_row      # starting row number in the sheet for writing the data
) {
  # Define the list of specific federal agencies
  agency_list <- c(
    "Agency for International Development",
    "Department of Agriculture",
    "Department of Commerce",
    "Department of the Interior",
    "Department of Defense",
    "Department of Education",
    "Department of Energy",
    "Department of Health and Human Services",
    "Department of Homeland Security",
    "Department of Housing and Urban Development",
    "Department of Justice",
    "Department of Labor",
    "Department of the Treasury",
    "Department of Transportation",
    "Department of Veterans Affairs",
    "Environmental Protection Agency",
    "Federal Election Commission",
    "General Services Administration",
    "National Aeronautics and Space Administration",
    "National Science Foundation",
    "Office of Personnel Management",
    "Small Business Administration",
    "Social Security Administration",
    "Department of State"
  )
  
  ## 1) Read and process the users file ----
  users <- tbl(con, sql(paste0("SELECT * FROM read_parquet('", users_file, "')")))
  
  # Unnest the comma-separated fields for organization, country, and sector.
  users_expanded <- users %>%
    mutate(
      org_list = string_split(organization_cleaned, ","),
      country_list = string_split(country_cleaned, ","),
      sector_list = string_split(sector, ",")
    ) %>%
    mutate(
      org = sql("unnest(org_list)"),
      country = sql("unnest(country_list)"),
      sector = sql("unnest(sector_list)")
    ) %>%
    mutate(
      org = trimws(org),
      country = trimws(country),
      sector = trimws(sector)
    ) %>%
    # Filter for U.S. government users 
    filter(country == "United States", sector == "government")
  
  ## 2) Read the aggregated commits file ----
  # This file has columns: author_id, branch, and min_commit_year.
  commits <- tbl(con, sql(paste0("SELECT * FROM read_parquet('", commits_file, "')")))
  
  ## 3) Join commits with users and select branch, min_commit_year, and org ----
  joined_data <- commits %>%
    inner_join(users_expanded, by = c("author_id" = "id")) %>%
    select(branch, min_commit_year, org) %>%
    distinct()
  
  ## 4) Compute branch-org data: for each branch-org combination, use the earliest commit year ----
  branch_org_data <- joined_data %>%
    group_by(branch, org) %>%
    summarise(first_commit_year = min(min_commit_year), .groups = "drop")
  
  ## 5) Agency Aggregation: count new branches per agency (only for agencies of interest) ----
  agency_counts <- branch_org_data %>%
    filter(org %in% agency_list) %>%
    group_by(org, first_commit_year) %>%
    summarise(new_branches = n_distinct(branch), .groups = "drop")
  
  ## 6) Federal Total Aggregation: overall distinct branch count (ignoring org) ----
  federal_counts <- joined_data %>%
    group_by(branch) %>%
    summarise(first_commit_year = min(min_commit_year), .groups = "drop") %>%
    group_by(first_commit_year) %>%
    summarise(new_branches = n_distinct(branch), .groups = "drop") %>%
    mutate(org = "Federal Total")
  
  ## 7) Pivot the aggregations to wide format (years as columns) ----
  # Collect the results into data frames before pivoting to avoid lazy evaluation issues.
  agency_counts_df <- agency_counts %>% collect()
  federal_counts_df <- federal_counts %>% collect()
  
  
  pivot_agencies <- agency_counts_df %>%
    pivot_wider(
      names_from = first_commit_year,
      values_from = new_branches,
      values_fill = list(new_branches = 0)
    )
  
  pivot_federal <- federal_counts_df %>%
    pivot_wider(
      names_from = first_commit_year,
      values_from = new_branches,
      values_fill = list(new_branches = 0)
    )
  
  ## 8) Combine the agency rows with the Federal Total row ----
  final_table <- bind_rows(pivot_agencies, pivot_federal)
  
  # -- Reorder the columns so that the year columns are in ascending order --
  
  # Determine the year columns
  year_columns <- setdiff(names(final_table), "org")
  
  # Sort the year column names numerically.
  year_columns_sorted <- as.character(sort(as.numeric(year_columns)))
  
  final_table <- final_table %>% 
    select(org, all_of(year_columns_sorted))
  
  # Rename "org" to "Institution"
  final_table <- final_table %>% rename(Institution = org)
  
  # -- Reorder the rows --
  
  # "Federal Total" first, then the agencies in the specified order.
  desired_order <- c(
    "Federal Total",
    "Agency for International Development",
    "Department of Agriculture",
    "Department of Commerce",
    "Department of the Interior",
    "Department of Defense",
    "Department of Education",
    "Department of Energy",
    "Department of Health and Human Services",
    "Department of Homeland Security",
    "Department of Housing and Urban Development",
    "Department of Justice",
    "Department of Labor",
    "Department of the Treasury",
    "Department of Transportation",
    "Department of Veterans Affairs",
    "Environmental Protection Agency",
    "Federal Election Commission",
    "General Services Administration",
    "National Aeronautics and Space Administration",
    "National Science Foundation",
    "Office of Personnel Management",
    "Small Business Administration",
    "Social Security Administration",
    "Department of State"
  )
  
  final_table <- final_table %>%
    mutate(Institution = factor(Institution, levels = desired_order)) %>%
    arrange(Institution) 
  
  ## 9) Write the final table to the Excel workbook ----
  if (!is.null(output_file)) {
    if (file.exists(output_file)) {
      wb <- loadWorkbook(output_file)
    } else {
      wb <- createWorkbook()
    }
    if (!(sheet_name %in% names(wb))) {
      addWorksheet(wb, sheet_name)
    }
    writeData(wb, sheet = sheet_name, x = final_table, startRow = start_row, colNames = TRUE)
    saveWorkbook(wb, output_file, overwrite = TRUE)
  }
  
  return(final_table)
}

#_______________________________________________________________________________

# ---- Production: Run for the SINV83 table ----
#_______________________________________________________________________________

# Define connection
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)

# Define file paths 
users_file_path   <- "user_data_country_sectors_cleaned_codegov_merged.parquet"
commits_file_path <- "unique_commits_2009_2023_codegov_merged.parquet"

# Define output Excel file and sheet name
output_excel <- "\\\\westat.com\\DFS\\DVSTAT\\Individual Directories\\Askew\\sectoring\\Code\\Production\\Output_codegov\\SEI_2026_Shells_Output_Preliminary_codegov.xlsx"
sheet <- "Table SINV-83"

# Run the function
table_SINV83 <- generate_table_SINV83(
  con = con,
  users_file = users_file_path,
  commits_file = commits_file_path,
  output_file = output_excel,
  sheet_name = sheet,
  start_row = 4
)

print(table_SINV83)

# Disconnect from DuckDB when finished.
dbDisconnect(con)

