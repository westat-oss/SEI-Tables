library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)
library(openxlsx)
library(tidyr)
library(purrr)

# ______________________________________________________________________________
# ---- Function: generate_table_INV4 ----
#
# This function uses DuckDB to join the aggregated commits file with the users file.
# The users file is un-nested for organization, country, and sector. We filter for U.S. government users.
# Then, for each distinct branch–institution (i.e. organization) combination, the earliest commit year is used.
#
# Three aggregations are performed:
#   1. Detailed Agency Aggregation: branch counts where the agency is in specified list.
#   2. Federal Total Aggregation: overall branch count for all distinct agencies.
#   3. All Other Federal Aggregation: branch where agency is not in specified list
#
# The overall counts are summed across all years (so no separate year columns), and the results are
# output with two columns: "Institution" and "Number of repositories".
#
# The final table is written to an Excel sheet.
# ______________________________________________________________________________

generate_table_INV4 <- function(
    con,
    users_file,    # e.g., "user_data_country_sectors_cleaned.parquet"
    commits_file,  # e.g., "unique_commits_2009_2023.parquet"
    output_file,   # full path to Excel workbook to write results to
    sheet_name,    # sheet name in the Excel workbook where results will be written
    start_row      # starting row number in the sheet for writing the data
) {
  # Define the list of specific federal agencies (for detailed agency rows)
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
  # IMPORTANT: This assumes that organization_cleaned, country_cleaned, and sector have aligned lists.
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
    # Filter for U.S. government users (we do not restrict by org here so that Federal Total is complete)
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
    distinct(branch, org, min_commit_year) %>% 
    group_by(branch, org) %>%
    summarise(first_commit_year = min(min_commit_year), .groups = "drop")
  
  ## 5) Agency Aggregation: overall distinct branch count per agency (only for agencies of interest) ----
  agency_counts_overall <- branch_org_data %>%
    filter(org %in% agency_list) %>%
    group_by(org) %>%
    summarise(new_branches = n_distinct(branch), .groups = "drop")
  
  ## 6) Federal Total Aggregation: overall distinct branch count (ignoring organization) ----
  federal_counts_overall <- branch_org_data %>%
    summarise(new_branches = n(), .groups = "drop") %>%
    mutate(org = "Federal Total")

 ## 7) All Other Federal Aggregation: overall distinct branch count from all distinct orgs not in specified agency list ----
  other_federal_counts <- branch_org_data %>%
    filter(!(org %in% agency_list)) %>%
    summarise(new_branches = n(), .groups = "drop") %>%
    mutate(org = "All Other Federal")
    
  ## 8) Combine the agency and federal totals into one table ----
  
  # Collect the aggregated results into data frames before binding
  agency_df <- agency_counts_overall %>% collect()
  federal_df <- federal_counts_overall %>% collect()
  other_federal_counts_df <- other_federal_counts %>% collect()
    
  final_table <- bind_rows(agency_df, federal_df, other_federal_counts_df)
  
  ## 9) Rename columns and order rows ----
  final_table <- final_table %>%
    rename(Institution = org, `Number of repositories` = new_branches)
  
  # Define desired row order: "Federal Total" first, then the agencies in the specified order.
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
    "Department of State",
    "All Other Federal"
  )
    
  final_table <- final_table %>%
    mutate(Institution = factor(Institution, levels = desired_order)) %>%
    arrange(Institution)
  
  ## 10) Write the final table to the Excel workbook ----
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

# ---- Production: Run for the INV-4 table ----
#_______________________________________________________________________________

# Define Connection
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)

# Define file paths 
users_file_path   <- "user_data_country_sectors_cleaned_codegov_merged.parquet"
commits_file_path <- "unique_commits_2009_2023_codegov_merged.parquet"

# Define output Excel file and sheet name 
output_excel <- "\\\\westat.com\\DFS\\DVSTAT\\Individual Directories\\Askew\\sectoring\\Code\\Production\\Deliverable\\SEI_2026_Shells_Output_v4.xlsx"
sheet <- "Table INV-4"
start_row <- 4

# Run the function
table_INV4 <- generate_table_INV4(
  con = con,
  users_file = users_file_path,
  commits_file = commits_file_path,
  output_file = output_excel,
  sheet_name = sheet,
  start_row = start_row
)

print(table_INV4)

# Disconnect from DuckDB when finished.
dbDisconnect(con)
