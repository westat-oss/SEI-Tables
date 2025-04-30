library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)
library(openxlsx)
library(tidyr)
library(purrr)

# ______________________________________________________________________________
# ---- Function: generate_table_INV4_supp ----
#
# This function is similar to generate_table_INV4, but focuses on top 10 businesses
# and top 10 universities globally. It uses the same method of counting distinct
# branches (via earliest commit year), but filters for sector = "business" or "academic".
#
# The final table includes:
#   - A header row: "Top 10 Businesses (Global)"
#   - The top 10 businesses
#   - A header row: "Top 10 Universities (Global)"
#   - The top 10 universities
#
# Each row has two columns: "Institution" and "Number of repositories".
# The table is written to a specified Excel workbook and sheet.
# ______________________________________________________________________________

generate_table_INV4_supp <- function(
    con,
    users_file,    # e.g., "user_data_country_sectors_cleaned.parquet"
    commits_file,  # e.g., "unique_commits_2009_2023.parquet"
    output_file,   # full path to Excel workbook to write results to
    sheet_name,    # sheet name in the Excel workbook where results will be written
    start_row      # starting row number in the sheet for writing the data
) {
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
    # Filter for sector = "business" or "academic" only
    filter(sector %in% c("business", "academic"))
  
  ## 2) Read the aggregated commits file ----
  commits <- tbl(con, sql(paste0("SELECT * FROM read_parquet('", commits_file, "')")))
  
  ## 3) Join commits with users, select branch, min_commit_year, and org ----
  joined_data <- commits %>%
    inner_join(users_expanded, c("author_id" = "id")) %>%
    select(branch, min_commit_year, org, sector) %>%
    distinct()
  
  ## 4) Compute branch-org data: for each branch-org combination, use earliest commit year ----
  branch_org_data <- joined_data %>%
    group_by(branch, org, sector) %>%
    summarise(first_commit_year = min(min_commit_year), .groups = "drop")
  
  ## 5) Count distinct branches per organization (overall, ignoring year) ----
  org_counts <- branch_org_data %>%
    group_by(org, sector) %>%
    summarise(new_branches = n_distinct(branch), .groups = "drop") %>%
    collect()  # bring into R for further manipulation
  
  # Separate into business and academic
  business_df <- org_counts %>%
    filter(sector == "business", !org %in% c("", "Misc. Business")) %>%
    arrange(desc(new_branches)) %>%
    slice_head(n = 10)
  
  academic_df <- org_counts %>%
    filter(sector == "academic", !org %in% c("", "Misc. Academic")) %>%
    arrange(desc(new_branches)) %>%
    slice_head(n = 10)
  
  # Create "header" rows
  business_header <- tibble(
    org = "Top 10 Businesses (Global)",
    sector = NA_character_,
    new_branches = NA_real_
  )
  academic_header <- tibble(
    org = "Top 10 Universities (Global)",
    sector = NA_character_,
    new_branches = NA_real_
  )
  
  # Combine in the desired order
  final_df <- bind_rows(
    business_header,
    business_df,
    academic_header,
    academic_df
  ) %>%
    select(org, new_branches)  # drop sector
  
  # Rename columns
  final_df <- final_df %>%
    rename(Institution = org, `Number of repositories` = new_branches)
  
  ## 6) Write the final table to the Excel workbook ----
  if (!is.null(output_file)) {
    if (file.exists(output_file)) {
      wb <- loadWorkbook(output_file)
    } else {
      wb <- createWorkbook()
    }
    if (!(sheet_name %in% names(wb))) {
      addWorksheet(wb, sheet_name)
    }
    writeData(wb, sheet = sheet_name, x = final_df, startRow = start_row, colNames = TRUE)
    saveWorkbook(wb, output_file, overwrite = TRUE)
  }
  
  return(final_df)
}

#_______________________________________________________________________________

# ---- Production: Run for Table INV4 Supplemental ----
#_______________________________________________________________________________

# Define Connection
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)

# Define file paths 
users_file_path   <- "user_data_country_sectors_cleaned.parquet"
commits_file_path <- "unique_commits_2009_2023.parquet"

# Define output Excel file and sheet name 
output_excel <- "\\\\westat.com\\DFS\\DVSTAT\\Individual Directories\\Askew\\sectoring\\Code\\Production\\Output_codegov\\SEI_2026_Shells_Output_Preliminary_codegov.xlsx"
sheet <- "Supp Data for Table INV-4"
start_row <- 4

table_INV4_supp <- generate_table_INV4_supp(
  con = con,
  users_file = users_file_path,
  commits_file = commits_file_path,
  output_file = output_excel,
  sheet_name = sheet,
  start_row = start_row
 )

print(table_INV4_supp)

dbDisconnect(con)
