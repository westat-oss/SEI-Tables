library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)
library(openxlsx)
library(tidyr)
library(purrr)

# ______________________________________________________________________________
# ---- Function: generate_sectors_table ----
#
# This function uses DuckDB to join the aggregated commits file with the users file.
# The users file is un-nested for sector and country, with missing sectors labeled as
# "Unclassified" and missing countries as "Missing". Fractional repository credit is computed
# per branch (each branch’s total credit sums to 1). Two aggregations are performed:
#   1. Country-Based Aggregation: Groups branches by region ("Total (US)" or "Total (Global)")
#      based on the country, using the earliest commit year.
#   2. Sector-Based Aggregation: Groups branches by sector (e.g. Academic, Government, Business,
#      Nonprofit, Unclassified) using the earliest commit year.
#
# The results are pivoted by year (2009–2023) and combined, with the final output written
# to an Excel sheet and returned as a data frame.
# ______________________________________________________________________________

generate_sectors_table <- function(
    con,
    users_file,    # e.g., "user_data_country_sectors_cleaned.parquet"
    commits_file,  # e.g., "unique_commits_2009_2023.parquet"
    output_file,   # full path to Excel workbook for output
    sheet_name,    # sheet name in the Excel workbook
    start_row      # starting row number for output
) {
  
  ## 1) Process Users File ----
  users <- tbl(con, sql(paste0("SELECT * FROM read_parquet('", users_file, "')")))
  
  users_expanded <- users %>%
    mutate(country_list = string_split(country_cleaned, ",")) %>%
    mutate(country_val = sql("unnest(country_list)")) %>%
    mutate(country_val = trimws(country_val)) %>%
    mutate(country_val = ifelse(is.na(country_val) | country_val == "",
                                "Missing", country_val)) %>%
    mutate(sector_list = string_split(sector, ",")) %>%
    mutate(sector_val = sql("unnest(sector_list)")) %>%
    mutate(sector_val = trimws(sector_val)) %>%
    mutate(sector_val = ifelse(is.na(sector_val) | sector_val == "",
                               "Unclassified", sector_val)) %>%
    select(id, country_val, sector_val)
  
  ## 2) Read Commits File ----
  commits <- tbl(con, sql(paste0("SELECT * FROM read_parquet('", commits_file, "')")))
  
  ## 3) Join Commits and Users ----
  joined_data <- commits %>%
    inner_join(users_expanded, by = c("author_id" = "id")) %>%
    select(branch, min_commit_year, author_id, sector_val, country_val)
  
  ## 4) Compute per-user credit ----
  user_frac <- joined_data %>%
    group_by(branch) %>%
    mutate(
      n_users    = n_distinct(author_id),
      user_credit = 1.0 / n_users
    ) %>% ungroup()
  
  ## 5) Expand to country-sector combinations ----
  cs_frac <- user_frac %>%
    group_by(branch, author_id) %>%
    mutate(
      n_countries   = n_distinct(country_val),
      n_sectors     = n_distinct(sector_val),
      cross_frac    = user_credit / (n_countries * n_sectors)
    ) %>%
    ungroup()
  
  ## 6) Separate country aggregations ----
  total_us <- cs_frac %>%
    filter(country_val == "United States") %>%
    distinct(branch, min_commit_year, author_id, country_val, sector_val, cross_frac) %>%
    group_by(min_commit_year) %>%
    summarise(total_fraction = sum(cross_frac), .groups = "drop") %>%
    mutate(Sector = "Total (US)")
  
  total_global <- cs_frac %>%
    filter(country_val != "United States" & country_val != "Missing") %>%
    distinct(branch, min_commit_year, author_id, country_val, sector_val, cross_frac) %>%
    group_by(min_commit_year) %>%
    summarise(total_fraction = sum(cross_frac), .groups = "drop") %>%
    mutate(Sector = "Total (Global)")
  
  ## 7) Sector-Based Aggregation ----
  sector_agg <- cs_frac %>%
    filter(country_val == "United States") %>%
    distinct(branch, min_commit_year, author_id, sector_val, country_val, cross_frac) %>%
    group_by(sector_val, min_commit_year) %>%
    summarise(total_fraction = sum(cross_frac), .groups = "drop") %>%
    rename(Sector = sector_val)
  
  ## 8) Pivot and Merge ----
  year_cols <- as.character(2009:2023)
  
  summary_df <- bind_rows(
    total_us      %>% collect(),
    total_global       %>% collect(),
    sector_agg %>% collect()
  ) %>%
    mutate(year = as.character(min_commit_year)) %>%
    select(Sector, year, total_fraction) %>%
    pivot_wider(
      names_from  = year,
      values_from = total_fraction,
      values_fill = 0
    ) %>%
    select(Sector, all_of(intersect(year_cols, names(.)))) %>%
    mutate(across(all_of(year_cols), ~ round(.x, 0)))
  
  
  ## 9) Order Columns and Rows ----
  
  desired_order <- c("Total (Global)", "Total (US)", "Academic", "Government", "Business", "Nonprofit", "Unclassified")
  
  final <- summary_df %>%
    mutate(Sector = recode(Sector,
                           "academic" = "Academic",
                           "government" = "Government",
                           "business" = "Business",
                           "nonprofit" = "Nonprofit")) %>%
    mutate(Sector = factor(Sector, levels = desired_order)) %>%
    arrange(Sector)
  
  
  ## 10) Write to Excel ----
  if (!is.null(output_file)) {
    wb <- if (file.exists(output_file)) loadWorkbook(output_file) else createWorkbook()
    if (!(sheet_name %in% names(wb))) addWorksheet(wb, sheet_name)
    writeData(wb, sheet = sheet_name, x = final, startRow = start_row, colNames = TRUE)
    saveWorkbook(wb, output_file, overwrite = TRUE)
  }
  
  return(final)
}

#_______________________________________________________________________________

# ---- Production: Run for Sectors Table ----
#_______________________________________________________________________________

 # Define Connection
 con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)
 
 # Define file paths 
 users_file_path   <- "user_data_country_sectors_cleaned.parquet"
 commits_file_path <- "unique_commits_2009_2023.parquet"
 
 # Define output Excel file and sheet name 
 output_excel <- "\\\\westat.com\\DFS\\DVSTAT\\Individual Directories\\Askew\\sectoring\\Code\\Production\\Output_codegov\\SEI_2026_Shells_Output_Preliminary_codegov.xlsx"
 sheet <- "Table X-XXX"
 start_row <- 4
 
 
 result_sectors <- generate_sectors_table(
   con = con,
   users_file = users_file_path,
   commits_file = commits_file_path,
   output_file = output_excel,
   sheet_name = sheet,
   start_row = start_row
 )

 print(result_sectors)
 
 dbDisconnect(con)
