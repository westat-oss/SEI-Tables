library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)
library(openxlsx)
library(tidyr)
library(purrr)

# ______________________________________________________________________________
# ---- Function: generate_sectors_table_supplemental ----
#
# This function uses DuckDB to join the aggregated commits file with the users file.
# The users file is un-nested for the country field (from country_cleaned), with missing
# values labeled as "Missing". Fractional repository credit is computed per branch 
# (each branch’s total credit sums to 1). The function then aggregates the fractional user 
# credits to countries and earliest commit year, pivots the data so that columns represent 
# years (2009–2023), calculates an overall total per country, and selects the top 10 
# countries (by overall total). The final output has one column "Country" and one column
# per year, with numeric values rounded to whole numbers. The result is written to an 
# Excel sheet and returned as a data frame.
# ______________________________________________________________________________

generate_sectors_table_supplemental <- function(
    con,
    users_file,
    commits_file,
    output_file,
    sheet_name,
    start_row
) {
  # 1) Read and clean users ----
  users <- tbl(con, sql(paste0("SELECT * FROM read_parquet('", users_file, "')")))
  users_expanded <- users %>%
    mutate(country_list = string_split(country_cleaned, ",")) %>%
    mutate(country_val = sql("unnest(country_list)")) %>%
    mutate(
      country_val = trimws(country_val),
      country_val = ifelse(is.na(country_val) | country_val == "", "Missing", country_val)
    ) %>%
    select(id, country_val)
  
  # 2) Read commits ----
  commits <- tbl(con, sql(paste0("SELECT * FROM read_parquet('", commits_file, "')")))
  
  # 3) Join commits and users ----
  joined_data <- commits %>%
    inner_join(users_expanded, by = c("author_id" = "id")) %>%
    select(branch, min_commit_year, author_id, country_val)
  
  # 4) Compute user-level credit ----
  user_frac <- joined_data %>%
    group_by(branch) %>%
    mutate(n_users = n_distinct(author_id),
           user_credit = 1.0 / n_users) %>%
    ungroup()
  
  # 5) Split across countries ----
  country_frac <- user_frac %>%
    group_by(branch, author_id) %>%
    mutate(n_countries = n_distinct(country_val),
           fraction = user_credit / n_countries) %>%
    ungroup()
  
  # 6) Aggregate by country and year ----
  country_agg <- country_frac %>%
    distinct(branch, min_commit_year, author_id, country_val, fraction) %>%
    group_by(country_val, min_commit_year) %>%
    summarise(total_fraction = sum(fraction), .groups = "drop")
  
  # 7) Pivot to wide format ----
  country_agg_df <- country_agg %>% collect()
  
  year_cols <- as.character(2009:2023)
  country_wide <- country_agg_df %>%
    mutate(year = as.character(min_commit_year)) %>%
    select(country_val, year, total_fraction) %>%
    pivot_wider(
      names_from = year,
      values_from = total_fraction,
      values_fill = list(total_fraction = 0)
    )
  
  # 8) Select top 10 by overall total ----
  country_wide <- country_wide %>%
    mutate(overall_total = rowSums(across(all_of(intersect(year_cols, names(.)))))) %>%
    rename(Country = country_val) %>%
    select(Country, all_of(year_cols), overall_total)
  
  top10_countries <- country_wide %>%
    arrange(desc(overall_total)) %>%
    slice_head(n = 11) %>%
    select(-overall_total) %>%
    mutate(across(all_of(year_cols), ~ round(.x, 0)))
  
  # 9) Write to Excel ----
  if (!is.null(output_file)) {
    wb <- if (file.exists(output_file)) loadWorkbook(output_file) else createWorkbook()
    if (!(sheet_name %in% names(wb))) addWorksheet(wb, sheet_name)
    writeData(wb, sheet = sheet_name, x = top10_countries, startRow = start_row, colNames = TRUE)
    saveWorkbook(wb, output_file, overwrite = TRUE)
  }
  
  return(top10_countries)
}

#_______________________________________________________________________________

# ---- Production: Run for Supplemental Countries Table ----
#_______________________________________________________________________________

# Define Connection
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)

# Define file paths 
users_file_path   <- "user_data_country_sectors_cleaned.parquet"
commits_file_path <- "unique_commits_2009_2023.parquet"

# Define output Excel file and sheet name 
output_excel <- "\\\\westat.com\\DFS\\DVSTAT\\Individual Directories\\Askew\\sectoring\\Code\\Production\\Output_codegov\\SEI_2026_Shells_Output_Preliminary_codegov.xlsx"
sheet <- "Supp Data for Table XXX-X"
start_row <- 4

result_countries <- generate_sectors_table_supplemental(
  con = con,
  users_file = users_file_path,
  commits_file = commits_file_path,
  output_file = output_excel,
  sheet_name = sheet,
  start_row = start_row
)

print(result_countries)
dbDisconnect(con)
