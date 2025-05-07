# ______________________________________________________________________________

# ---- Load Packages ----
# ______________________________________________________________________________

 library(DBI)
 library(duckdb)
 library(dplyr)
 library(dbplyr)
 library(openxlsx)
 
# ______________________________________________________________________________
 
# ---- Function: generate_FigureINV_A ----
# This function uses DuckDB to join users and commits, then computes a table summarizing
# the number of U.S. collaborations, non-U.S. collaborations (branch may include U.S.), 
# non-U.S. collaborations (branches excluding U.S.). A collaboration on a branch means 
# there are at least two unique users within it, and at least two unique countries.
# The final summary is written to an Excel sheet.
#_______________________________________________________________________________ 
 

 # Function to generate the aggregated table for a single year.
 generate_FigureINV_A <- function(
    con,
    users_file,     # e.g., full path to "user_data_country_sectors_cleaned.parquet"
    commits_file,   # e.g., full path to "unique_commits_2009.parquet"
    year,           # The commit year to filter on
    us_country,     # value for country = "United States"
    top_n,          # Value for how many countries to output
    output_file,    # Excel template file to write results to
    sheet_name      # Sheet name in the Excel template where results will be written
 ) {
   
   ## 1) Reference the users parquet file as a lazy table ----
   users <- tbl(con, sql(paste0(
     "SELECT *, 
     FROM read_parquet('", 
     users_file, 
     "')"
     )))
   
   ## 2) Expand (unnest) the comma-separated countries ----
   users_expanded <- users %>%
     mutate(countries = string_split(country_cleaned, ',')) %>% # split unique values for users on ","
     select(id, countries) %>%      # "id" comes from the users file
     mutate(country = sql("unnest(countries)")) %>% # create separate rows for each country value
     mutate(country = trimws(country)) %>% # Trim any leading/trailing whitespace from country names
     mutate(country = ifelse(is.na(country) | country == "", "Missing Country", country)) # label NA as "Missing Country"
   
   ## 3) Explicitly read the commits file ---- (Nick - was having trouble reading table just from the connection)
   commits <- tbl(con, sql(paste0(
     "SELECT * FROM read_parquet('", 
     commits_file, 
     "')"
   )))
   
   ## 4) Join commits with the expanded user data ----
   joined_data <- commits %>%
     filter(commit_year == year) %>%
     inner_join(users_expanded, by = c("author_id" = "id"))  %>%
     select(branch, commit_year, author_id, country)
   
   ## 5) Identify branches that have at least one U.S. user ----
   repos_with_us <- joined_data %>%
     filter(country == us_country) %>%
     distinct(branch)
   
   ## 6) Count collaborations WITH the U.S ----
   # Must include:
   # - At least one U.S. user 
   # - At least one non-U.S. user
   # - At least 2 distinct users overall
  
   valid_with_us_branches <- joined_data %>%
    semi_join(repos_with_us, by = "branch") %>%
    group_by(branch) %>%
    summarise(
      n_users = n_distinct(author_id),
      has_non_us = any(country != us_country & country != "Missing Country"),
      .groups = "drop"
    ) %>%
    filter(n_users > 1, has_non_us)

   collaborations_with_us <- joined_data %>%
    semi_join(valid_with_us_branches, by = "branch") %>%
    filter(country != us_country & country != "Missing Country") %>%
    group_by(country) %>%
    summarise(collaborations_with_us = n_distinct(branch), .groups = "drop")
   
   ## 7) Count collaborations among NON-U.S. countries (U.S. may be present)----
   # Must include:
   # - At least 2 users
   # - At least 2 distinct non-U.S. countries (excluding "Missing Country")
  
  valid_non_us_inclusive <- joined_data %>%
    group_by(branch) %>%
    summarise(
      n_users = n_distinct(author_id),
      n_non_us_countries = n_distinct(country[country != us_country & country != "Missing Country"]),
      .groups = "drop"
    ) %>%
    filter(n_users > 1, n_non_us_countries > 1)
  
 collaborations_with_non_us <- joined_data %>%
    semi_join(valid_non_us_inclusive, by = "branch") %>%
    filter(country != us_country & country != "Missing Country") %>%
    group_by(country) %>%
    summarise(collaborations_with_non_us = n_distinct(branch), .groups = "drop")
  
  ## 8) Collaborations with NON-U.S. countries or economies, EXCLUDING U.S.----
  # Must include:
  # - No U.S. users
  # - At least 2 users
  # - At least 2 distinct non-"Missing Country" countries
  non_us_only <- joined_data %>%
    anti_join(repos_with_us, by = "branch")
  
  valid_non_us_exclusive <- non_us_only %>%
    group_by(branch) %>%
    summarise(
      n_users = n_distinct(author_id),
      n_countries = n_distinct(country[country != "Missing Country"]),
      .groups = "drop"
    ) %>%
    filter(n_users > 1, n_countries > 1)
  
  collaborations_non_us_excl_us <- non_us_only %>%
    semi_join(valid_non_us_exclusive, by = "branch") %>%
    filter(country != "Missing Country") %>%
    group_by(country) %>%
    summarise(collaborations_non_us_excl_us = n_distinct(branch), .groups = "drop")
   
   ## 9)  Combine all three counts into final table and export ----
   combined_table <- collaborations_with_us %>%
    full_join(collaborations_with_non_us, by = "country") %>%
    full_join(collaborations_non_us_excl_us, by = "country") %>%
    mutate(
      collaborations_with_us = coalesce(collaborations_with_us, 0),
      collaborations_with_non_us = coalesce(collaborations_with_non_us, 0),
      collaborations_non_us_excl_us = coalesce(collaborations_non_us_excl_us, 0)
    ) %>%
    arrange(desc(collaborations_with_us)) %>%
    head(top_n)
   
   ## 10) Pull the result into R memory and return it ----
   result <- combined_table %>%
    collect() %>%
    rename(
      "Country" = country,
      "Collaborations with United States" = collaborations_with_us,
      "Collaborations with non-U.S. countries or economies" = collaborations_with_non_us,
      "Collaborations with non-U.S. countries or economies, excluding U.S." = collaborations_non_us_excl_us
    )
   
   ## 11) Write the result to the excel workbook ----
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
     writeData(wb, sheet = sheet_name, x = result, startRow = 4, colNames = TRUE)
     saveWorkbook(wb, output_file, overwrite = TRUE)
   }
   
   return(result)
 }
 
 #______________________________________________________________________________
 
 # ---- Production: Run on the year 2023, run on 2024 when available ----
 #______________________________________________________________________________
 
 # define connection
 con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)
 
 # Full file paths for the parquet files 
 users_file_path <- "user_data_country_sectors_cleaned.parquet"
 commits_file_path <- "unique_commits_2023.parquet"
 
 # Define production year
 production_year <- 2023
 
 # Define output Excel file and sheet name
 output_excel <- "\\\\westat.com\\DFS\\DVSTAT\\Individual Directories\\Askew\\sectoring\\Code\\Production\\Output_codegov\\SEI_2026_Shells_Output_Preliminary_codegov.xlsx"
 sheet <- "Data for Figure INV-A"
 
 
 result_2023 <- generate_FigureINV_A(
   con          = con,
   users_file   = users_file_path,
   commits_file = commits_file_path,
   year         = production_year,
   us_country   = "United States",
   top_n        = 11, # asssuming missing will be in top 10, so we grab top 11       
   output_file  = output_excel,
   sheet_name   = sheet
 )
 
 # Print the result to see the output for 2023
 print(result_2023)
 
 #______________________________________________________________________________
 
 # --- Production: Supplemental ----
 #______________________________________________________________________________
 
 # define connection
 con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:", read_only = FALSE)
 
 # Full file paths for the parquet files 
 users_file_path <- "user_data_country_sectors_cleaned.parquet"
 commits_file_path <- "unique_commits_2023.parquet"
 
 # Define production year
 production_year <- 2023
 
 # Define output Excel file and sheet name
 output_excel <- "\\\\westat.com\\DFS\\DVSTAT\\Individual Directories\\Askew\\sectoring\\Code\\Production\\Output_codegov\\SEI_2026_Shells_Output_Preliminary_codegov.xlsx"
 sheet <- "Supp Data for Figure INV-A"
 
 
 result_2023_supp <- generate_FigureINV_A(
   con          = con,
   users_file   = users_file_path,
   commits_file = commits_file_path,
   year         = production_year,
   us_country   = "United States",
   top_n        = 51, # assuming missing will be in top 50, so we grab top 51       
   output_file  = output_excel,
   sheet_name   = sheet
 )
 
 # Print the result to see the output for 2023
 print(result_2023_supp)

 
