# SEI-Tables

This repository contains the R scripts and output files used to generate the **2026 Science and Engineering Indicators (SEI) tables** on open-source software (OSS) contributions.  
These tables draw on GitHub data (and related sources) to track international collaboration, sectoral participation, and contributions by U.S. federal agencies and institutions.  

## Repository Structure

- `Table Shell Code/` – R scripts used to clean data and generate SEI tables  
- `Table Shell Output/` – Output Excel files containing the finalized SEI tables  

---

## Workflow

1. **Data Cleaning**  
   Before producing tables, raw OSS datasets are processed using the cleaning scripts below. These scripts expand user metadata, standardize countries/sectors, and prepare commit-level data for analysis.  

   - `01_OSS_Data_Prep_Codegov.R` – Prepares Code.gov data for integration with GitHub datasets  
   - `01_OSS_Data_Prep_GitHub.R` – Prepares GitHub contributor and commit data (unnesting authors, handling missing country/sector entries, etc.)  

2. **Table Production**  
   Each script in the `Table Shell Code/` directory produces one or more tables. The output table names correspond to the sheet names in the final Excel workbook.

   ### Figure INV-A – International Collaborations with the U.S. and Non-U.S. Countries
   **Script:** `02_OSS_Fig_INVA.R`  
   **Purpose:** Summarizes international collaboration patterns in OSS by identifying repositories (branches) with contributors from multiple countries. Distinguishes between collaborations involving the United States and those occurring solely among non-U.S. countries. Highlights the ten most active contributing countries.  

   ---

   ### Figure YYY-Y – GitHub Collaboration Network by Country (2023)
   **Script:** `03_OSS_Codeveloped_Repos.R`  
   **Purpose:** Presents pairwise summaries of international collaborations, listing country pairs and the number of repositories they co-developed in 2023.  

   ---

   ### Total Repos Table – GitHub Fractional Repository Count by Country (2023)
   **Script:** `04_OSS_Total_Repos.R`  
   **Purpose:** Provides fractional counts of repositories attributed to each country. Repository credit is distributed proportionally across all countries with contributors, ensuring totals preserve the overall repository count.  

   ---

   ### Table SINV-83 – Number of New GitHub Repositories Contributed to by Selected U.S. Federal Agencies (2009–2023)
   **Script:** `05_OSS_Table_SINV83.R`  
   **Purpose:** Reports the number of new GitHub repositories contributed to by selected federal agencies. A repository is “new” if federal contributors appear during its first year of participation. Totals are reported by agency, overall, and an “All Other Federal” category.  

   ---

   ### Table INV-4 – GitHub Repositories Contributed to by Selected U.S. Federal Agencies (2009–2023)
   **Script:** `06_OSS_Table_INV4.R`  
   **Purpose:** Reports the total number of unique GitHub repositories contributed to by federal agencies over 2009–2023. Unlike SINV-83, this aggregates across the full time span.  

   ---

   ### Table INV-4 Supplemental – Top Academic and Business Contributors (2009–2023)
   **Script:** `07_OSS_Table_INV4_Supp.R`  
   **Purpose:** Presents the top 10 business and top 10 academic institutions worldwide contributing to OSS repositories. Uses the same methodology as INV-4, with category totals and “all other” rows.  

   ---

   ### Table XXX-X – New GitHub Repositories by U.S. Sector (2009–2023)
   **Script:** `08_OSS_Table_Sectors.R`  
   **Purpose:** Tracks annual participation in new repositories by U.S.-based contributors, distributing fractional credit across sectors (academic, business, government, nonprofit), geographic categories, and overall totals.  

   ---

   ### Supplemental Data for Table X-XXX – Top 10 Contributing Countries (2009–2023)
   **Script:** `09_OSS_Table_Sectors_Supplemental.R`  
   **Purpose:** Provides a time series of global participation in new repositories, highlighting the 10 most active countries. Includes totals, “all other countries,” and missing data categories.  

---

## Dependencies

All scripts are written in **R** and require the following packages (minimum versions recommended):  

- `dplyr`  
- `duckdb`  
- `dbplyr`  
- `openxlsx`  
- `tidyr`  
- `purrr`  

---

## Output

The final Excel workbook in `Table Shell Output/` contains each table as a separate sheet. Sheet names correspond directly to the table names listed above.  

---

For questions about methodology or script updates, please contact the SEI analysis team.


