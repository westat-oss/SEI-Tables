
# Data for Figure INV-A

**Caption**: International collaborations with the United States and with non-U.S. countries in development of open-source software, by 10 largest contributing countries.

**Time Period**: 2023

**Methods**: 

The data from `unique_commits` is filtered to the target time period and then joined with the users file `user_data_country_sectors_cleaned` based on the commit author ID (the `author_id` column in the commits data corresponds to the `id` column in the user data), resulting in data containing the fields: `branch`, `commit_year`, and `country`.

To identify collaborations with the U.S., the data are subsetted to only branches that have at least one commit author where `country == 'United States'`. For collaborations among non-U.S. countries, the data are subsetted to commit authors where `country != 'United States'`.

The table of counts is obtained separately for each dataset (the U.S. collaborations data and the non-U.S. collaborations data) by grouping the data by `country` and counting the distinct values of `branch`.

Note that for authors with a missing value of `country`, no imputation is conducted. These authors are simply grouped under the "Missing Country" category.

**Display Details**:

The table of counts is sorted in descending order based on the count of collaborations with the U.S.

# Supp Data for Figure INV-A

**Caption**: International collaboration with the United States and with non-U.S. countries in development of open-source software, by 50 largest contributing countries

**Time Period**: 2023

**Methods**:

This table is produced using the same method as the table "Data for Figure INV-A." The only difference is that the supplemental data table lists 50 countries rather than 10.

**Display Details**:

The table of counts is sorted in descending order based on the count of collaborations with the U.S.

# Data for Figure YYY-Y

**Caption**: GitHub collaboration network by country: 2023 (Number of GitHub repositories)

**Time Period**: 2023

**Methods**:

The data from `unique_commits` is filtered to the target time period and then joined with the users file `user_data_country_sectors_cleaned` based on the commit author ID (the `author_id` column in the commits data corresponds to the `id` column in the user data), resulting in data containing the fields `branch` and `country`. The data are deduplicated so as to only contain distinct combinations of `branch` and `country`, resulting in the dataset `co_dev_data`.

In `co_dev_data`, separately for each value of `branch`, a list of authors' countries is obtained. For that branch, a list is made of distinct pairs of observed countries, and the country with the first name based on alphabetical sorting is denoted `Country 1` and the other is denoted `Country 2`. For instance, the list of country pairs for a given branch could 'Canada - U.S.' but not 'U.S. - Canada'. The result is a dataset named `country_pairs` containing the fields `branch`, `Country1`, and `Country2`.

The table of counts of 'co-developed repositories' is obtained by grouping `country_pairs` by combinations of `Country1` and `Country2`, and counting the distinct values of `branch`.

**Display Details**:

The table is sorted in descending order based on the number of co-developed repositories.

# Total Repos

**Caption**: GitHub fractional repository count by country: 2023 (Number of Github repositories)

**Time Period**: 2023

**Methods**:

The data from `unique_commits` is filtered to the target time period and then joined with the users file `user_data_country_sectors_cleaned` based on the commit author ID (the `author_id` column in the commits data corresponds to the `id` column in the user data), resulting in data containing the fields `branch` and `country`. The data are deduplicated so as to only contain distinct combinations of `branch`, `author_id`, and `country`, resulting in the dataset `joined_data`. Branches are dropped from the data if they do not have at least one commit author with a non-missing value of `country`.

In `joined_data`, fractional credit for each branch is assigned to the author as one divided by the branch's number of distinct authors. The dataset with the authors' fractional credits is named `user_credit`.

In the dataset `user_credit`, fractional credit is then assigned to author-country combinations separately for each branch. For each branch, the author-country credit is calculated by dividing the author's fractional credit by the number of countries associated with that author. The resulting fractional dataset of author-country fractional credits for each branch is named `country_frac`.

The final fractional counts at the country level (denoted "Total repositories") are obtained by grouping `country_frac` by country and summing up the author-country fractional credits from all branches.

**Temporary QC Notes**: 

Check the method used to exclude branches that don't have at least one commit author with a non-missing value of `country`.

Also check that, when assigning fractional credit to countries within users, there are no missing values of `country`.

Check the step labeled "Aggregate to total fractional credit per country". It's not clear to me why the `distinct()` function would be need in the following: `country_frac %>% distinct(branch, author_id, country, fraction)`.

# Table SINV-83

**Caption**: Number of new GitHub repositories contributed to by selected entities: 2009-2023 (Number)

**Time Period**: 2009-2023

# Table INV-4

**Caption**: GitHub repositories contributed to by selected entities: 2009-2023

**Time Period**: 2009-2023

# Supp Data for Table INV-4

**Caption**: GitHub repositories contributed to by selected entities: 2009-2023 

**Time Period**: 2009-2023

# Table XXX-X

**Caption**: Number of new GitHub repositories contributed to by selected sectors in the U.S.: 2009-2023 (Number)

**Time Period**: 2009-2023

# Supp Data for Table XXX-X

**Caption**: Number of new GitHub repositories contributed to by 10 largest contributing countries: 2009-2023

**Time Period**: 2023
