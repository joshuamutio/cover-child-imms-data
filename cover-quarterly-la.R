library(tidyverse)
library(rvest)
library(here)
library(readODS)
library(janitor)
library(readxl)
library(lubridate)

# thresholds from PHFA

optimal_performance_standard <- 0.95
efficiency_standard <- 0.9

# files pre-2018/19 don't have data tables and will need separate code for processing

urls <-
  c(
    "https://www.gov.uk/government/statistics/cover-of-vaccination-evaluated-rapidly-cover-programme-2018-to-2019-quarterly-data",
    "https://www.gov.uk/government/statistics/cover-of-vaccination-evaluated-rapidly-cover-programme-2019-to-2020-quarterly-data",
    "https://www.gov.uk/government/statistics/cover-of-vaccination-evaluated-rapidly-cover-programme-2020-to-2021-quarterly-data",
    "https://www.gov.uk/government/statistics/cover-of-vaccination-evaluated-rapidly-cover-programme-2021-to-2022-quarterly-data"
  )

data_files <- lapply(urls, function(url) {
  read_html(url) %>%
    html_nodes("a") %>%
    html_attr("href") %>%
    str_subset("\\.(ods|ODS|xlsx|xls|XLSX|XLS)") %>%
    # exclude GP-level files
    str_subset("GP|gp", negate = T) %>%
    unique()
}) %>%
  unlist() %>%
  # one file is archived
  c("https://webarchive.nationalarchives.gov.uk/ukgwa/20211123180403mp_/https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1020952/Q21-22_Q1_COVER_data_tables.ods") # 2021-22 Q1 has been archived

# create folder for downloads
dir.create(here("data", "cover-quarterly-la"),recursive = T)

# download
lapply(data_files, function(x) {
  download.file(x, here("data", "cover-quarterly-la", basename(x)), quiet = TRUE, mode = "wb")
}) 

cover_quarterly_la_files <- list.files(here("data", "cover-quarterly-la"), pattern = "*.(ods|ODS|xls|XLS|xlsx|XLSX)", full.names = T)

# read ODS/Excel files
cover_quarterly_la_df <- lapply(cover_quarterly_la_files, function(x) {
  if (grepl("\\.(ods|ODS)$", x)) {
    df <- read_ods(x, sheet = "Data_file", col_names = F)
    first_row <- which(tolower(df[, 1]) == "country")
    last_row <- nrow(df)
    last_col <- ncol(df)
    range <- paste0("R", first_row, "C1:R", last_row, "C", last_col)
    read_ods(x, sheet = "Data_file", range = range) %>%
      add_column(file = basename(x)) %>%
      mutate_each(as.character) %>%
      mutate_each(as.numeric, starts_with(c("1", "2", "5"))) %>%
      pivot_longer(
        cols = starts_with(c("1", "2", "5")),
        names_to = "metric",
        values_to = "value",
        values_drop_na = TRUE
      ) %>%
      clean_names() 
  } else if ("Data file" %in% excel_sheets(x)) {
    read_excel(x, sheet = "Data file") %>%
      add_column(file = basename(x)) %>%
      mutate_each(as.character) %>%
      mutate_each(as.numeric, starts_with(c("1", "2", "5"))) %>%
      pivot_longer(
        cols = starts_with(c("1", "2", "5")),
        names_to = "metric",
        values_to = "value",
        values_drop_na = TRUE
      ) %>%
      clean_names()
  }
}) %>%
  bind_rows()

cover_quarterly_la_df <- cover_quarterly_la_df %>%
  mutate(org_code = ifelse(is.na(upper_tier_local_authority_ods_code), ods_upper_tier_la_code, upper_tier_local_authority_ods_code)) %>%
  select(
    org_code,
    file,
    financial_year_quarter,
    metric,
    value
  )

cover_quarterly_la_df <- cover_quarterly_la_df %>%
  mutate("metric_lower" = tolower(metric))

cover_quarterly_la_df$metric_lower <- str_replace_all(cover_quarterly_la_df$metric_lower, "numerator", "num")
cover_quarterly_la_df$metric_lower <- str_replace_all(cover_quarterly_la_df$metric_lower, "denominator", "den")
cover_quarterly_la_df$metric_lower <- str_replace_all(cover_quarterly_la_df$metric_lower, "denom", "den")
cover_quarterly_la_df$metric_lower <- str_replace_all(cover_quarterly_la_df$metric_lower, "primary", "prim")
cover_quarterly_la_df$metric_lower <- str_replace_all(cover_quarterly_la_df$metric_lower, "booster", "boo")
cover_quarterly_la_df$metric_lower <- str_replace_all(cover_quarterly_la_df$metric_lower, " ", "_")

cover_quarterly_la_df <- cover_quarterly_la_df %>%
  filter(str_count(metric_lower, "_") > 1) %>%
  mutate(metric = metric_lower) %>%
  select(!metric_lower) %>%
  separate(metric, sep = -4, into = c("metric", "name")) %>%
  pivot_wider(names_from = name, values_from = value) %>%
  select(financial_year_quarter, file, org_code, metric, num = `_num`, den = `_den`)

# Q19-20_Q4 is missing date column:

cover_quarterly_la_df %>%
  filter(is.na(financial_year_quarter)) %>%
  distinct(file)
cover_quarterly_la_df_1920Q4 <- cover_quarterly_la_df %>%
  filter(file == "Q19-20_Q4_web_tables.ods") %>%
  mutate(financial_year_quarter = "FY2019_20_Q4")
cover_quarterly_la_df <- cover_quarterly_la_df %>%
  filter(file != "Q19-20_Q4_web_tables.ods") %>%
  rbind(cover_quarterly_la_df_1920Q4)

# join via NHSEI hierarchies master

la_stp_mapping <- read_csv("https://cubestoragepublic.blob.core.windows.net/cube-public/NHSEIHierarchiesMaster/NHSEIHierarchiesMaster.csv")

# replace codes
cover_quarterly_la_df$org_code[cover_quarterly_la_df$org_code == 810] <- 738 # bournemouth
cover_quarterly_la_df$org_code[cover_quarterly_la_df$org_code == 811] <- 738 # poole
cover_quarterly_la_df$org_code[cover_quarterly_la_df$org_code == 612] <- 916 # buckinghamshire

# 504 - northamptonshire split - to do

cover_quarterly_la_df <- cover_quarterly_la_df %>%
  left_join(la_stp_mapping, by = c("org_code" = "OrganisationCode")) %>%
  select(financial_year_quarter,
         Region = RegionName,
         STP = STPName,
         OrganisationName,
         metric,
         den,
         num
  )

cover_quarterly_la_df %>%
  filter(grepl("NonSTP", STP, fixed = TRUE)) %>%
  group_by(OrganisationName) %>%
  summarise() # should be blank

# replace metric names
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "12m_dtapipvhib3", "12m DTaP/IPV/Hib3")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "12m_menb", "12m MenB2")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "12m_pcv1", "12m PCV1")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "12m_pcv2", "12m PCV2")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "12m_pcv", "12m PCV2")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "12m_rota", "12m Rota2")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "24m_dtapipvhib3_prim", "24m DTaP/IPV/Hib3")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "24m_hibmenc_boo", "24m Hib/MenC")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "24m_menb_boo", "24m MenB-booster")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "24m_mmr1", "24m MMR1")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "24m_pcv_boo", "24m PCV-booster")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "5y_dtapipv_boo", "5yr DTaP/IPV-booster")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "5y_dtapipvhib3_prim", "5yr DTaP/IPV/Hib3")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "5y_hibmenc_boo", "5yr Hib/MenC")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "5y_mmr1", "5yr MMR1")
cover_quarterly_la_df$metric <- str_replace_all(cover_quarterly_la_df$metric, "5y_mmr2_boo", "5yr MMR2")

# standards and dates
cover_quarterly_la_df <- cover_quarterly_la_df %>%
  mutate("Efficiency standard" = efficiency_standard) %>%
  mutate("Optimal performance standard" = optimal_performance_standard) %>%
  mutate("Dataset" = "Coverage of vaccination evaluated rapidly (COVER) quarterly data") %>%
  mutate(Year = if_else(substr(cover_quarterly_la_df$financial_year_quarter, 12, 12) == "4",
                        as.double(substr(financial_year_quarter, 3, 6)) + 1,
                        as.double(substr(cover_quarterly_la_df$financial_year_quarter, 3, 6))
  )) %>%
  mutate(month = if_else(substr(cover_quarterly_la_df$financial_year_quarter, 12, 12) == "4", "01",
                         if_else(substr(cover_quarterly_la_df$financial_year_quarter, 12, 12) == "1", "04",
                                 if_else(substr(cover_quarterly_la_df$financial_year_quarter, 12, 12) == "2", "07", "10")
                         )
  ))

cover_quarterly_la_df <- cover_quarterly_la_df %>%
  mutate(date = ymd(paste0(cover_quarterly_la_df$Year, cover_quarterly_la_df$month, "01"))) %>%
  mutate("Frequency" = "Quarterly") %>%
  separate(metric, into = c("cohort", "vaccination"), sep = " ", remove = F)

cover_quarterly_la_df_final <- cover_quarterly_la_df %>%
  mutate(Quarter = str_replace_all(financial_year_quarter, c("_Q" = " Q", "FY" = "", "_" = "-"))) %>%
  # pcv dosing schedule changed
  filter(metric != "12m PCV2") %>%
  mutate(LastUpdated = Sys.time())

# write to csv

write.csv(cover_quarterly_la_df_final, here("cover-quarterly-la.csv"), row.names = F)

# remove folder

unlink(here("data"), recursive = T)
