# CA residential rates from FF1
# Sara Johns
# sjohns@berkeley.edu
# 1/22/2021

# 0. Set up

# Clear workspace
rm(list = ls())

# Load packages
library(pacman)
p_load(tidyverse, readxl, Hmisc, glue, foreign, stringr, data.table, lubridate, qs, rgdal, DescTools, dplyr)

dropbox <- "/Users/sarajohns/Dropbox/Sloan Residential Rate Database/FF1Sch60/"

#--------------------------------------------------------------------------------------------#
# 1. Read in and merge data
#--------------------------------------------------------------------------------------------#

# 1994 - 2018 hand cleaned in getResRates.R, pull in somewhat clean 2019 data from PUDL: https://data.catalyst.coop/
# all same info in PUDL data for full time period but residential rates haven't been separated out which makes it annoying

# 1994-2018
resDT <- as.data.table(read_csv(paste0(dropbox, "data/resRates/resrates_clean.csv"), 
                                guess_max = 34915))
# keep only CA IOUs in cleaned FERC form 1 data
ca_util <- resDT[(fercID==133 | fercID == 155 | fercID == 161),]
ca_10s <- ca_util[year > 2009,] # 2010-2018
# keep only columns we want
ca_10s <- ca_10s[, c("fercID", "utility", "year", "rateName", "MWh", "R", "N", "KWhperN", "P")]
colnames(ca_10s) <- c("fercID", "utility", "year", "rateName", "MWh", "revenue", "num_cstmr", "KWhper_cust", "avg_price")

# 1994-2019
pudl <- as.data.table(read_csv(paste0(dropbox, "data/resRates/f1_sales_by_sched.csv")))
pudl_ca <- pudl[(respondent_id==133 | respondent_id == 155 | respondent_id == 161) & report_year == 2019, ] # just keep CA IOUs, 2019
rm(pudl)
# get res rows in pudl data
pudl_cares <- pudl_ca[c(2:33, 98:416, 1809:1817),]
# keep/rename columns we want
pudl_cares <- pudl_cares[,c("respondent_id","respondent_id_label", "report_year", "sched_num_ttl", "mwh_sold", 
                            "revenue", "avg_num_cstmr", "kwh_sale_cstmr", "revenue_kwh_sold")] 
colnames(pudl_cares) <- c("fercID", "utility", "year", "rateName", "MWh", "revenue", "num_cstmr", "KWhper_cust", "avg_price")


# combine 2019 from pudl with 2010-2018
ca_all <- rbind(ca_10s, pudl_cares)

#--------------------------------------------------------------------------------------------#
# 2. Designate rates
#--------------------------------------------------------------------------------------------#

# make consistent rate name
ca_all[, rateName := toupper(rateName)]

# designate care & fera (hopefully catches most of them)
ca_all[, CARE_rate := ifelse(str_detect(rateName, "CARE"), 1, 0)]
ca_all[str_detect(rateName, "FERA"), CARE_rate := 1]
# other CARE rates?
ca_all[str_detect(rateName, "LOW-INCOME"), CARE_rate := 1]
ca_all[fercID==155 & rateName=="DRLI", CARE_rate := 1] # this seems to be the only SDGE CARE rate

# remove rates like lighting etc & missing
ca_all[str_detect(rateName, "OL"), remove := 1] # lighting
ca_all[str_detect(rateName, "DWL"), remove := 1] # lighting
ca_all[str_detect(rateName, "GS"), remove := 1] # SCE has a general service in its residential account, but I don't think it's a res rate
ca_all[str_detect(rateName, "PA") & fercID==161, remove := 1] # same for ag pumping
ca_all[str_detect(rateName, "MIS-RS"), remove := 1]
ca_all[avg_price <= 0, remove := 1]
ca_all[MWh <= 0, remove := 1]

ca_all[is.na(remove), remove:=0]

# add cleaner utility name
ca_all[, utility := ifelse(fercID==133, "PGE", 
                           ifelse(fercID==155, "SDGE", "SCE"))]

# note: leaves in multi-family, RV parks, domestic service to employees (SCE DE rate)

#--------------------------------------------------------------------------------------------#
# 3. Get average rate
#--------------------------------------------------------------------------------------------#

# sum MWh and revenue (Q and P) by utility, year, CARE rate
ca_summary <- ca_all[remove==0,.(sum_MWh = sum(MWh, na.rm=T),
                                    sum_revenue = sum(revenue, na.rm=T)),
                     by = c("fercID", "utility", "year", "CARE_rate")]
# calculate avg price per kwh
ca_summary[, avg_price := (sum_revenue / sum_MWh) / 1000]
# set order so csv is easy to look through
ca_summary <- ca_summary[order(utility, year)]

write_csv(ca_all, paste0(dropbox, "data/resRates/ca_all_rates.csv"))
write_csv(ca_summary, paste0(dropbox, "data/resRates/ca_summary.csv"))

