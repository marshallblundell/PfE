#################################################################################
#     File Name           :     C:/Users/Marshall/Dropbox/Next 10 Social Programs in Electric Rates/Report1Repo/pfe_paper1_analysis_public.R
#     Created By          :     Marshall Blundell
#     Creation Date       :     [2020-10-21 14:39]
#     Last Modified       :     [2021-02-22 11:25]
#     Description         :      
#
#     Written in R Version 3.6.2.
#     
#     Runs all analysis for first "Paying for Electricity" paper.
#
#     Outputs all graphs used in the paper, as well as some extra graphs
#     and tables.
#
#################################################################################

# 
# Preamble.
#
library(pacman)
p_load(tidyverse, lubridate, readxl, stringr, data.table, writexl)

# Change this to your directory
dropbox.dir <- "C:/Users/Marshall/Dropbox/Next 10 Social Programs in Electric Rates/Report1Data/"

# 
# Function to run BTM PV production simulation using LBNL tracking the sun data and PV Watts
#
# If you wish to run this you must download LBNL's 2019 tracking the sun data and put it in the OriginalData\lbnl_pv_data_public_tts folder: https://emp.lbl.gov/tracking-the-sun
#
# Run-time is long because I pause to circumvent rate
# limits in the PV Watts API. Repository has the output
# already so you should not have to run it.
#
# If you do want to run your own simulation you need a
# pv watts key.
#
run.pv.sim <- function(pv.watts.key = "") {

    p_load(jsonlite, httr)

    # 
    # Load and clean tts data
    #

    # Load - drop a few variables that get read as different
    # types that we don't need, make zip character
    pv.dat <- read_csv(paste0(dropbox.dir, "OriginalData/", "lbnl_pv_data_public_tts/TTS_LBNL_public_file_19-Oct-2020_all.csv"))

    # Change var names to lower case
    colnames(pv.dat) <- tolower(colnames(pv.dat))
    # Remove spaces
    colnames(pv.dat) <- str_replace_all(colnames(pv.dat), " ", ".")
    colnames(pv.dat) <- str_replace_all(colnames(pv.dat), "_", ".")
    # Remove hash
    colnames(pv.dat) <- str_replace_all(colnames(pv.dat), "#", "")

    # Convert missing values to R missing values
    missing.replace <- function(var) {
        if (class(var) == "character") {
            var[which(var == "-9999")] <- NA
        } else {
            var[which(var == -9999)] <- NA
        }
        return(var)
    }
    pv.dat <- pv.dat %>% 
        mutate_all(missing.replace)

    # Manage date
    pv.dat <- pv.dat %>% 
        mutate(installation.date = date(dmy_hms(installation.date)))

    # Clean zip code to be just the first 5 digits
    # Filter any out that have fewer
    pv.dat <- pv.dat %>% 
        mutate(zip.code = str_sub(zip.code, 1, 5)) %>%
        filter(str_length(zip.code) == 5) %>%
        rename(system.size = system.size.dc)

    # Examine providers if the state is in CA
    sum(is.na(pv.dat$state)) # Never missing
    # Filter to large 3 ious and residential
    pv.dat <- pv.dat %>% 
        filter(utility.service.territory %in% c("Pacific Gas and Electric", "San Diego Gas and Electric", "Southern California Edison")) %>%
        mutate(iou = case_when(utility.service.territory == "Pacific Gas and Electric" ~ "pge",
                               utility.service.territory == "San Diego Gas and Electric" ~ "sdge",
                               utility.service.territory == "Southern California Edison" ~ "sce")) %>%
        select(-utility.service.territory)

    # Save
    write_rds(pv.dat, paste0(dropbox.dir, "ModifiedData/", "pv.dat.rds"))

    # 
    # Restrict to CA IOUs and clean for pv watts requests
    #
    pv.dat <- read_rds(paste0(dropbox.dir, "ModifiedData/", "pv.dat.rds"))

    # Restrict to res
    pv.dat <- pv.dat %>% 
        filter(customer.segment == "RES")

    # If we are missing tilt, azimuth, module.technology, or
    # module efficiency variable 1, replace with 2, then with 3
    pv.dat <- pv.dat %>% 
        mutate(
               tilt = case_when(is.na(tilt.1) ~ tilt.2,
                                is.na(tilt.1) & is.na(tilt.2) ~ tilt.3,
                                TRUE ~ tilt.1),
               azimuth = case_when(is.na(azimuth.1) ~ azimuth.2,
                                is.na(azimuth.1) & is.na(azimuth.2) ~ azimuth.3,
                                TRUE ~ azimuth.1))

    pv.dat.requests <- pv.dat %>% 
        select(installation.date, system.size, tracking, ground.mounted, zip.code, city, state, iou, tilt, azimuth)

    # Assess missingness
    lapply(pv.dat.requests, function(x) sum(is.na(x)) / dim(pv.dat.requests)[1])

    # A few (~15obs) have missing zip - drop these
    pv.dat.requests <- pv.dat.requests %>%
        filter(!is.na(zip.code) & str_length(zip.code == 5))

    # Aggregate to get avg tilt, azimuth, efficiency, system size (avg. and aggregate) by zip code and IOU
    request.dat <- pv.dat.requests %>% 
        group_by(zip.code, iou) %>% 
        summarize(count = n(), avg.tilt = mean(tilt, na.rm=T), avg.azimuth = mean(azimuth, na.rm=T), avg.capacity = mean(system.size, na.rm=T), agg.capacity = sum(system.size, na.rm=T))
    request.dat <- ungroup(request.dat)

    # Fill in azimuth with 180 for those zip codes missing it

    # Read in file of zip code lat/long centroids
    zips <- read_tsv(paste0(dropbox.dir, "OriginalData/USzipcentroids/US.txt"), col_names = F)
    zips <- zips %>% 
        rename(zip.code = X2, lat = X10, lon = X11) %>% 
        select(zip.code, lat, lon)
    zips <- zips %>% mutate(zip.code = as.character(zip.code))

    # Merge
    request.dat <- left_join(request.dat, zips)

    # Read in file of zip code lat/long centroids for 2018
    zips.gaz.18 <- read_tsv(paste0(dropbox.dir, "OriginalData/2018_Gaz_zcta_national/2018_Gaz_zcta_national.txt"), col_names = T)
    zips.gaz.18 <- zips.gaz.18 %>% 
        rename(zip.code = GEOID, lat = INTPTLAT, lon = INTPTLONG) %>% 
        select(zip.code, lat, lon)

    # Read in file of zip code lat/long centroids for 2010
    zips.gaz.10 <- read_tsv(paste0(dropbox.dir, "OriginalData/2010_Gaz_zcta_national/Gaz_zcta_national.txt"), col_names = T)
    zips.gaz.10 <- zips.gaz.10 %>% 
        rename(zip.code = GEOID, lat = INTPTLAT, lon = INTPTLONG) %>% 
        select(zip.code, lat, lon)

    # Merge with 2018 gazeteer file and update
    request.dat <- left_join(request.dat, zips.gaz.18, by = c("zip.code"))
    request.dat <- request.dat %>% 
        mutate(
               lat.x = if_else(is.na(lat.x), lat.y, lat.x), 
               lon.x = if_else(is.na(lon.x), lon.y, lon.x)
        )
    request.dat <- request.dat %>% 
        select(-lat.y, -lon.y) %>% 
        rename(lat = lat.x, lon = lon.x)

    # Merge with 2010 gazeteer file and update
    request.dat <- left_join(request.dat, zips.gaz.10, by = c("zip.code"))
    request.dat <- request.dat %>% 
        mutate(lat.x = if_else(is.na(lat.x), lat.y, lat.x), lon.x = if_else(is.na(lon.x), lon.y, lon.x))
    request.dat <- request.dat %>% 
        select(-lat.y, -lon.y) %>% 
        rename(lat = lat.x, lon = lon.x)

    # Save missing - only 283 customers so not enough to matter
    miss <- request.dat %>% 
        filter(is.na(lat))
    sum(miss$count)
    write_rds(miss, paste0(dropbox.dir, "ModifiedData/miss.zips.lat.lon.rds"))

    # Filter the rest and make requests
    request.dat <- request.dat %>% 
        filter(!is.na(lat))

    # Fill in azimuth, tilt
    request.dat <- request.dat %>% 
        mutate(
               avg.tilt = if_else(is.na(avg.tilt), lat, avg.tilt),
               avg.azimuth = if_else(is.na(avg.azimuth), 180, avg.azimuth)
               )

    # Make request headers
    # USE YOUR OWN KEY, not mine. Go to PV Watts to make one
    url.prefix <- "https://developer.nrel.gov/api/pvwatts/v6.json?"

    # Use value of 14% for losses - from nrel default values
    request.dat <- request.dat %>% 
        mutate(url = paste0(url.prefix, "api_key=", pv.watts.key, "&lat=", lat, "&lon=", lon, "&azimuth=", avg.azimuth, "&tilt=", avg.tilt, "&system_capacity=", avg.capacity, "&module_type=0", "&array_type=1", "&losses=14", "&timeframe=hourly"))

    make.request.save.data <- function(url) {

        raw.result <- GET(url)
        this.raw.content <- rawToChar(raw.result$content)
        this.content <- fromJSON(this.raw.content)


        tib <- tryCatch(
                        {
                            tibble("lat" = this.content$inputs$lat,
                                "lon" = this.content$inputs$lon,
                                "azimuth" = this.content$inputs$azimuth,
                                "tilt" = this.content$inputs$tilt,
                                "system_capacity" = this.content$inputs$system_capacity,
                                "module_type" = this.content$inputs$module_type,
                                "array_type" = this.content$inputs$array_type,
                                "losses" = this.content$inputs$losses,
                                "station.state" = this.content$station_info$state,
                                "ac" = this.content$output$ac,
                                "poa" = this.content$output$poa,
                                "dn" = this.content$output$dn,
                                "dc" = this.content$output$dc,
                                "df" = this.content$output$df,
                                "tamb" = this.content$output$tcell,
                                "wspd" = this.content$output$wspd)},
                        error = function(cond) {
                            print(this.content$errors)
                            return(NULL)
                        }
        )
        return(tib)
    }

    # Grab data but pause for an hour every 1000
    len <- length(request.dat$url)
    requests.output <- vector(mode = "list", length = len)
    for(i in 1:len) {
        if(i %% 1000 == 0) {
            Sys.sleep(61*60)
        }
        if(i %% 100 == 0) {
            print(i)
        }
        requests.output[[i]] <- make.request.save.data(request.dat$url[i])
    }

    # Save
    write_rds(requests.output, paste0(dropbox.dir, "ModifiedData/requests.output.rds"))

    # 
    # Load and clean pv watts output, merge with zip-year level
    # data
    #
    requests.output <- read_rds(paste0(dropbox.dir, "ModifiedData/requests.output.rds"))

    # Go through output and clean
    clean.requests.output <- function(i) {
        if (!is.null(requests.output[[i]])) {
            cleaned <- requests.output[[i]] %>% 
                select(station.state, ac, poa, dn, dc, df, tamb, wspd) %>% 
                mutate(hour = as.integer(row.names(.)))
            cleaned <- bind_cols(request.dat[i,] %>% slice(rep(1:n(), each = dim(cleaned)[1])), cleaned)
            output <- cleaned
        } else {
            output <- NULL
        }
        return(output)
    }

    len <- length(request.dat$url)
    cleaned <- lapply(1:len, clean.requests.output)
    final <- do.call(bind_rows, cleaned)
    final <- final %>% 
        rename(
               pvwatts.tilt = avg.tilt,
               pvwatts.azimuth = avg.azimuth,
               pvwatts.capacity = avg.capacity,
               pvwatts.lat = lat,
               pvwatts.lon = lon,
               pvwatts.ac = ac,
               pvwatts.poa = poa,
               pvwatts.dn = dn,
               pvwatts.dc = dc,
               pvwatts.df = df,
               pvwatts.tamb = tamb,
               pvwatts.wspd = wspd
               ) %>% 
        select(-count, -url, -agg.capacity)

    # Make year 
    pv.dat.requests$inst.year <- year(pv.dat.requests$installation.date)

    # Now make a panel dataset, where each year is actually
    # the population of panels installed during or before
    # a given year
    years <- 2010:2019
    keep.and.add.year <- function(panel.year) {
        dat <- pv.dat.requests %>% 
            filter(inst.year <= panel.year) %>% 
            mutate(panel.year = panel.year)
        return(dat)
    }
    pv.dat.requests.panel <- do.call(bind_rows, lapply(years, keep.and.add.year))


    # Now get a zip-code year dataset
    # Aggregate to get system size (avg. and aggregate) by zip code, year
    panel.dat <- pv.dat.requests.panel %>% 
        group_by(zip.code, panel.year, iou) %>% 
        summarize(count = n(), avg.capacity = mean(system.size, na.rm=T), agg.capacity = sum(system.size, na.rm=T))
    panel.dat <- ungroup(panel.dat)

    # Get rid of variables we don't need in the pvwatts data
    # so the merge is more efficient
    final <- final %>% 
        select(zip.code, iou, pvwatts.capacity, pvwatts.ac, hour)

    # Save the two pieces - pv watts data (zip-hour level) and 
    # the lbnl data (zip-year level)
    write_rds(panel.dat, paste0(dropbox.dir, "ModifiedData/", "lbnl.pv.cleaned.for.pvwatts.rds"))
    write_rds(final, paste0(dropbox.dir, "ModifiedData/", "pvwatts.output.minimal.cleaned.rds"))

    # 
    # Get quick annual gen and capacity by iou.
    # This is all we use for report 1.
    #

    # Collapse pv watts data to annual just to get ball park gen number
    # This was a quick summary for meredith
    final.annual <- final %>% 
        group_by(zip.code, iou, pvwatts.capacity) %>% 
        summarize(pvwatts.ac.kwh = sum(pvwatts.ac / 1000))

    annual.zip.gen <- left_join(panel.dat, final.annual)
    annual.zip.gen <- annual.zip.gen %>% 
        filter(!is.na(pvwatts.ac.kwh))

    annual.zip.gen <- annual.zip.gen %>% 
        mutate(annual.gen = pvwatts.ac.kwh * (agg.capacity / pvwatts.capacity))

    annual.gen <- annual.zip.gen %>% 
        group_by(iou, panel.year) %>% 
        summarize(annual.gen = sum(annual.gen), capacity = sum(agg.capacity))
    annual.gen <- annual.gen %>% 
        mutate(annual.gen.gwh = annual.gen / 1e6, capacity.gw = capacity / 1e6)

    annual.gen.meredith <- annual.gen %>% 
        select(iou, panel.year, annual.gen.gwh, capacity.gw)

    write_csv(meredith, paste0(dropbox.dir, "ModifiedData/", "pv.prod.by.utility.year.csv"))

}

# 
# Function to clean some data we need later.
# Repo includes cleaned datasets so you should not have to run this.
#
# In particular:
#
# BTM PV production simulation using LBNL tracking the sun data and PV Watts ** Takes a few hours to run because of rate limits on PV Watts. **
# Ferc form 1
# Gas prices
# 2019 rates using advice letters/tariff sheets
#
# If you do run this, you must go to PV Watts site and generate your own key, and hcange that variable below in the function call to the pv simulation.
#

run.preliminary.data.cleaning <- function(pv.sim = F, ferc.clean = F, gas.prices.clean = F, compute.2019.rates = F) {
    if (pv.sim == T) {
        # You must have a key here if you run this
        run.pv.sim(pv.watts.key = "")
    }

    if (ferc.clean == T) {

        # 
        # FERC form 1 Load and clean - this is the newer source
        #
        ferc.f1 <- read_excel(paste0(dropbox.dir, "OriginalData/ferc_form_1/snl_regulated_energy_companies_expenses_pull_with_all_2019_data.xlsx"), sheet = "cleaned")

        # Make long
        colnames(ferc.f1) <- str_replace(colnames(ferc.f1), " ([0-9]{4})", "_\\1")
        ferc.f1 <- ferc.f1 %>% pivot_longer(cols = 'Total Intangible Plant: Add ($000)_2019':'Adminstrative & General Maintenance Expense ($000)_1988', names_to = c(".value", "year"), names_sep = "_")

        # Rename non-financial vars
        ferc.f1 <- ferc.f1 %>% dplyr::rename(utility = 'Company Name',
                                      snl.id = 'SNL Institution Key', 
                                      eia.id = 'EIA Utility Code',
                                      electric = 'Electric? Yes/No',
                                      electric.gen = 'Electric Generation? Yes/No',
                                      regulated.gen = 'Regulated Generation? Yes/No',
                                      ipp.merchant = 'IPP/Merchant? Yes/No',
                                      elec.trans = 'Electric Transmission? Yes/No',
                                      elec.dist = 'Electric Distribution? Yes/No',
                                      states.of.operation = 'Electric States of Operation')

        # Rename financial vars
        ferc.f1 <- ferc.f1 %>% dplyr::rename(intangible.capital.additions = 'Total Intangible Plant: Add ($000)',
                                      generation.capital.additions = 'Total Production Plant: Add ($000)',
                                      transmission.capital.additions = 'Total Transmission Plant: Add ($000)',
                                      distribution.capital.additions = 'Total Distribution Plant: Add ($000)',
                                      admin.capital.additions = 'Total General Plant: Add ($000)',
                                      regional.trans.mkt.operations.capital.additions = 'Total Regional Transmission & Mkt Oper: Add ($000)',
                                      plant.in.service.capital.additions = 'Total Elec Plant In Svc: Add ($000)',
                                      gen.op.expense = 'Power Production Total Operations Expense ($000)',
                                      gen.maint.expense = 'Power Production Total Maintenance Expense ($000)',
                                      transmission.op.expense = 'Transmission Total Operations Expense ($000)',
                                      transmission.main.expense = 'Transmission Total Maintenance Expense ($000)',
                                      regional.trans.mkt.operations.op.expense = 'Regional Mkt Total Operations Expense ($000)',
                                      regional.trans.mkt.operations.maint.expense = 'Regional Market Total Maintenance Expense ($000)',
                                      distribution.op.expense = 'Distribution Total Operations Expense ($000)',
                                      distribution.main.expense = 'Distribution Total Maintenance Expense ($000)',
                                      customer.accounts.expense = 'Total Customer Accounts Expense ($000)',
                                      customer.svc.and.informational.expense = 'Total Customer Svc & Informational Expense ($000)',
                                      sales.expense = 'Total Sales Expense ($000)',
                                      admin.op.expense = 'Adminstrative & General Total Operations Expense ($000)',
                                      admin.main.expense = 'Adminstrative & General Maintenance Expense ($000)')

        # 
        # Load and clean sales and customer numbers
        #
        ferc.f1.sales <- read_excel(paste0(dropbox.dir, "OriginalData/ferc_form_1/snl_regulated_energy_companies_sales_and_customer_info_pull_with_all_2019_data.xlsx"), sheet = "cleaned")

        # Make long
        colnames(ferc.f1.sales) <- str_replace(colnames(ferc.f1.sales), " ([0-9]{4})", "_\\1")
        ferc.f1.sales <- ferc.f1.sales %>% pivot_longer(cols = 'Residential Electric Volume, Total (MWh)_2019':'Total Retail Electric Volume, Total (MWh)_1988', names_to = c(".value", "year"), names_sep = "_")

        # Rename identifiers
        ferc.f1.sales <- ferc.f1.sales %>% dplyr::rename(utility = 'Company Name',
                                      snl.id = 'SNL Institution Key')

        # Rename others and for now just keep
        ferc.f1.sales <- ferc.f1.sales %>% dplyr::rename(total.retail.price.bundled.cents.per.kwh = starts_with('Total Retail Electric Price, Bundled ('),
                                                  total.retail.price.cents.per.kwh = starts_with('Total Retail Electric Price, Total'),
                                                  total.sales.mwh = 'Total Sales of Electricity Volume (MWh)',
                                                  total.customers = 'Total Retail Electric Customers, Total (actual)',
                                                  total.res.customers = 'Residential Electric Customers, Total (actual)',
                                                  res.price.cents.per.kwh = starts_with('Residential Electric Price, Total'),
                                                  bundled.res.price.cents.per.kwh = starts_with('Residential Electric Price, Bundled'),
                                                  total.retail.sales.mwh = 'Total Retail Electric Volume, Total (MWh)')

        ferc.f1.sales <- ferc.f1.sales %>% select(utility, snl.id, year, bundled.res.price.cents.per.kwh, res.price.cents.per.kwh, starts_with("total"))

        ferc.f1 <- merge(ferc.f1, ferc.f1.sales)

        # Drop companies that only operate in AK or HI
        # This is Alaska Electric Light and Power Company, Hawaiian Electric Company Inc., Maui Electric Company Limited, Hawaii Electric Light Company Inc.
        ferc.f1 <- ferc.f1 %>% filter(!(states.of.operation %in% c("AK", "HI")))

        ferc.f1 <- as_tibble(ferc.f1)

        # Make numeric
        ferc.f1 <- ferc.f1 %>% mutate_at(12:38,as.numeric)

        # Make res price in dollars per kwh
        ferc.f1 <- ferc.f1 %>% 
            mutate(res.price.dollars.per.kwh = res.price.cents.per.kwh / 100,
                   bundled.res.price.dollars.per.kwh = bundled.res.price.cents.per.kwh / 100) %>% 
            select(-res.price.cents.per.kwh, -bundled.res.price.cents.per.kwh)

        # Drop if an obs is missing all values for t or d but not
        # missing sales
        dim(ferc.f1)
        ferc.f1 <- ferc.f1 %>% filter(!(is.na(distribution.capital.additions) & is.na(distribution.op.expense) & is.na(distribution.main.expense) & !is.na(total.sales.mwh)))
        ferc.f1 <- ferc.f1 %>% filter(!(is.na(transmission.capital.additions) & is.na(transmission.op.expense) & is.na(transmission.main.expense) & !is.na(total.sales.mwh)))
        ferc.f1 <- ferc.f1 %>% filter(!(is.na(generation.capital.additions) & is.na(gen.op.expense) & is.na(gen.maint.expense) & !is.na(total.sales.mwh)))
        ferc.f1 <- ferc.f1 %>% filter(!(is.na(admin.capital.additions) & is.na(admin.op.expense) & is.na(admin.main.expense) & is.na(customer.accounts.expense) & is.na(customer.svc.and.informational.expense) & is.na(sales.expense) & !is.na(total.sales.mwh)))

        # Get totals
        ferc.f1 <- ferc.f1 %>% rowwise() %>% mutate(d.total = sum(distribution.capital.additions, distribution.op.expense, distribution.main.expense, na.rm=T),
                                                    t.total = sum(transmission.capital.additions, transmission.op.expense, transmission.main.expense, na.rm=T),
                                                    g.total = sum(generation.capital.additions, gen.op.expense, gen.maint.expense, na.rm=T),
                                                    admin.other.total = sum(admin.capital.additions, admin.op.expense, admin.main.expense, customer.accounts.expense, customer.svc.and.informational.expense, sales.expense, na.rm = T))

        # Mark CA vs rest of us
        ferc.f1 <- ferc.f1 %>% mutate(utility = str_to_lower(utility))
        ferc.f1 <- ferc.f1 %>% mutate(ca.iou = utility %in% c("pacific gas and electric company", "southern california edison company", "san diego gas & electric company"))

        # Load and clean consumption data for CA IOUs so we can compare
        # Load consumption data
        c <- read_csv(paste0(dropbox.dir, "OriginalData/consumption_by_utility_year_sector/", "ElectricityByUtility.csv")) %>% 
            select(-"Utility Type") %>% 
            rename(total.gwh = "Total Usage", res.gwh = "Residential", utility = "Utility Name", year = Year) %>% 
            select(utility, year, res.gwh, total.gwh) %>% 
            mutate(utility = str_to_lower(utility))

        c <- c %>% 
            mutate(
                   utility = if_else(utility == "san diego gas and electric company", "san diego gas & electric company", utility),
                   res.mwh.cec.data = res.gwh * 1000, total.mwh.cec.data = total.gwh * 1000
                   ) %>% 
            select(-ends_with("gwh"))

        # Merge in CEC consumption data for CA IOUs and compare
        ferc.f1 <- merge(ferc.f1, c, all.x = T)

        # Make a quick plot comparing CEC vs ferc form 1 data on sales - SCE and PG&E are fine
        cec.v.ferc.data <- ferc.f1 %>% 
            filter(ca.iou == T) %>% 
            select(utility, year, total.sales.mwh, total.retail.sales.mwh, total.mwh.cec.data) %>%
            mutate(total.retail.sales.mwh = as.numeric(total.retail.sales.mwh))
        cec.v.ferc.data <- cec.v.ferc.data %>% pivot_longer(cols = c("total.sales.mwh", "total.retail.sales.mwh", "total.mwh.cec.data"))
        cec.v.ferc.line <- ggplot(cec.v.ferc.data %>% filter(utility == "san diego gas & electric company"), aes(x = as.factor(year), y = value, color = name, group = name)) +
            geom_line() +
            theme(axis.text=element_text(size=7))
        cec.v.ferc.line
        cec.v.ferc.line <- ggplot(cec.v.ferc.data %>% filter(utility == "pacific gas and electric company"), aes(x = as.factor(year), y = value, color = name, group = name)) +
            geom_line() +
            theme(axis.text=element_text(size=7))
        cec.v.ferc.line
        cec.v.ferc.line <- ggplot(cec.v.ferc.data %>% filter(utility == "southern california edison company"), aes(x = as.factor(year), y = value, color = name, group = name)) +
            geom_line() +
            theme(axis.text=element_text(size=7))
        cec.v.ferc.line

        # We use retail sales for 2000 onwards, and total sales before that
        ferc.f1 <- ferc.f1 %>% mutate(total.mwh = if_else(year >= 2000, as.numeric(total.retail.sales.mwh), total.sales.mwh))

        # Drop if total sales missing
        ferc.f1 <- ferc.f1 %>% filter(!is.na(total.mwh))
        ferc.f1 <- ferc.f1 %>% filter(total.mwh > 0)

        # Save cleaned file
        write_rds(ferc.f1, paste0(dropbox.dir, "ModifiedData/ferc.f1.v2.clean.rds"))

        # Save csv of prices
        # write_csv(ferc.f1 %>% filter(ca.iou == T & year >= 2000) %>% select(utility, year, res.price.dollars.per.kwh, bundled.res.price.dollars.per.kwh), paste0(modifieddata, "res.avg.bundled.rates.by.utility.year.for.meredith.csv"))
    }

    if (gas.prices.clean == T) {

        # 
        # Load and clean gas data - this is monthly avg prices ($/m btu)
        # and volumes from SNL
        #
        gas.prices <- read_excel(paste0(dropbox.dir, "OriginalData/gas_prices/ca_hubs_gas_prices_and_volumes_01_2010_to_07_2020.xlsx"), skip=1)

        colnames(gas.prices) <- c("dt", "norcal.border.malin.price", "norcal.border.malin.volume", "socal.border.price", "socal.border.volume", "socal.citygate.price", "socal.citygate.volume", "kern.river.station.price", "kern.river.station.volume", "pge.gate.price", "pge.gate.volume", "pge.south.price", "pge.south.volume")

        gas.prices <- gas.prices %>%
            mutate(year = year(dt), month = month(dt))
        gas.prices <- gas.prices %>%
            mutate_at(vars(matches(("price|volume"))), as.numeric)

        # Take weighted avg but exclude pge south because it's not used much and has a lot of missing values
        gas.prices <- gas.prices %>%
            mutate(
                   norcal.avg.price = (norcal.border.malin.price * norcal.border.malin.volume + pge.gate.price * pge.gate.volume) / (norcal.border.malin.volume + pge.gate.volume),
                   socal.avg.price = (socal.border.price * socal.border.volume + socal.citygate.price * socal.citygate.volume + kern.river.station.price * kern.river.station.volume) / (socal.border.volume + socal.citygate.volume + kern.river.station.volume)
            )

        gas.prices <- gas.prices %>% 
            mutate(date = date(dt))

        # Now plot socal border vs pg&e citygate
        eia.hubs.data <- gas.prices %>% 
            select(date, pge.gate.price, socal.border.price)

        eia.hubs.data <- eia.hubs.data %>% 
            pivot_longer(cols = ends_with("price"))
        eia.hubs.price <- ggplot(eia.hubs.data, aes(x=date, y=value, group=name, color=name)) +
            geom_line() +
            scale_x_date() +
            ylim(0, NA) +
            labs(x = "", y = "$/million BTU", title="Monthly avg. price for PG&E Citygate and Socal Border Hubs")
        eia.hubs.price

        # Do the weighted avg. of all northern and southern hubs
        avg.hubs.data <- gas.prices %>% 
            select(date, norcal.avg.price, socal.avg.price)

        avg.hubs.data <- avg.hubs.data %>% 
            pivot_longer(cols = ends_with("price"))
        avg.hubs.price <- ggplot(avg.hubs.data, aes(x=date, y=value, group=name, color=name)) +
            geom_line() +
            scale_x_date() +
            ylim(0, NA) +
            labs(x = "", y = "$/million BTU", title="Monthly avg. price for avg. Northern and Southern CA hubs")
        avg.hubs.price

        write_rds(gas.prices, paste0(dropbox.dir, "ModifiedData/gas.prices.clean.rds"))

    }

    if (compute.2019.rates == T) {

        # 
        # Compute 2019 rates by component using advice letters and tariff sheets
        #

        # Load system data and weight rates accordingly
        file.prefix <- "OriginalData/snl_system_load_data/"

        file.name <- "actual_load_caiso_and_planning_areas_2016_through_2019.xlsx"
        sys.load <- as.data.table(read_excel(paste0(dropbox.dir, file.prefix, file.name)))

        setnames(sys.load, names(sys.load), c("dt.p", "caiso.load",  "hour.beginning", "dt.gmt", "pge.load", "sce.load", "sdge.load"))

        # Fix dates 
        sys.load[, date := date(dt.p)]
        sys.load[, hour.beginning := hour(dt.p)]

        # Get iou load
        sys.load <- as_tibble(sys.load) %>% 
            select(date, pge.load, sce.load, sdge.load) %>% 
            filter(year(date) >= 2019) %>%
            group_by(date) %>%
            summarize(pge.load = sum(pge.load, na.rm = T),
                sce.load = sum(sce.load, na.rm = T),
                sdge.load = sum(sdge.load, na.rm = T)) %>%
            ungroup() %>%
            pivot_longer(cols = ends_with("load"), names_to = c("iou"), names_pattern = "(pge|sce|sdge).load", values_to = "load.mw")

        # Load rates data
        rates <- read_excel(paste0(dropbox.dir, "OriginalData/advice_letters/rates_data.xlsx"))

        # Turn date into date
        rates <- rates %>%
            mutate(start.date = date(start.date),
                   end.date = date(end.date))

        rates <- setDT(rates)[ , .(iou = iou, cat = cat, noncare.rate = noncare.rate, care.rate = care.rate, date = seq(start.date, end.date, by = "day")), by = 1:nrow(rates)]

        # Merge in rates
        rates <- merge(rates, sys.load)

        # SDG&E has summer pricing june - oct
        # First drop rates in periods when they don't apply
        # Then replace summer and winter categories with single category
        rates <- rates %>% 
            filter(!(grepl("winter", cat) & (month(date) %in% 6:10)) & !(grepl("summer", cat) & !(month(date) %in% 6:10))) %>%
            mutate(cat = str_replace(cat, " (summer|winter)", ""))

        # Get average rates
        rates <- rates %>%
            group_by(iou, cat) %>%
            summarize(noncare.rate = weighted.mean(noncare.rate, load.mw),
                      care.rate = weighted.mean(care.rate, load.mw)) %>% 
            ungroup()

        # We are ignoring tiers because the CIA is speicified as
        # a discount for tier 1, charge for tier 2 so remove that
        # Seems to be that revenue ignoring CIA is about right
        # Otherwise group these into categories
        # Group into major categories
        rates <- rates %>% 
            mutate(cat.major = case_when(cat %in% c("ND", "CTC", "LGC", "RS", "DWR", "NSGC", "NDC", "G") ~ "G",
                                         cat %in% c("T", "TRA") ~ "T",
                                         cat %in% c("D") ~ "D",
                                         cat %in% c("PPP", "ECRA", "PUCRF") ~ "PPP/Other")) %>%
            group_by(iou, cat.major) %>%
            summarize(noncare.rate = sum(noncare.rate),
                      care.rate = sum(care.rate)) %>%
            ungroup() %>%
            filter(!is.na(cat.major))

        # Save for Meredith
        write_xlsx(rates, paste0(dropbox.dir, "Tables/care_and_noncare_rates_by_iou_and_major_component_for_2019.xlsx"))
        write_rds(rates, paste0(dropbox.dir, "ModifiedData/care_and_noncare_rates_by_iou_and_major_component_for_2019.rds"))
    }
}

run.preliminary.data.cleaning(pv.sim = F, ferc.clean = F, gas.prices.clean = F, compute.2019.rates = F)

# 
# Analysis starts here
#

# 
# Load FERC data and make summary box/whisker plots
ferc.f1 <- read_rds(paste0(dropbox.dir, "ModifiedData/ferc.f1.v2.clean.rds"))

# Get d and t dollars per kwh
ferc.f1 <- ferc.f1 %>% mutate(t.dollars.per.kwh = (t.total * 1000) / (total.mwh * 1000),
                              d.dollars.per.kwh = (d.total * 1000) / (total.mwh * 1000),
                              g.dollars.per.kwh = (g.total * 1000) / (total.mwh * 1000),
                              admin.other.dollars.per.kwh = (admin.other.total * 1000) / (total.mwh * 1000))

# Drop if values are crazy -- if t or d is more than a dollar per kwh.
# There was just one utility reporting strange numbers here
dim(ferc.f1)
ferc.f1  <- ferc.f1 %>% filter(t.dollars.per.kwh < 1 & d.dollars.per.kwh < 1)

# Nantucket electric company 2006; United Power, Inc. 2004; Newcorp Resources Electric Coooperative Inc., 2002.

# Make categorical variable for utility
ferc.f1 <- ferc.f1 %>% mutate(utility.cat = if_else(utility %in% c("pacific gas and electric company", "southern california edison company", "san diego gas & electric company"), utility, "other"))

# Load SCE bundled sales from ferc form 1
# We use this to get bundled revenue
sce.bundled.rev <- read_excel(paste0(dropbox.dir, "OriginalData/ferc_form_1/snl_sce_bundled_res_rev_and_sales.xlsx"), sheet = "clean") %>%
    select(-bundled.res.sales.mwh)

# Load net bundled sales we obtained from SCE
sce.bundled.sales <- read_excel(paste0(dropbox.dir, "OriginalData/SCE_Annual_Domestic_kWh_Borenstein.xlsx"), range = "Domestic Bundled Sales!B2:H22") %>%
    rename(
           year = "Year",
           channel.1.kwh = "Channel 1 kWh...6",
           net.kwh = "Net kWh...7"
           ) %>%
    select(year, net.kwh) %>%
    mutate(iou = "sce")

sce.corrected.price <- inner_join(sce.bundled.rev, sce.bundled.sales) %>%
    mutate(bundled.res.price.sce.corrected = bundled.res.revenue.thousands * 1000 / net.kwh, 
           utility = "southern california edison company",
           year = as.character(year)) %>%
    select(year, utility, bundled.res.price.sce.corrected)

# Residential price, bundled
# Update ferc f1 SCE price with corrected price
ferc.f1 <- left_join(ferc.f1, sce.corrected.price) %>%
    mutate(bundled.res.price.dollars.per.kwh = if_else(utility ==  "southern california edison company", bundled.res.price.sce.corrected, bundled.res.price.dollars.per.kwh)) 

# Load cpi, adjust to 2019 dollars
cpi <- read_excel(paste0(dropbox.dir, "OriginalData/cpi/r-cpi-u-rs-allitems.xlsx"), skip = 5) %>%
    select(YEAR, AVG) %>%
    mutate(end.year.cpi = max(AVG * (YEAR == 2019), na.rm = T)) %>%
    rename(year.cpi = AVG, year = YEAR) %>%
    mutate(year = as.character(year))

ferc.f1 <- left_join(ferc.f1, cpi) %>%
    mutate(t.dollars.per.kwh = t.dollars.per.kwh * end.year.cpi / year.cpi,
           d.dollars.per.kwh = d.dollars.per.kwh * end.year.cpi / year.cpi,
           g.dollars.per.kwh = g.dollars.per.kwh * end.year.cpi / year.cpi,
           admin.other.dollars.per.kwh = admin.other.dollars.per.kwh * end.year.cpi / year.cpi,
           bundled.res.price.dollars.per.kwh = bundled.res.price.dollars.per.kwh * end.year.cpi / year.cpi)

p_load(Hmisc)
# Distribution $/kWh by year for major U.S. utilities
d.per.kwh.scatter.data <- ferc.f1 %>%
    select(utility, ca.iou, year, d.dollars.per.kwh, total.mwh) %>%
    group_by(year) %>%
    dplyr::summarize(
                     p05 = wtd.quantile(d.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.05, normwt = T),
                     p25 = wtd.quantile(d.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.25),
                     p50 = wtd.quantile(d.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.50),
                     p75 = wtd.quantile(d.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.75),
                     p95 = wtd.quantile(d.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.95, normwt = T),
                     pge.dot = max(d.dollars.per.kwh * (utility == "pacific gas and electric company"), na.rm = T),
                     sce.dot = max(d.dollars.per.kwh * (utility == "southern california edison company"), na.rm = T),
                     sdge.dot = max(d.dollars.per.kwh * (utility == "san diego gas & electric company"), na.rm = T)
                     )

# Make graph
d.per.kwh.scatter <- ggplot(ferc.f1) + 
    geom_boxplot(data = d.per.kwh.scatter.data, aes(x = as.factor(year), ymin = p05, lower = p25, middle = p50, upper = p75, ymax = p95), stat = "identity") +
    geom_point(data = ferc.f1 %>% filter(ca.iou), aes(x = as.factor(year), y = d.dollars.per.kwh, color = factor(utility.cat, levels = c("pacific gas and electric company", "southern california edison company", "san diego gas & electric company"), labels = c("PG&E", "SCE", "SDG&E"))), position = position_dodge(width = 0.25), size = 6) +
    labs(x = "Year", y = "$/kWh (real 2019$)", color = "IOU") +
    scale_x_discrete(breaks = 1988:2019, labels = c(88:99, "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", 10:19)) +
    theme_minimal() +
    theme(axis.text.x=element_text(size=12),
          axis.text.y=element_text(size=15),
          axis.title=element_text(size=17),
          legend.text=element_text(size=17),
          legend.title=element_text(size=18),
          plot.title=element_text(size=20))

d.per.kwh.scatter
aspect.ratio <- .8
name <- "Graphs/d_per_kwh_scatter_weighted.pdf"
ggsave(paste0(dropbox.dir, name), device = "pdf", height = 7 * aspect.ratio , width = 10)

# 
# Note SDG&E is high for T in 2012 - this is driven by
# large capital addition, an order of magnitude above
# previous year
#

# Transmission $/kWh by year for major U.S. utilities
t.per.kwh.scatter.data <- ferc.f1 %>%
    select(utility, ca.iou, year, t.dollars.per.kwh, total.mwh) %>%
    group_by(year) %>%
    dplyr::summarize(
                     p05 = wtd.quantile(t.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.05, normwt = T),
                     p25 = wtd.quantile(t.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.25),
                     p50 = wtd.quantile(t.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.50),
                     p75 = wtd.quantile(t.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.75),
                     p95 = wtd.quantile(t.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.95, normwt = T),
                     pge.dot = max(t.dollars.per.kwh * (utility == "pacific gas and electric company"), na.rm = T),
                     sce.dot = max(t.dollars.per.kwh * (utility == "southern california edison company"), na.rm = T),
                     sdge.dot = max(t.dollars.per.kwh * (utility == "san diego gas & electric company"), na.rm = T)
                     )

# Make graph
t.per.kwh.scatter <- ggplot(ferc.f1) + 
    geom_boxplot(data = t.per.kwh.scatter.data, aes(x = as.factor(year), ymin = p05, lower = p25, middle = p50, upper = p75, ymax = p95), stat = "identity") +
    geom_point(data = ferc.f1 %>% filter(ca.iou), aes(x = as.factor(year), y = t.dollars.per.kwh, color = factor(utility.cat, levels = c("pacific gas and electric company", "southern california edison company", "san diego gas & electric company"), labels = c("PG&E", "SCE", "SDG&E"))), position = position_dodge(width = 0.25), size = 6) +
    labs(x = "Year", y = "$/kWh (real 2019$)", color = "IOU") +
    scale_x_discrete(breaks = 1988:2019, labels = c(88:99, "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", 10:19)) +
    theme_minimal() +
    theme(axis.text.x=element_text(size=12),
          axis.text.y=element_text(size=15),
          axis.title=element_text(size=17),
          legend.text=element_text(size=17),
          legend.title=element_text(size=18),
          plot.title=element_text(size=20))

t.per.kwh.scatter
aspect.ratio <- .8
name <- "Graphs/t_per_kwh_scatter_weighted.pdf"
ggsave(paste0(dropbox.dir, name), device = "pdf", height = 7 * aspect.ratio , width = 10)

# Generation $/kWh by year for major U.S. utilities
g.per.kwh.scatter.data <- ferc.f1 %>%
    select(utility, ca.iou, year, g.dollars.per.kwh, total.mwh) %>%
    group_by(year) %>%
    dplyr::summarize(
                     p05 = wtd.quantile(g.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.05, normwt = T),
                     p25 = wtd.quantile(g.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.25),
                     p50 = wtd.quantile(g.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.50),
                     p75 = wtd.quantile(g.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.75),
                     p95 = wtd.quantile(g.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.95, normwt = T),
                     pge.dot = max(g.dollars.per.kwh * (utility == "pacific gas and electric company"), na.rm = T),
                     sce.dot = max(g.dollars.per.kwh * (utility == "southern california edison company"), na.rm = T),
                     sdge.dot = max(g.dollars.per.kwh * (utility == "san diego gas & electric company"), na.rm = T)
                     )

# Make graph
g.per.kwh.scatter <- ggplot(ferc.f1) + 
    geom_boxplot(data = g.per.kwh.scatter.data, aes(x = as.factor(year), ymin = p05, lower = p25, middle = p50, upper = p75, ymax = p95), stat = "identity") +
    geom_point(data = ferc.f1 %>% filter(ca.iou), aes(x = as.factor(year), y = g.dollars.per.kwh, color = factor(utility.cat, levels = c("pacific gas and electric company", "southern california edison company", "san diego gas & electric company"), labels = c("PG&E", "SCE", "SDG&E"))), position = position_dodge(width = 0.25), size = 6) +
    labs(x = "Year", y = "$/kWh (real 2019$)", color = "IOU") +
    scale_x_discrete(breaks = 1988:2019, labels = c(88:99, "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", 10:19)) +
    theme_minimal() +
    theme(axis.text.x=element_text(size=12),
          axis.text.y=element_text(size=15),
          axis.title=element_text(size=17),
          legend.text=element_text(size=17),
          legend.title=element_text(size=18),
          plot.title=element_text(size=20))

g.per.kwh.scatter
aspect.ratio <- .8
name <- "Graphs/g_per_kwh_scatter_weighted.pdf"
ggsave(paste0(dropbox.dir, name), device = "pdf", height = 7 * aspect.ratio , width = 10)

# Administrative and other expenses
admin.other.per.kwh.scatter.data <- ferc.f1 %>%
    select(utility, ca.iou, year, admin.other.dollars.per.kwh, total.mwh) %>%
    group_by(year) %>%
    dplyr::summarize(
                     p05 = wtd.quantile(admin.other.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.05, normwt = T),
                     p25 = wtd.quantile(admin.other.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.25),
                     p50 = wtd.quantile(admin.other.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.50),
                     p75 = wtd.quantile(admin.other.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.75),
                     p95 = wtd.quantile(admin.other.dollars.per.kwh, w = as.numeric(total.mwh), probs = 0.95, normwt = T),
                     pge.dot = max(admin.other.dollars.per.kwh * (utility == "pacific gas and electric company"), na.rm = T),
                     sce.dot = max(admin.other.dollars.per.kwh * (utility == "southern california edison company"), na.rm = T),
                     sdge.dot = max(admin.other.dollars.per.kwh * (utility == "san diego gas & electric company"), na.rm = T)
                     )

# Make graph
admin.other.per.kwh.scatter <- ggplot(ferc.f1) + 
    geom_boxplot(data = admin.other.per.kwh.scatter.data, aes(x = as.factor(year), ymin = p05, lower = p25, middle = p50, upper = p75, ymax = p95), stat = "identity") +
    geom_point(data = ferc.f1 %>% filter(ca.iou), aes(x = as.factor(year), y = admin.other.dollars.per.kwh, color = factor(utility.cat, levels = c("pacific gas and electric company", "southern california edison company", "san diego gas & electric company"), labels = c("PG&E", "SCE", "SDG&E"))), position = position_dodge(width = 0.25), size = 6) +
    labs(x = "Year", y = "$/kWh (real 2019$)", color = "IOU") +
    scale_x_discrete(breaks = 1988:2019, labels = c(88:99, "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", 10:19)) +
    theme_minimal() +
    theme(axis.text.x=element_text(size=12),
          axis.text.y=element_text(size=15),
          axis.title=element_text(size=17),
          legend.text=element_text(size=17),
          legend.title=element_text(size=18),
          plot.title=element_text(size=20))

admin.other.per.kwh.scatter
aspect.ratio <- .8
name <- "Graphs/admin_other_per_kwh_scatter_weighted.pdf"
ggsave(paste0(dropbox.dir, name), device = "pdf", height = 7 * aspect.ratio , width = 10)

bundled.res.price.scatter.data <- ferc.f1 %>%
    filter(year >= 2000) %>%
    select(utility, ca.iou, year, bundled.res.price.dollars.per.kwh, total.retail.sales.mwh) %>%
    group_by(year) %>%
    dplyr::summarize(
              p05 = wtd.quantile(bundled.res.price.dollars.per.kwh, w = as.numeric(total.retail.sales.mwh), probs = 0.05, normwt = T),
              p25 = wtd.quantile(bundled.res.price.dollars.per.kwh, w = as.numeric(total.retail.sales.mwh), probs = 0.25),
              p50 = wtd.quantile(bundled.res.price.dollars.per.kwh, w = as.numeric(total.retail.sales.mwh), probs = 0.50),
              p75 = wtd.quantile(bundled.res.price.dollars.per.kwh, w = as.numeric(total.retail.sales.mwh), probs = 0.75),
              p95 = wtd.quantile(bundled.res.price.dollars.per.kwh, w = as.numeric(total.retail.sales.mwh), probs = 0.95, normwt = T),
              pge.dot = max(bundled.res.price.dollars.per.kwh * (utility == "pacific gas and electric company"), na.rm = T),
              sce.dot = max(bundled.res.price.dollars.per.kwh * (utility == "southern california edison company"), na.rm = T),
              sdge.dot = max(bundled.res.price.dollars.per.kwh * (utility == "san diego gas & electric company"), na.rm = T))


bundled.res.price.scatter <- ggplot(ferc.f1 %>% filter(year >= 2000)) + 
    geom_boxplot(data = bundled.res.price.scatter.data, aes(x = as.factor(year), ymin = p05, lower = p25, middle = p50, upper = p75, ymax = p95), stat = "identity") +
    geom_point(data = ferc.f1 %>% filter(ca.iou & year >= 2000), aes(x = as.factor(year), y = bundled.res.price.dollars.per.kwh, color = factor(utility.cat, levels = c("pacific gas and electric company", "southern california edison company", "san diego gas & electric company"), labels = c("PG&E", "SCE", "SDG&E"))), position = position_dodge(width = 0.25), size = 6) +
    labs(x = "Year", y = "$/kWh (real 2019$)", color = "IOU") +
    scale_x_discrete(breaks = 2000:2019, labels = c("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", 10:19)) +
    theme_minimal() +
    theme(axis.text.x=element_text(size=14),
          axis.text.y=element_text(size=15),
          axis.title=element_text(size=17),
          legend.text=element_text(size=17),
          legend.title=element_text(size=18),
          plot.title=element_text(size=20))

bundled.res.price.scatter
aspect.ratio <- .8
name <- "Graphs/bundled_res_dollars_per_kwh_scatter_weighted.pdf"
ggsave(paste0(dropbox.dir, name), device = "pdf", height = 7 * aspect.ratio , width = 10)

p_unload(Hmisc)


# 
# Load CPI
#
cpi <- read_excel(paste0(dropbox.dir, "OriginalData/cpi/r-cpi-u-rs-allitems.xlsx"), skip = 5) %>%
    select(YEAR, AVG) %>%
    mutate(end.year.cpi = max(AVG * (YEAR == 2019), na.rm = T)) %>%
    rename(year.cpi = AVG, year = YEAR)

# 
# Get forecast based on 5-year avg of ex-post load
#
# Read system load.
# Believe I had to do this in chunks because of SNL 
file.prefix <- "OriginalData/snl_system_load_data/"

file.name <- "actual_load_caiso_and_planning_areas_2010_through_2015.xlsx"
sys.load.2010.to.2015 <- as.data.table(read_excel(paste0(dropbox.dir, file.prefix, file.name)))

file.name <- "actual_load_caiso_and_planning_areas_2016_through_2019.xlsx"
sys.load.2016.to.2019 <- as.data.table(read_excel(paste0(dropbox.dir, file.prefix, file.name)))

file.name <- "actual_load_caiso_2005_through_2009.xlsx"
sys.load.2005.to.2009 <-  as.data.table(read_excel(paste0(dropbox.dir, file.prefix, file.name)))

sys.load <- bind_rows(sys.load.2005.to.2009, sys.load.2010.to.2015, sys.load.2016.to.2019)

setnames(sys.load, names(sys.load), c("dt.p", "caiso.load",  "hour.beginning", "dt.gmt", "pge.load", "sce.load", "sdge.load"))

# Fix dates 
sys.load[, date := date(dt.p)]
sys.load[, hour.beginning := hour(dt.p)]

# Get iou load. We use this later for marginal energy costs
sys.load.by.iou <- as.tibble(sys.load) %>% 
    select(dt.p, hour.beginning, date, pge.load, sce.load, sdge.load) %>% 
    filter(year(date) >= 2010)

sys.load <- as.tibble(sys.load) %>% 
    select(dt.p, hour.beginning, date, caiso.load)

# Get days of week, holidays, months
sys.load <- sys.load %>% 
    mutate(
           dow = as.character(wday(date)), 
           month = as.character(month(date)), 
           hour.beginning = as.character(hour.beginning), 
           month.x.dow = paste0(month, "x", dow), 
           month.x.hour.beginning = paste0(month, "x", hour.beginning)
           )

# Make holidays variable
p_load(timeDate, chron)
hlist <- c("USNewYearsDay", "USInaugurationDay", "USMLKingsBirthday", "USLincolnsBirthday", "USWashingtonsBirthday", "USMemorialDay", "USIndependenceDay", "USLaborDay", "USColumbusDay", "USElectionDay", "USVeteransDay", "USThanksgivingDay", "USChristmasDay", "USCPulaskisBirthday", "USGoodFriday")

myholidays  <- dates(as.character(holiday(2005:2019,hlist)),format="Y-M-D")

sys.load <- sys.load %>% 
    mutate(holiday = is.holiday(date, myholidays), year = year(date))

p_load(fixest)

# Takes a one year tibble as argument, for which we want predicted values
# Returns tibble with new predicted values
get.predicted.load = function(predict.dat) {

    pred.year <- predict.dat$year[1]

    dat.train <- sys.load %>% filter(year >= pred.year - 5 & year < pred.year)

    # Month, hour, dow fe
    model <- feols(caiso.load ~ holiday | hour.beginning + dow + month, data = dat.train)
    predict.dat <- predict.dat %>% mutate(caiso.load.pred = predict(model, newdata = predict.dat))

    # Month x hour, dow fe
    model <- feols(caiso.load ~ holiday | month.x.hour.beginning + dow, data = dat.train)
    predict.dat <- predict.dat %>% mutate(caiso.load.pred.month.x.hour = predict(model, newdata = predict.dat))

    return(predict.dat)
}

# Run above function to get a rolling prediction
sys.load.predictions <- sys.load %>% 
    filter(year >= 2010) %>% 
    split(.$year) %>% 
    map_df(get.predicted.load)

# load duration curve type plot of predictions
# This is the specification we ultimately went with
ldc.pred.1.dat <- sys.load.predictions %>% 
    arrange(year, -caiso.load.pred.month.x.hour) %>% 
    group_by(year) %>% 
    mutate(rank.hour = row_number()) %>% 
    rename(value = caiso.load.pred.month.x.hour) %>% 
    mutate(type = "Predicted, month x hour, dow FE")

# This is more parsimonious
ldc.pred.2.dat <- sys.load.predictions %>% 
    arrange(year, -caiso.load.pred) %>% 
    group_by(year) %>% 
    mutate(rank.hour = row_number()) %>% 
    rename(value = caiso.load.pred) %>% 
    mutate(type = "Predicted, month, hour, dow FE")

ldc.actual.dat <- sys.load.predictions %>% 
    arrange(year, -caiso.load) %>% 
    group_by(year) %>% 
    mutate(rank.hour = row_number()) %>% 
    rename(value = caiso.load) %>% 
    mutate(type = "Actual")

ldc.dat <- bind_rows(ldc.pred.1.dat, ldc.actual.dat) %>% 
    select(rank.hour, value, type, year)

ldc <- ggplot(ldc.dat, aes(x=rank.hour, y=value, group = type, color = type)) +
    geom_line() +
    facet_wrap(~year) +
    labs(x = "Hour rank", y = "MW", title="Load duration curve for CAISO actual, and predicted system load values")
ldc
aspect.ratio <- .6
name <- "Graphs/caiso.ldc.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

ldc.2500 <- ggplot(ldc.dat %>% filter(rank.hour <= 2500), aes(x=rank.hour, y=value, group = type, color = type)) +
    geom_line() +
    facet_wrap(~year) +
    labs(x = "Hour rank", y = "MW", title="Load duration curve for CAISO actual, and predicted system load top 2,500 hours")
ldc.2500
aspect.ratio <- .6
name <- "Graphs/caiso.ldc.2500.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# How often does top 250 pred miss top 10 actual
ldc.2500.pred.v.actual.dat <- ldc.pred.1.dat %>%
    select(rank.hour, year, caiso.load, value)
ldc.2500.pred.v.actual.dat <- ldc.2500.pred.v.actual.dat %>% 
    arrange(year, -caiso.load) %>% 
    group_by(year) %>% 
    mutate(rank.hour.actual = row_number()) 
ldc.2500.pred.v.actual.dat %>% filter(rank.hour.actual <= 10)

#
# Generation capacity avoided (marginal) cost estimation
#

# First grab kw-year numbers from E3 avoided cost
# calculators (OriginalData\avoided_cost_calculator) and interpolate.
# Comes from 'Market Dynamics' tab in ACC
# Grab the values unadjusted for losses. We don't grab E3 loss factors but use our own.

# NB both 2017 and 2016 acc give value for 2016, and we
# use the forecast from these calculators for 2017
#
# Drop 2020 when we do interpolation for the gap
cap.values <- tibble(
       year = c(2010, 2016, 2017, 2018, 2019, 2020),
       capacity.dollars.per.kw.year = c(52.68, 107, 102.59, 98.40, 91.62, 196.58)
       )

# Draw a line
line <- lm(data = cap.values %>% filter(year != 2020), capacity.dollars.per.kw.year ~ year)

# Predict
interpolated.values <- tibble(
                              year = 2010:2019
                              )
interpolated.values <- interpolated.values %>% 
    mutate(interpolated.capacity.dollars.per.kw.year = predict(line, newdata = interpolated.values))

# Merge and replace missing with predictions
interpolated.values <- full_join(interpolated.values, cap.values)
interpolated.values <- interpolated.values %>% 
    mutate(capacity.dollars.per.kw.year = if_else(is.na(capacity.dollars.per.kw.year), interpolated.capacity.dollars.per.kw.year, capacity.dollars.per.kw.year)) %>% 
    select(-interpolated.capacity.dollars.per.kw.year)

# Function to make allocation factor given 8760 values
make.allocation.factor.threshold <- function(values.tib, n.top.vals = 500) {

    values.8760 <- values.tib %>% select(load.mw)

    rank <- frank(-values.8760, ties.method = "first", na.last = T)

    threshold <- as.numeric(values.8760[which(rank == n.top.vals + 1),])

    load.minus.threshold  = values.8760 - threshold

    sum <- sum(load.minus.threshold[which(rank <= n.top.vals),])

    allocation.factor = (load.minus.threshold / sum) * as.integer(rank <= n.top.vals)
    return(bind_cols(values.tib %>% select(dt.p), allocation.factor))
}
    
# # First dow, month, hour, fes
# sys.load.predictions <- sys.load.predictions %>% mutate(load.mw = caiso.load.pred)
# allocation.factors.pred <- sys.load.predictions %>% group_by(year) %>% group_modify(~ make.allocation.factor(.x)) %>% rename(allocation.factor.pred = load.mw)
#
# Now month x hour, dow fes
sys.load.predictions <- sys.load.predictions %>% 
    mutate(load.mw = caiso.load.pred.month.x.hour)
allocation.factors <- sys.load.predictions %>% 
    group_by(year) %>% 
    group_modify(~ make.allocation.factor.threshold(.x)) %>% 
    rename(allocation.factor.pred.month.x.hour = load.mw)

# Check - looks good
# allocation.factors %>% summarize(alloc.tot = sum(allocation.factor.pred, na.rm=T))
allocation.factors %>% summarize(alloc.tot = sum(allocation.factor.pred.month.x.hour, na.rm=T))

gen.cap <- inner_join(sys.load, allocation.factors)

# Merge in interpolated e3 values and also create the RA $30/kW-year value
gen.cap <- left_join(gen.cap, interpolated.values) %>% mutate(ra.dollars.per.kw.year = 30)

# Now make long, adding iou
gen.cap <- bind_rows(gen.cap %>% mutate(iou = "pge"),
          gen.cap %>% mutate(iou = "sce"),
          gen.cap %>% mutate(iou = "sdge"))

# Get gen cap vars
ra.factor <- 1.15 # This is resource adequacy factor that E3 uses
gen.cap <- gen.cap %>% 
    mutate(
           e3.gen.cap.dollars.per.mw = capacity.dollars.per.kw.year * allocation.factor.pred.month.x.hour * ra.factor * 1000,
           ra.gen.cap.dollars.per.mw = ra.dollars.per.kw.year * allocation.factor.pred.month.x.hour * ra.factor * 1000
           )

# Avg monthly capacity value
monthly.plot.dat <- gen.cap %>% 
    mutate(month = month(date))

monthly.plot.dat <- monthly.plot.dat %>% 
    group_by(year, iou, month) %>% 
    summarize(avg.e3.gen.cap = mean(e3.gen.cap.dollars.per.mw, na.rm=T),
                                                              avg.ra.gen.cap = mean(ra.gen.cap.dollars.per.mw, na.rm=T))

ggplot(data = monthly.plot.dat, aes(fill=iou, y=avg.e3.gen.cap, x=as.numeric(month))) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = 1:12) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Month", y = "Avg. monthly value of generation capacity ($/MWh)", title = "Avg. monthly generation capacity value, E3 method (option 1), dow, month.x.hour FEs predicted system load")

aspect.ratio <- .6
name <- "Graphs/avg.monthly.generation.capacity.value.e3.method.month.x.hour.fes.pred.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

ggplot(data = monthly.plot.dat, aes(fill=iou, y=avg.ra.gen.cap, x=as.numeric(month))) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = 1:12) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Month", y = "Avg. monthly value of generation capacity ($/MWh)",title = "Avg. monthly generation capacity value, RA method (option 2), dow, month.x.hour FEs predicted system load")

aspect.ratio <- .6
name <- "Graphs/avg.monthly.generation.capacity.value.ra.method.fes.pred.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# Avg hourly value
hourly.plot.dat <- gen.cap %>% 
    group_by(year, iou, hour.beginning) %>% 
    summarize(
              avg.e3.gen.cap = mean(e3.gen.cap.dollars.per.mw, na.rm=T),
              avg.ra.gen.cap = mean(ra.gen.cap.dollars.per.mw, na.rm=T)
              )

ggplot(data = hourly.plot.dat, aes(fill=iou, y=avg.e3.gen.cap, x=as.numeric(hour.beginning))) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = seq(from = 0, to = 23, by = 3)) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Hour", y = "Avg. monthly value of generation capacity ($/MWh)", title = "Avg. hourly generation capacity value, E3 method (option 1), hour, dow, month FEs predicted system load")

aspect.ratio <- .6
name <- "Graphs/avg.hourly.generation.capacity.value.e3.method.fes.pred.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

ggplot(data = hourly.plot.dat, aes(fill=iou, y=avg.ra.gen.cap, x=as.numeric(hour.beginning))) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = seq(from = 0, to = 23, by = 3)) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Hour", y = "Avg. monthly value of generation capacity ($/MWh)", title = "Avg. hourly generation capacity value, RA method (option 2), hour, dow, month FEs predicted system load")

aspect.ratio <- .6
name <- "Graphs/avg.hourly.generation.capacity.value.ra.method.fes.pred.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# load duration curve type plot
ldc.dat <- gen.cap %>% 
    arrange(iou, year, -ra.gen.cap.dollars.per.mw) %>% 
    filter(ra.gen.cap.dollars.per.mw > 0)

ldc.dat <- ldc.dat %>% 
    group_by(iou, year) %>% 
    mutate(rank.hour = row_number())

ggplot(ldc.dat, aes(x=rank.hour, y=ra.gen.cap.dollars.per.mw, group=iou, color=iou)) +
    geom_line() +
    facet_wrap(~year + iou)
    labs(x = "Hour rank", y = "$/million BTU", title="Monthly avg. price for PG&E Citygate and Socal Border Hubs")

# 
# T and D capacity
#

# Load pg&e data. These are from avoided cost calculators, collected from GRCs
pge.capacity.values <- read_excel(paste0(dropbox.dir, "OriginalData/t_and_d_capacity_values/pge_t_and_d_capacity_values.xlsx"))

# Rename 3A and 3B to just 3, average using pcaf kw like division
# Some years PG&E has 3, some it splits into 3A and 3B, so we will just avg them if there are multiple
pge.capacity.values <- pge.capacity.values %>% 
    mutate(cz = if_else(grepl("3[AB]", cz), "3", cz))

# Get climate zone values from weighted avg of division values
pge.cz.capacity.values <- pge.capacity.values %>%
    group_by(iou, base.year, calculator.year, escalation.rate, cz, transmission.loss.factor, distribution.loss.factor) %>%
    summarize(
              transmission.dollars.per.kw.year = weighted.mean(transmission.dollars.per.pcaf.kw.year * mtr.to.trans.claf, pcaf.kw),
              primary.dollars.per.kw.year = weighted.mean(primary.dollars.per.pcaf.kw.year * mtr.to.primary.claf, pcaf.kw),
              secondary.dollars.per.kw.year = weighted.mean(secondary.dollars.per.pcaf.kw.year * mtr.to.secondary.claf, pcaf.kw)
              ) %>%
    ungroup()

# Fill in missing years. We have all the values we need
# since we have GRC values from 2011-2017
# calculator.year is the year of the avoided cost calcultor
# new.year is the year of t and d value we filling in
get.values.for.missing.year <- function(calc.year, new.year) {
    ret <- pge.cz.capacity.values %>% 
        filter(calculator.year == calc.year) %>% 
        mutate(calculator.year = new.year)
    return(ret)
}

pge.cz.capacity.values <- bind_rows(pge.cz.capacity.values, 
          get.values.for.missing.year(2010, 2011), # Note that here we are using 2010 calculator values for 2011. The 2010 calculator uses 2011 GRC values
          get.values.for.missing.year(2010, 2012),
          get.values.for.missing.year(2010, 2013),
          get.values.for.missing.year(2010, 2013),
          get.values.for.missing.year(2016, 2014), # 2016 calcultor uses 2014 grc values. So here we are filling in 2014 with 2014 GRC values
          get.values.for.missing.year(2016, 2015))

# Get distribution dollars per kw-year as sum of primary
# and secondary, multiplied by relevant escalation rate
# This calculation was erroneous in the avoided cost calcultor itself for many years
pge.cz.capacity.values <- pge.cz.capacity.values %>%
    mutate(
           distribution.dollars.per.kw.year = (primary.dollars.per.kw.year + secondary.dollars.per.kw.year) * ((1 + escalation.rate)^(calculator.year - base.year)),
           transmission.dollars.per.kw.year = transmission.dollars.per.kw.year * ((1 + escalation.rate)^(calculator.year - base.year)))

# Clean up
pge.cz.capacity.values <- pge.cz.capacity.values %>%
    rename(year = calculator.year) %>%
    select(iou, year, cz, transmission.dollars.per.kw.year, distribution.dollars.per.kw.year)

# This is from the old billing data in another project
# We drop 2016 since it's incomplete and just use 2015 for
# 2016 onwards
pge.cz.weights <- readRDS(paste0(dropbox.dir, "ModifiedData/", "total.annual.res.consumption.by.cz.rds")) %>%
    filter(!is.na(cz)) %>%
    mutate(cz = as.character(cz))
add.row <- function(new.year) {
    result <- pge.cz.weights %>%
        filter(year == 2015) %>%
        mutate(year = new.year)
    return(result)
}
pge.cz.weights <- bind_rows(pge.cz.weights %>% filter(year < 2016),
                            add.row(2016),
                            add.row(2017),
                            add.row(2018),
                            add.row(2019))

# Merge with weights
pge.cz.capacity.values <- inner_join(pge.cz.capacity.values, pge.cz.weights)

# Collapse, taking weighted avg across climate zones
pge.capacity.values <- pge.cz.capacity.values %>%
    group_by(iou, year) %>%
    summarize(transmission.dollars.per.kw.year = weighted.mean(transmission.dollars.per.kw.year, tot.kwh),
              distribution.dollars.per.kw.year = weighted.mean(distribution.dollars.per.kw.year, tot.kwh)) %>%
    ungroup()

# Output file for Severin. These are not actually final because we subsequently decided to average them to smooth out lumpiness of GRC data
write_xlsx(pge.capacity.values, paste0(dropbox.dir, "OriginalData/t_and_d_capacity_values/pge.final.for.severin.xlsx"))

# Load SDG&E and SCE capacity values
# These are not by climate zone
# Also from avoided cost calculators
sce.sdge.capacity.values <- read_excel(paste0(dropbox.dir, "OriginalData/t_and_d_capacity_values/sce_sdge_t_and_d_capacity_values.xlsx"))

# Fill in missing years. We have all the values we need
# since we have GRC values from 2011-2017
get.values.for.missing.year <- function(iou.select, calc.year, new.year) {
    ret <- sce.sdge.capacity.values %>%
        filter(iou == iou.select, calculator.year == calc.year) %>% 
        mutate(calculator.year = new.year)
    return(ret)
}

# This code sucks but ast least it is quite clear which year of calculator values is being used for which year of t&d values (or vice versa)
# One issue is we don't know the GRC year the 2010 avoided cost calculator values are taken from
sce.sdge.capacity.values <- bind_rows(sce.sdge.capacity.values,
                                      get.values.for.missing.year("sce", 2010, 2011),
                                      get.values.for.missing.year("sce", 2010, 2012),
                                      get.values.for.missing.year("sce", 2010, 2013),
                                      get.values.for.missing.year("sce", 2010, 2014),
                                      get.values.for.missing.year("sce", 2016, 2015),
                                      get.values.for.missing.year("sdge", 2010, 2011),
                                      get.values.for.missing.year("sdge", 2010, 2012),
                                      get.values.for.missing.year("sdge", 2010, 2013),
                                      get.values.for.missing.year("sdge", 2010, 2014),
                                      get.values.for.missing.year("sdge", 2010, 2015))

# Get T and D values scaled up by appropriate escalation rate relative to base year
# Eventually we will apply our own loss factors
sce.sdge.capacity.values <- sce.sdge.capacity.values %>%
    mutate(
           transmission.dollars.per.kw.year = transmission.dollars.per.kw.year * ((1 + escalation.rate)^(calculator.year - base.year)),
           distribution.dollars.per.kw.year = distribution.dollars.per.kw.year * ((1 + escalation.rate)^(calculator.year - base.year))
           ) %>%
    select(-base.year, -transmission.loss.factor, -distribution.loss.factor, -escalation.rate) %>%
    rename(year = calculator.year)

# Append
t.and.d.capacity.values <- bind_rows(pge.capacity.values, sce.sdge.capacity.values)

# Output all IOU values. This is not final as we average them
write_xlsx(t.and.d.capacity.values, paste0(dropbox.dir, "OriginalData/t_and_d_capacity_values/all_ious_final_for_Meredith.xlsx"))

# Just take average for the time perioid by iou
# This is to smooth out lumpiness of GRC values
t.and.d.capacity.values <- t.and.d.capacity.values %>%
    group_by(iou) %>%
    mutate(
           transmission.dollars.per.kw.year = mean(transmission.dollars.per.kw.year),
           distribution.dollars.per.kw.year = mean(distribution.dollars.per.kw.year)
           )

t.n.d.cap <- inner_join(t.and.d.capacity.values, allocation.factors)

t.n.d.cap <- t.n.d.cap %>% 
    mutate(
           t.dollars.per.mw = transmission.dollars.per.kw.year * allocation.factor.pred.month.x.hour * 1000,
           d.dollars.per.mw = distribution.dollars.per.kw.year * allocation.factor.pred.month.x.hour * 1000,
           date = date(dt.p),
           hour.beginning = hour(dt.p)) %>%
    select(-allocation.factor.pred.month.x.hour, -transmission.dollars.per.kw.year, - distribution.dollars.per.kw.year)

# OK
# test <- t.n.d.cap %>% group_by(iou, year) %>% summarize(t.dollars.per.kw.year = sum(t.dollars.per.mw) / 1000,
#                                                         d.dollars.per.kw.year = sum(d.dollars.per.mw) / 1000)
#

# Avg monthly value
monthly.plot.dat <- t.n.d.cap %>% 
    mutate(month = month(dt.p))

monthly.plot.dat <- monthly.plot.dat %>% 
    group_by(year, iou, month) %>% 
    summarize(avg.t.cap = mean(t.dollars.per.mw, na.rm=T), 
              avg.d.cap = mean(d.dollars.per.mw, na.rm=T))

ggplot(data = monthly.plot.dat, aes(fill=iou, y=avg.t.cap, x=as.numeric(month))) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = 1:12) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Month", y = "Avg. monthly value of transmission capacity ($/MWh)", title = "Avg. monthly transmission capacity value")

aspect.ratio <- .6
name <- "Graphs/avg.monthly.transmission.capacity.value.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

ggplot(data = monthly.plot.dat, aes(fill=iou, y=avg.d.cap, x=as.numeric(month))) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = 1:12) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Month", y = "Avg. monthly value of distribution capacity ($/MWh)", title = "Avg. monthly distribution capacity value")


aspect.ratio <- .6
name <- "Graphs/avg.monthly.distribution.capacity.value.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# Avg hourly value
hourly.plot.dat <- t.n.d.cap %>% 
    group_by(year, iou, hour.beginning) %>% 
    summarize(avg.t.cap = mean(t.dollars.per.mw, na.rm=T),
              avg.d.cap = mean(d.dollars.per.mw, na.rm=T))

ggplot(data = hourly.plot.dat, aes(fill=iou, y=avg.t.cap, x=as.numeric(hour.beginning))) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = seq(from = 0, to = 23, by = 3)) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Hour", y = "Avg. hourly value of transmission capacity ($/MWh)", title = "Avg. hourly transmission capacity value")

aspect.ratio <- .6
name <- "Graphs/avg.hourly.transmission.capacity.value.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

ggplot(data = hourly.plot.dat, aes(fill=iou, y=avg.d.cap, x=as.numeric(hour.beginning))) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = seq(from = 0, to = 23, by = 3)) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Hour", y = "Avg. hourly value of distribution capacity ($/MWh)", title = "Avg. hourly distribution capacity value")

aspect.ratio <- .6
name <- "Graphs/avg.hourly.distribution.capacity.value.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# 
# Marginal energy costs
#
# Uses 
# 1. lmps for dlaps from snl
# 2. gas prices
# 3. I can find variable OM costs for natrual gas plants from e3 documentation
# 4. emissions factor - 0.05307
# 4. alpha_it to adjust for line losses from pvsmc2
# 5. ghg permit prices - CA quarterly prices
#

# Get alpha_it
# load by utility - we loaded and cleaned above for generation capacity
sys.load.by.iou <- sys.load.by.iou %>% 
    rename(pge = pge.load,
                                sce = sce.load,
                                sdge = sdge.load)
sys.load.by.iou <- pivot_longer(sys.load.by.iou, cols = c(pge, sce, sdge), names_to = "iou", values_to = "load.mw")

# These come from Severin's PvSMC1 paper
# We take the avg over years within IOU
avg.res.losses <- read_excel(paste0(dropbox.dir, "OriginalData/losses/avg_res_losses_from_PvSMC1.xlsx")) %>% 
    group_by(iou) %>% 
    summarize(avg.res.losses = mean(avg.res.losses))

# Here we follow method from appendix (p52 onwards) of PvSMC1 for deriving marginal losses
alphas.dat <- sys.load.by.iou %>% 
    group_by(iou) %>% 
    summarize(
              sum.q = sum(load.mw * 1000, na.rm=T), 
              sum.q.sq = sum((load.mw * 1000)^2, na.rm=T)
    )
alphas.dat <- inner_join(alphas.dat, avg.res.losses)
alphas.dat <- alphas.dat %>% 
    mutate(alpha_i = (1 - 0.25) * avg.res.losses * (sum.q / sum.q.sq))

marginal.losses <- left_join(sys.load.by.iou, alphas.dat)
marginal.losses <- marginal.losses %>% 
    mutate(dl.dq = 2 * load.mw * 1000 * alpha_i)

# Output table of marginal losses
marginal.losses.table <- marginal.losses %>% 
    mutate(year = year(date)) %>% 
    group_by(year) %>% 
    summarize(
              min = min(dl.dq, na.rm=T),
              p25 = quantile(dl.dq, probs = c(0.25), na.rm=T),
              mean = mean(dl.dq, na.rm=T),
              median = quantile(dl.dq, probs = c(0.5), na.rm=T),
              p75 = quantile(dl.dq, probs = c(0.75), na.rm=T),
              max = max(dl.dq, na.rm=T)
    )
write_xlsx(marginal.losses.table, paste0(dropbox.dir, "ModifiedData/marginal.losses.table.xlsx"))

# Load lmps
lmps <- read_excel(paste0(dropbox.dir, "OriginalData/lmps/snl_dlap_lmps.xlsx"), skip=1)

colnames(lmps) <- c("dt", "pge.hourly.da.lmp", "sdge.hourly.da.lmp", "sce.hourly.da.lmp")

lmps <- lmps %>% 
    mutate(
           date = date(dt), 
           month = month(dt), 
           year = year(dt), 
           hour.beginning = hour(dt)
    )

# Get long by iou
lmps <- lmps %>% 
    rename(
           pge = pge.hourly.da.lmp, 
           sdge = sdge.hourly.da.lmp, 
           sce = sce.hourly.da.lmp
    )
lmps <- pivot_longer(lmps, cols = c(pge, sce, sdge), names_to = "iou", values_to = "hourly.da.lmp")

# Average lmps so we get only one observation for daylight savings adjustment - we're not using a load weighted avg here since this is just a quick correction of a handful of obs
lmps <- lmps %>% group_by(dt, date, month, year, hour.beginning, iou) %>% summarize(hourly.da.lmp = mean(hourly.da.lmp)) %>% ungroup()

# Variable O&M costs are taken from E3 avoided cost calculators
# There is a gap (no calculators) from 2011 to 2015
# Escalation rate from 2010 calc is 2.5%
# We apply this to 2011-2015, 2017, 2020
variable.om.costs <- tibble(year = c(2010:2020),
                            var.om.cost.dollars.per.mwh = c(3.02, NA, NA, NA, NA, NA, 0.66, 0.6765, 5, 5.52, 5.658))
# Plug gap with escalation rate
variable.om.costs <- variable.om.costs %>% 
    mutate(var.om.cost.dollars.per.mwh = if_else(is.na(var.om.cost.dollars.per.mwh), 3.02 * (1.025^(row_number()-1)), var.om.cost.dollars.per.mwh))

# Load permit prices
permit.prices <- read_excel(paste0(dropbox.dir, "OriginalData/permit_prices/permit_prices.xlsx"))

# Drop 2012
permit.prices <- permit.prices %>% 
    filter(year > 2012)

# Add a row with 0 for jan 2010
permit.prices <- bind_rows(tibble(auction.settlement.price = 0, year = 2010, month = 1), permit.prices)
# Make data and carryforward permit price to other dates
permit.prices <- permit.prices %>% 
    mutate(date = make_date(year = year, month = month, day = 1))
merge.date <- tibble(date = seq(ymd('2010-01-01'),ymd('2020-07-26'),by='days'))
permit.prices <- full_join(permit.prices, merge.date) %>% 
    arrange(date) %>% 
    select(-year, -month)
p_load(zoo)
permit.prices <- permit.prices %>% 
    mutate(auction.settlement.price = na.locf(auction.settlement.price))

# Load gas prices - cleaned these above
gas.prices <- read_rds( paste0(dropbox.dir, "ModifiedData/gas.prices.clean.rds"))
gas.prices <- gas.prices %>% 
    mutate(date = date(dt)) %>% 
    select(-dt)
merge.date <- tibble(date = seq(ymd('2010-01-01'),ymd('2020-06-30'),by='days'))
gas.prices <- left_join(merge.date, gas.prices) %>% 
    arrange(date)
# Take weighted avg but exclude pge south because it's not used much and has a lot of missing values
gas.prices <- gas.prices %>% 
    mutate(
           norcal.avg.price = (norcal.border.malin.price * norcal.border.malin.volume + pge.gate.price * pge.gate.volume) / (norcal.border.malin.volume + pge.gate.volume),

           socal.avg.price = (socal.border.price * socal.border.volume + socal.citygate.price * socal.citygate.volume + kern.river.station.price * kern.river.station.volume) / (socal.border.volume + socal.citygate.volume + kern.river.station.volume)
           ) %>% 
    select(date, norcal.avg.price, socal.avg.price)
gas.prices <- gas.prices %>% 
    mutate(
           norcal.avg.price = na.locf(norcal.avg.price), 
           socal.avg.price = na.locf(socal.avg.price)
           )

# Get long by iou
gas.prices <- gas.prices %>% 
    mutate(
           pge = norcal.avg.price, 
           sce = socal.avg.price, 
           sdge = socal.avg.price
           ) %>% 
    select(-norcal.avg.price, -socal.avg.price) %>% 
    pivot_longer(cols = c(pge, sce, sdge), names_to = "iou", values_to = "gas.price.dollars.per.mmbtu")

# Emissions factor. This is from EPA. Units are metric tons / mmBtu
emissions.factor <- 0.05307

# Merge LMP, gas prices, and permit prices
heat.rates <- inner_join(lmps, gas.prices)
heat.rates <- inner_join(heat.rates, permit.prices)
heat.rates <- inner_join(heat.rates, variable.om.costs)

heat.rates <- heat.rates %>% 
    mutate(heat.rate.mmbtu.per.mwh = (hourly.da.lmp - var.om.cost.dollars.per.mwh) / (gas.price.dollars.per.mmbtu + emissions.factor * auction.settlement.price))

# These are bounds that e3 uses
# If computed heat rates are wild we just bound them above and below
heat.rate.lb <- 0
heat.rate.ub <- 12.5
heat.rates <- heat.rates %>% 
    mutate(heat.rate.mmbtu.per.mwh.bounded = case_when(
                                                       heat.rate.mmbtu.per.mwh < heat.rate.lb ~ heat.rate.lb,
                                                       heat.rate.mmbtu.per.mwh > heat.rate.ub ~ heat.rate.ub,
                                                       TRUE ~ heat.rate.mmbtu.per.mwh
           )
    )

# Now get marginal energy cost
mecs <- inner_join(lmps, permit.prices)
mecs <- inner_join(mecs, heat.rates %>% select(dt, iou, heat.rate.mmbtu.per.mwh.bounded), by = c("dt", "iou"))

mecs <- inner_join(mecs, marginal.losses %>% select(date, hour.beginning, iou, dl.dq), by = c("date", "hour.beginning", "iou"))

mecs <- mecs %>% mutate(loss.factor = 1 / (1 - dl.dq),
                        mec = (hourly.da.lmp - (auction.settlement.price * heat.rate.mmbtu.per.mwh.bounded * emissions.factor)))

# Avg monthly value
monthly.plot.dat <- mecs %>% mutate(month = month(date))

monthly.plot.dat <- monthly.plot.dat %>% group_by(year, iou, month) %>% summarize(avg.marginal.energy.cost = mean(mec, na.rm=T))

ggplot(data = monthly.plot.dat, aes(fill=iou, y=avg.marginal.energy.cost, x=month)) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = 1:12) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Month", y = "Avg. monthly value of energy ($/MWh)")

aspect.ratio <- .6
name <- "Graphs/avg.monthly.energy.value.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# Avg hourly value
hourly.plot.dat <- mecs %>% group_by(year, iou, hour.beginning) %>% summarize(avg.marginal.energy.cost = mean(mec, na.rm=T))

ggplot(data = hourly.plot.dat, aes(fill=iou, y=avg.marginal.energy.cost, x=hour.beginning)) + 
    geom_bar(position="dodge", stat="identity") +
    scale_x_continuous(breaks = seq(from = 0, to = 23, by = 3)) +
    theme_minimal() +
    facet_wrap(~year) +
    labs(x = "Hour", y = "Avg. hourly value of energy ($/MWh)")

aspect.ratio <- .6
name <- "Graphs/avg.hourly.energy.value.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# 
# Get carbon (social) and cap and trade (private)
# from mecs
#

# Carbon is cost in excess of cap and trade cost
mecs <- mecs %>% 
    mutate(
           cap.and.trade.cost = auction.settlement.price * heat.rate.mmbtu.per.mwh.bounded * emissions.factor,
           ghg.cost.50 = (50 - auction.settlement.price) * heat.rate.mmbtu.per.mwh.bounded * emissions.factor,
           ghg.cost.10 = (10 - auction.settlement.price) * heat.rate.mmbtu.per.mwh.bounded * emissions.factor,
           ghg.cost.100 = (100 - auction.settlement.price) * heat.rate.mmbtu.per.mwh.bounded * emissions.factor
           )

# 
# Get ancillary services
#

# Load data - from caiso market issus and performance reports
ancillary.services.dat <- read_excel(paste0(dropbox.dir, "OriginalData/ancillary_services/as_data.xlsx")) %>%
    mutate(year = as.integer(year))

mecs <- left_join(mecs, ancillary.services.dat) %>%
    mutate(ancillary.services = mec * as.pct.of.energy)

# 
# Compile results
#
res <- inner_join(gen.cap %>% 
                        select(hour.beginning, date, iou, e3.gen.cap.dollars.per.mw, ra.gen.cap.dollars.per.mw) %>% 
                        mutate(hour.beginning = as.numeric(hour.beginning)),
                  mecs)
                  
res <- inner_join(res,
                  t.n.d.cap %>% 
                      select(hour.beginning, date, iou, t.dollars.per.mw, d.dollars.per.mw), by = c("hour.beginning", "date", "iou"))
 
res <- res %>% 
    arrange(date, hour.beginning, iou) %>% 
    select(-dt)

res <- res %>% 
    select(iou, year, month, date, hour.beginning, everything())

write_xlsx(res, paste0(dropbox.dir, "Tables/pfe_avoided_costs_raw_by_iou_hour.xlsx"))
write_rds(res, paste0(dropbox.dir, "ModifiedData/pfe_avoided_costs_raw_by_iou_hour.rds"))

# 
# Grab E3 estimates from avoided cost calculator.
# These are at the climate zone level
#
file.prefix <- "OriginalData/avoided_cost_calculator/"
file.suffix <- ".xlsx"

# 
# Function to load
#
# Arguments: year
# Output: tibble
#
load.acc <- function(year, cz) {

    # This grabs the right iou based on the climate zone
    sce.czs <- as.character(c(6, 8, 9, 10, 13, 14, 15, 16))
    sdge.czs <- as.character(c(7, 10, 14, 15))
    iou <- case_when(
              cz %in% sce.czs ~ "SCE",
              cz %in% sdge.czs ~ "SDG&E",
              TRUE ~ "PG&E"
              )

    # Read data
    file.prefix <- "OriginalData/avoided_cost_calculator/"
    acc <- read_excel(paste0(dropbox.dir, file.prefix, year, "_avoided_cost_by_utility/", iou, " ", year, " by CZ component.xlsx"), sheet = paste0("CZ", cz), skip = 1)

    acc <- acc %>% 
        mutate(cz = cz, iou = tolower(str_replace(iou, "&", "")), year = year)

    return(acc)
}

# Run function to load for each year and climate zone and then bind rows
czs <- as.character(c(1:16))

# 2010 ACC has emissions even though there was no carbon prie, so we zero this out. Turns out they took a carbon
# price forecast for 2012 onwards and extrapolated back to
# 2008. That's not right since there was no market back then.
# We also add the cap and trade value back into energy since
# it was removed from the energy cost for those years erroneously.
acc.2010 <- map_dfr(2010, function(x) map2_dfr(x, czs, load.acc)) %>%
    rename_all(funs(str_to_lower(str_replace_all(., " ", ".")))) %>%
    rename(dt = 'date/time.stamp', t.and.d = "t&d", cap.and.trade = emissions)

# Load each, do some renaming and get correct year as we go
czs <- c(1, 2, 4:16)
czs <- c("3A", "3B", czs)

# 2016
y <- 2016
acc.2016 <- map_dfr(y, function(x) map2_dfr(x, czs, load.acc)) %>% 
    rename_all(funs(str_to_lower(str_replace_all(., " ", ".")))) %>% 
    rename(dt = 'date/time.stamp', cap.and.trade = emissions)
dt.new <- as_datetime(acc.2016$dt)
year(dt.new) <- y
acc.2016 <- acc.2016 %>% 
    mutate(dt = dt.new)

# 2017
y <- 2017
acc.2017 <- map_dfr(y, function(x) map2_dfr(x, czs, load.acc)) %>% 
    rename_all(funs(str_to_lower(str_replace_all(., " ", ".")))) %>% 
    rename(dt = 'date/time.stamp', ghg.adder = societal.carbon, cap.and.trade = emissions) %>% 
    select(-societal.criteria.pollutants)
dt.new <- as_datetime(acc.2017$dt)
year(dt.new) <- y
acc.2017 <- acc.2017 %>% 
    mutate(dt = dt.new)

# 2018
y <- 2018
acc.2018 <- map_dfr(y, function(x) map2_dfr(x, czs, load.acc)) %>% 
    rename_all(funs(str_to_lower(str_replace_all(., " ", ".")))) %>% 
    rename(dt = 'date/time.stamp', avoided.rps = "avoided.rps.-.not.used") %>% 
    select(-societal.criteria.pollutants)
dt.new <- as_datetime(acc.2018$dt)
year(dt.new) <- y
acc.2018 <- acc.2018 %>% 
    mutate(dt = dt.new)

# 2019
y <- 2019
acc.2019 <- map_dfr(y, function(x) map2_dfr(x, czs, load.acc)) %>% 
    rename_all(funs(str_to_lower(str_replace_all(., " ", ".")))) %>% 
    rename(dt = 'date/time.stamp', avoided.rps = "avoided.rps.[not.used]")
dt.new <- as_datetime(acc.2019$dt)
year(dt.new) <- y
acc.2019 <- acc.2019 %>% 
    mutate(dt = dt.new)

acc <- bind_rows(acc.2010, acc.2016, acc.2017, acc.2018, acc.2019)

# Replicate czs 10, 14, and 15 for sdg&e
acc.sdge.czs.to.add <- acc %>% 
    filter(iou == "sce", cz %in% c("10", "14", "15")) %>% 
    mutate(iou = "sdge")
acc <- bind_rows(acc, acc.sdge.czs.to.add)

# 2010 did not have separate t and d so we make one variable
# for later years to facilitate comparison
acc <- acc %>% 
    mutate(t.and.d = if_else(is.na(transmission), t.and.d, transmission + distribution))

# Get date/hour variables
acc <- acc %>% 
    mutate(hour.beginning = hour(dt), year = year(dt), date = date(dt)) %>% 
    select(iou, year, cz, date, hour.beginning, dt, year, everything())

# Check
count <- acc %>% 
    group_by(iou, cz, year) %>% 
    summarize(num = n())

# Add ghg cost reflecting permit price of 50
# To do this we take the cap and trade cost that we
# computed in the pfe version, and the $50 ghg adder,
# sum those then subtract E3's cap and trade cost
# This adds in a $50 ghg cost (an adder above cap and
# trade cost as it was calculated by E3
ghg.adder.for.e3 <- res %>%
    mutate(e3.ghg.50 = ghg.cost.50 + cap.and.trade.cost) %>%
    select(iou, e3.ghg.50, hour.beginning, date)
acc <- inner_join(acc, ghg.adder.for.e3)
acc <- acc %>% 
    mutate(ghg.50 = e3.ghg.50 - cap.and.trade) %>%
    select(-e3.ghg.50)

# 
# Combine our result so far from our own calculations
# with acc and compare
#

# Load weights to use for collapsing climate zones 3A and
# 3B into just 3, since our climate zone data does not have
# A and B
pge.div.weights <- read_excel(paste0(dropbox.dir, "OriginalData/t_and_d_capacity_values/pge_t_and_d_capacity_values.xlsx"))
# Rename 2010 cz
pge.div.weights <- pge.div.weights %>% 
    mutate(cz = if_else(calculator.year == 2010 & grepl("3[AB]", cz), "3", cz))

pge.div.weights <- pge.div.weights %>% 
    select(iou, base.year, calculator.year, division, cz, pcaf.kw) %>% 
    group_by(cz, calculator.year) %>%
    summarize(pcaf.kw = sum(pcaf.kw)) %>%
    mutate(iou = "pge") %>%
    rename(year = calculator.year) %>%
    ungroup()

# Add weights and collapse pg&e
acc.pge.t.and.d <- left_join(acc %>% filter(iou == "pge"), pge.div.weights) %>%
    mutate(cz = if_else(grepl("3[AB]", cz), "3", cz)) %>%
    group_by(cz, iou, dt, year) %>%
    summarize(t.and.d = weighted.mean(t.and.d, pcaf.kw, na.rm=T),
              transmission = weighted.mean(transmission, pcaf.kw, na.rm=T),
              distribution = weighted.mean(distribution, pcaf.kw, na.rm=T)) %>%
    ungroup()

# Now use cz weights from above to get down to iou level
acc.pge.t.and.d <- left_join(acc.pge.t.and.d, pge.cz.weights) %>%
    group_by(iou, dt, year) %>%
    summarize(t.and.d = weighted.mean(t.and.d, tot.kwh, na.rm=T),
              transmission = weighted.mean(transmission, tot.kwh, na.rm=T),
              distribution = weighted.mean(distribution, tot.kwh, na.rm=T)) %>%
    ungroup()

# Select cz 1 for all other variables for pg&e since those don't vary, merge with new weighte t and d values
acc.pge <- inner_join(acc %>% 
                            filter(iou == "pge") %>% 
                            select(-t.and.d, -transmission, -distribution) %>% 
                            filter(cz == "1"), 
                        acc.pge.t.and.d)

# Select climate zones 6 and 7 for others
# Does not matter since everything except for t&d at pg&e does
# not vary by climate zone
acc.not.pge <- acc %>% 
    filter(iou != "pge" & cz %in% c("6", "7"))

acc <- bind_rows(acc.pge, acc.not.pge) %>% 
    select(-cz)

# Do some other cleaning
acc.compare <- acc %>% rename_at(vars(-(1:5)), list(~paste0(., ".acc"))) %>% 
    select(-dt, -year)

master.comparison.table <- left_join(res, acc.compare, by = c("iou", "date", "hour.beginning"))

master.comparison.table <- left_join(master.comparison.table, sys.load.by.iou %>% select(iou, date, hour.beginning, load.mw))

master.comparison.table <- master.comparison.table %>% 
    select(iou, year, month, date, hour.beginning, e3.gen.cap.dollars.per.mw, ra.gen.cap.dollars.per.mw, capacity.acc, t.dollars.per.mw, d.dollars.per.mw, t.and.d.acc, transmission.acc, distribution.acc, mec, energy.acc, ancillary.services, ancillary.services.acc, ghg.cost.10, ghg.cost.50, ghg.cost.100, ghg.50.acc, cap.and.trade.cost, cap.and.trade.acc, ghg.adder.acc, losses.acc, hourly.da.lmp,  auction.settlement.price, heat.rate.mmbtu.per.mwh.bounded, dl.dq, loss.factor, load.mw) %>% 
    rename(e3.gen.capacity = e3.gen.cap.dollars.per.mw, 
           ra.gen.capacity = ra.gen.cap.dollars.per.mw, 
           gen.capacity.acc = capacity.acc,
           t.capacity = t.dollars.per.mw,
           d.capacity = d.dollars.per.mw,
           t.capacity.acc = transmission.acc,
           d.capacity.acc = distribution.acc,
           t.and.d.capacity.acc = t.and.d.acc,
           energy = mec, 
           ghg.50 = ghg.cost.50, 
           ghg.10 = ghg.cost.10, 
           ghg.100 = ghg.cost.100, 
           cap.and.trade = cap.and.trade.cost) %>%
    mutate(t.and.d.capacity = t.capacity + d.capacity)

# Now we have all the components we make losses category
# This is so we can view losses as a separate component
# in a stacked chart and in tables.
# When we visualize avoided cost in something like the waterfall, each component is adjusted for losses, rather than using a separate losses category.
master.comparison.table <- master.comparison.table %>%
    mutate(losses.preferred = (ra.gen.capacity + t.capacity + d.capacity + energy + ancillary.services + ghg.50 + cap.and.trade) * (loss.factor - 1),
           losses.scc.10 = (ra.gen.capacity + t.capacity + d.capacity + energy + ancillary.services + ghg.10 + cap.and.trade) * (loss.factor - 1),
           losses.scc.100 = (ra.gen.capacity + t.capacity + d.capacity + energy + ancillary.services + ghg.100 + cap.and.trade) * (loss.factor - 1),
           losses.zero.t.and.d = (ra.gen.capacity + energy + ancillary.services + ghg.50 + cap.and.trade) * (loss.factor - 1))
write_rds(master.comparison.table, paste0(dropbox.dir, "ModifiedData/master.comparison.table.rds"))

# Make table of avg. values by year, iou
master.comparison.table.year.iou <- master.comparison.table %>% 
    group_by(iou, year) %>% 
    summarize(mean.e3.gen.capacity = mean(e3.gen.capacity), 
              mean.ra.gen.capacity = mean(ra.gen.capacity), 
              mean.gen.capacity.acc = mean(gen.capacity.acc, na.rm=T), 
              mean.t.capacity = mean(t.capacity, na.rm = T),
              mean.t.capacity.acc = mean(t.capacity.acc, na.rm = T),
              mean.d.capacity = mean(d.capacity, na.rm = T),
              mean.d.capacity.acc = mean(d.capacity.acc, na.rm = T),
              mean.t.and.d.capacity = mean(t.and.d.capacity, na.rm = T),
              mean.t.and.d.capacity.acc = mean(t.and.d.capacity.acc, na.rm = T),
              mean.energy = mean(energy, na.rm=T), 
              mean.energy.acc = mean(energy.acc, na.rm=T),
              mean.ancillary.services = mean(ancillary.services, na.rm = T),
              mean.ancillary.services.acc = mean(ancillary.services.acc, na.rm = T),
              mean.ghg.adder.10 = mean(ghg.10, na.rm =T), 
              mean.ghg.adder.50 = mean(ghg.50, na.rm =T), 
              mean.ghg.adder.100 = mean(ghg.100, na.rm =T), 
              mean.ghg.adder.50.acc = mean(ghg.50.acc, na.rm =T), 
              mean.cap.and.trade = mean(cap.and.trade, na.rm=T), 
              mean.cap.and.trade.acc = mean(cap.and.trade.acc, na.rm=T), 
              mean.losses.preferred = mean(losses.preferred, na.rm=T), 
              mean.losses.acc = mean(losses.acc, na.rm=T))

# Make table of load-weighted avg. values by year, iou
master.comparison.table.year.iou.load.weighted <- master.comparison.table %>% 
    filter(!is.na(load.mw)) %>% 
    group_by(iou, year) %>% 
    summarize(mean.e3.gen.capacity = weighted.mean(e3.gen.capacity, load.mw), 
              mean.ra.gen.capacity = weighted.mean(ra.gen.capacity, load.mw), 
              mean.gen.capacity.acc = weighted.mean(gen.capacity.acc, load.mw, na.rm=T), 
              mean.t.capacity = weighted.mean(t.capacity, load.mw, na.rm = T),
              mean.t.capacity.acc = weighted.mean(t.capacity.acc, load.mw, na.rm = T),
              mean.d.capacity = weighted.mean(d.capacity, load.mw, na.rm = T),
              mean.d.capacity.acc = weighted.mean(d.capacity.acc, load.mw, na.rm = T),
              mean.t.and.d.capacity = weighted.mean(t.and.d.capacity, load.mw, na.rm = T),
              mean.t.and.d.capacity.acc = weighted.mean(t.and.d.capacity.acc, load.mw, na.rm = T),
              mean.energy = weighted.mean(energy, load.mw, na.rm=T), 
              mean.energy.acc = weighted.mean(energy.acc, load.mw, na.rm=T), 
              mean.ancillary.services = weighted.mean(ancillary.services, load.mw, na.rm = T),
              mean.ancillary.services.acc = weighted.mean(ancillary.services.acc, load.mw, na.rm = T),
              mean.ghg.adder.10 = weighted.mean(ghg.10, load.mw, na.rm =T), 
              mean.ghg.adder.50 = weighted.mean(ghg.50, load.mw, na.rm =T), 
              mean.ghg.adder.100 = weighted.mean(ghg.100, load.mw, na.rm =T), 
              mean.ghg.adder.50.acc = weighted.mean(ghg.50.acc, load.mw, na.rm =T), 
              mean.cap.and.trade = weighted.mean(cap.and.trade, load.mw, na.rm=T), 
              mean.cap.and.trade.acc = weighted.mean(cap.and.trade.acc, load.mw, na.rm=T), 
              mean.losses.preferred = weighted.mean(losses.preferred, load.mw, na.rm=T), 
              mean.losses.acc = weighted.mean(losses.acc, load.mw, na.rm=T))
write_rds(master.comparison.table.year.iou.load.weighted, paste0(dropbox.dir, "ModifiedData/master.comparison.table.year.iou.load.weighted.rds"))

# 
# Finally make a table that is just values from avoided cost calculator
#
acc.compare.year.iou <- master.comparison.table %>% 
    filter(year %in% c(2010, 2016:2019)) %>%
    group_by(iou, year) %>%
    summarize(
              mean.gen.capacity.acc = mean(gen.capacity.acc, na.rm=T),
              mean.energy.acc = mean(energy.acc, na.rm=T),
              mean.ghg.50.acc = mean(ghg.50.acc, na.rm =T),
              mean.cap.and.trade.acc = mean(cap.and.trade.acc, na.rm=T),
              mean.losses.acc = mean(losses.acc, na.rm=T),
              mean.ancillary.services.acc = mean(ancillary.services.acc, na.rm=T),
              mean.t.and.d.capacity.acc = mean(t.and.d.capacity.acc, na.rm=T),
              mean.t.capacity.acc = mean(t.capacity.acc, na.rm=T),
              mean.d.capacity.acc = mean(d.capacity.acc, na.rm=T))

acc.compare.year.iou.load.weighted <- master.comparison.table %>% 
    filter(year %in% c(2010, 2016:2019)) %>%
    filter(!is.na(load.mw)) %>%
    group_by(iou, year) %>%
    summarize(
              mean.gen.capacity.acc = weighted.mean(gen.capacity.acc, load.mw, na.rm=T),
              mean.energy.acc = weighted.mean(energy.acc, load.mw, na.rm=T),
              mean.ghg.adder.acc = weighted.mean(ghg.adder.acc, load.mw, na.rm =T),
              mean.cap.and.trade.acc = weighted.mean(cap.and.trade.acc, load.mw, na.rm=T),
              mean.losses.acc = weighted.mean(losses.acc, load.mw, na.rm=T),
              mean.ancillary.services.acc = weighted.mean(ancillary.services.acc, load.mw, na.rm=T),
              mean.t.and.d.capacity.acc = weighted.mean(t.and.d.capacity.acc, load.mw, na.rm=T),
              mean.t.capacity.acc = weighted.mean(t.capacity.acc, load.mw, na.rm=T),
              mean.d.capacity.acc = weighted.mean(d.capacity.acc, load.mw, na.rm=T))


write_xlsx(list("Master Table" = master.comparison.table,
                "Simple Averages" = master.comparison.table.year.iou,
                "Load Weighted Averages" = master.comparison.table.year.iou.load.weighted,
                "E3 ACC Simple Averages" = acc.compare.year.iou, 
                "E3 ACC Load Weighted Averages" = acc.compare.year.iou.load.weighted), 
           paste0(dropbox.dir, "Tables/master_e3_v_pfe_acc_comparison_table.xlsx"))

# Get losses summary stats
losses.summary.stats <- master.comparison.table %>% 
    filter(!is.na(load.mw)) %>% 
    group_by(iou, year) %>% 
    summarize(mean.loss.factor = mean(loss.factor, na.rm=T),
    mean.loss.factor.load.weighted = weighted.mean(loss.factor, load.mw, na.rm=T))

write_xlsx(list("Losses summary stats" = losses.summary.stats),
           paste0(dropbox.dir, "Tables/losses_summary_stats.xlsx"))

#
# Make table for report with our various measures of avoided cost, by year
#
# Note now we're moving to $/kWh rather than $/MWh
#

# Load discount from utility cost reports data which we use for 2014 onwards
utility.cost.reports.data <- read_excel(paste0(dropbox.dir, "OriginalData/utility_cost_reports/effective_care_discounts.xlsx")) %>%
    rename(care.discount.cost.reports = care.discount)

# Load prices from CARE reports, use these to get discount for 2010-2013
care.reports.data <- read_excel(paste0(dropbox.dir, "OriginalData/care_reports/noncare_and_care_prices_by_iou_year.xlsx")) %>%
    mutate(
           noncare.price = noncare.bill.dollars / noncare.kwh,
           care.price = care.bill.dollars / care.kwh,
           care.discount = (noncare.price - care.price) / noncare.price
           ) %>%
    select(iou, year, care.discount, care.kwh, noncare.kwh, care.customers)

# Merge care discounts data and make single variable that uses utility cost reports data for discount from 2014 onweards, care reports data otherwise
care.data <- left_join(care.reports.data, utility.cost.reports.data) %>%
    mutate(care.discount = if_else(is.na(care.discount.cost.reports), care.discount, care.discount.cost.reports))

# Output
write_xlsx(care.data %>% select(iou, year, care.discount) %>% mutate(data.source = if_else(year >= 2014, "utility cost reports", "care reports")), paste0(dropbox.dir, "Tables/care_data_from_care_reports_and_utility_cost_reports.xlsx"))

# Load customer counts from ferc form 1
ferc.f1 <- read_rds(paste0(dropbox.dir, "ModifiedData/ferc.f1.v2.clean.rds")) %>% 
    filter(ca.iou) %>%
    mutate(iou = case_when(utility == "pacific gas and electric company" ~ "pge",
                           utility == "southern california edison company" ~ "sce",
                           utility == "san diego gas & electric company" ~ "sdge"),
           year = as.numeric(year)) %>%
    select(iou, year, total.res.customers, bundled.res.price.dollars.per.kwh)

# Load SCE bundled sales from ferc form 1
# We use this to get bundled revenue
sce.bundled.rev <- read_excel(paste0(dropbox.dir, "OriginalData/ferc_form_1/snl_sce_bundled_res_rev_and_sales.xlsx"), sheet = "clean") %>%
    select(-bundled.res.sales.mwh)

# Load net bundled sales we obtained from SCE
sce.bundled.sales <- read_excel(paste0(dropbox.dir, "OriginalData/SCE_Annual_Domestic_kWh_Borenstein.xlsx"), range = "Domestic Bundled Sales!B2:H22") %>%
    rename(
           year = "Year",
           channel.1.kwh = "Channel 1 kWh...6",
           net.kwh = "Net kWh...7"
           ) %>%
    select(year, net.kwh) %>%
    mutate(iou = "sce")

sce.corrected.price <- inner_join(sce.bundled.rev, sce.bundled.sales) %>%
    mutate(bundled.res.price.sce.corrected = bundled.res.revenue.thousands * 1000 / net.kwh) %>%
    select(year, iou, bundled.res.price.sce.corrected)


prices <- left_join(care.data, ferc.f1) %>%
    left_join(., sce.corrected.price) %>%
    mutate(
           bundled.res.price.dollars.per.kwh = if_else(iou == "sce", bundled.res.price.sce.corrected, bundled.res.price.dollars.per.kwh), # Replace SCE price in FERC with new price we computed using revenue and net bundled sales
           care.kwh.annual = care.kwh * 12 * care.customers,
           noncare.kwh.annual = noncare.kwh * 12 * (total.res.customers - care.customers),
            noncare.price = (bundled.res.price.dollars.per.kwh * (noncare.kwh.annual + care.kwh.annual)) / ((1 - care.discount) * care.kwh.annual + noncare.kwh.annual),
            care.price = noncare.price * (1 - care.discount)
            ) %>%
    select(iou, year, noncare.price, care.price)
           




# # Load prices from CARE reports
# prices <- read_excel(paste0(dropbox.dir, "OriginalData/care_reports/noncare_and_care_prices_by_iou_year.xlsx"))
#
# # Get price
# prices <- prices %>%
#     mutate(noncare.price = noncare.bill.dollars / noncare.kwh,
#            care.price = care.bill.dollars / care.kwh) %>%
#     select(iou, year, noncare.price, care.price)

# # Model and extrapolate historic prices to 2019
# noncare.lines <- lm(data = prices, noncare.price ~ year*iou)
# care.lines <- lm(data = prices, care.price ~ year*iou)
#
# predicted <- tibble(iou = c("pge", "sce", "sdge"),
#                      year = c(2019, 2019, 2019)) %>%
#     mutate(noncare.price = predict(noncare.lines, newdata = .),
#                                    care.price = predict(care.lines, newdata = .))
#
# prices <- bind_rows(predicted, prices)


summary.main.result <- master.comparison.table %>% 
    filter(!is.na(load.mw)) %>%
    mutate(preferred = (ra.gen.capacity + t.capacity + d.capacity + energy + ancillary.services + ghg.50 + cap.and.trade + losses.preferred) / 1000,
           scc.10 = (ra.gen.capacity + t.capacity + d.capacity + energy + ancillary.services + ghg.10 + cap.and.trade + losses.scc.10) / 1000,
           scc.100 = (ra.gen.capacity + t.capacity + d.capacity + energy + ancillary.services + ghg.100 + cap.and.trade + losses.scc.100) / 1000,
           e3 = (gen.capacity.acc + t.and.d.capacity.acc + energy.acc + ancillary.services.acc + ghg.50.acc + cap.and.trade.acc + losses.acc) / 1000,
           zero.t.and.d = (ra.gen.capacity + energy + ancillary.services + ghg.50 + cap.and.trade + losses.zero.t.and.d) / 1000) %>%
    group_by(iou, year) %>%
    summarize(preferred = weighted.mean(preferred, load.mw, na.rm=T),
              scc.10 = weighted.mean(scc.10, load.mw, na.rm=T),
              scc.100 = weighted.mean(scc.100, load.mw, na.rm=T),
               e3 = weighted.mean(e3, load.mw, na.rm=T),
               zero.t.and.d = weighted.mean(zero.t.and.d, load.mw, na.rm=T))

summary.main.result <- left_join(summary.main.result, prices)

write_xlsx(list("Summary Main Result" = summary.main.result), paste0(dropbox.dir, "Tables/summary_main_result.xlsx"))
write_rds(summary.main.result, paste0(dropbox.dir, "ModifiedData/summary_main_result.rds"))

# Make table of lmps - these track large avoided cost nicely
lmps %>% group_by(iou, year) %>% summarize(hourly.da.lmp = mean(hourly.da.lmp, na.rm=T))

# 
# Make time series graph of 5 avoided cost numbers for each iou
#
five.acs.plot.data  <- summary.main.result %>% 
    ungroup() %>%
    select(-noncare.price, care.price) %>%
    pivot_longer(cols = c(preferred, scc.10, scc.100, e3, zero.t.and.d)) %>%
    mutate(name = factor(name, levels = c("preferred", "scc.10", "scc.100", "e3", "zero.t.and.d"), labels = c("Primary", "SCC10", "SCC100", "E3", "T&D=0")),
           iou.factor = factor(iou, levels = c("pge", "sce", "sdge"), labels = c("PG&E", "SCE", "SDG&E")))

five.acs.plot.data <- left_join(five.acs.plot.data, cpi) %>%
    mutate(value = value * end.year.cpi / year.cpi)

# All
five.acs.plot <- ggplot(five.acs.plot.data, aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    scale_y_continuous(limits = c(0, .15), breaks = seq(0, .15, .05)) +
    theme_minimal() +
    scale_color_discrete("") +
    labs(x = "Year", y = "$/kWh (real $2019)") +
    theme(axis.text=element_text(size=14),
          axis.title=element_text(size=15),
          legend.text=element_text(size=15),
          legend.title=element_text(size=15),
          plot.title=element_text(size=18),
          strip.text = element_text(size=18),
          legend.position = "bottom",
          panel.grid.minor.x = element_blank()) +
    facet_wrap(~iou.factor, ncol=1, scale = "free")
five.acs.plot

aspect.ratio <- 1.4
name <- "Graphs/five_avoided_costs_time_series_plot.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# PG&E
five.acs.plot.pge <- ggplot(five.acs.plot.data %>% filter(iou == "pge"), aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    scale_y_continuous(limits = c(0, .15), breaks = seq(0, .15, .05)) +
    theme_minimal() +
    scale_color_discrete("") +
    labs(x = "Year", y = "$/kWh (real $2019)") +
    theme(axis.text=element_text(size=14),
          axis.title=element_text(size=15),
          legend.text=element_text(size=15),
          legend.title=element_text(size=15),
          plot.title=element_text(size=18),
          strip.text = element_text(size=18),
          legend.position = "bottom",
          panel.grid.minor.x = element_blank())
five.acs.plot.pge

aspect.ratio <- .6
name <- "Graphs/five_avoided_costs_time_series_plot_pge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# SCE
five.acs.plot.sce <- ggplot(five.acs.plot.data %>% filter(iou == "sce"), aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    scale_y_continuous(limits = c(0, .15), breaks = seq(0, .15, .05)) +
    theme_minimal() +
    scale_color_discrete("") +
    labs(x = "Year", y = "$/kWh (real $2019)") +
    theme(axis.text=element_text(size=14),
          axis.title=element_text(size=15),
          legend.text=element_text(size=15),
          legend.title=element_text(size=15),
          plot.title=element_text(size=18),
          strip.text = element_text(size=18),
          legend.position = "bottom",
          panel.grid.minor.x = element_blank())
five.acs.plot.sce

aspect.ratio <- .6
name <- "Graphs/five_avoided_costs_time_series_plot_sce.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# SDG&E
five.acs.plot.sdge <- ggplot(five.acs.plot.data %>% filter(iou == "sdge"), aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    scale_y_continuous(limits = c(0, .15), breaks = seq(0, .15, .05)) +
    theme_minimal() +
    scale_color_discrete("") +
    labs(x = "Year", y = "$/kWh (real $2019)") +
    theme(axis.text=element_text(size=14),
          axis.title=element_text(size=15),
          legend.text=element_text(size=15),
          legend.title=element_text(size=15),
          plot.title=element_text(size=18),
          strip.text = element_text(size=18),
          legend.position = "bottom",
          panel.grid.minor.x = element_blank())
five.acs.plot.sdge

aspect.ratio <- .6
name <- "Graphs/five_avoided_costs_time_series_plot_sdge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# 
# Make grouped bar of preferred and care and non-care price for 2019
#

# This dataset is generated above
# These are the same non-CARE rates we use for the height of the waterfall graph, and the revenue recovery calculation
waterfall.rates.2019 <- read_rds(paste0(dropbox.dir, "ModifiedData/care_and_noncare_rates_by_iou_and_major_component_for_2019.rds")) %>%
    group_by(iou) %>%
    summarize(noncare.price = sum(noncare.rate),
              care.price = sum(care.rate))

preferred.vs.price.grouped.bar.data  <- summary.main.result %>% 
    ungroup() %>%
    filter(year == 2019) %>%
    select(iou, year, preferred) %>%
    left_join(., waterfall.rates.2019) %>%
    pivot_longer(cols = c(preferred, noncare.price, care.price)) %>%
    mutate(name = factor(name, levels = c("preferred", "noncare.price", "care.price"), labels = c("Primary Marginal Cost", "Non-CARE Price", "CARE Price")),
           iou = factor(iou, levels = c("pge", "sce", "sdge"), labels = c("PG&E", "SCE", "SDG&E")))


preferred.vs.price.grouped.bar <- ggplot(preferred.vs.price.grouped.bar.data, aes(x=iou, y=value, fill=name)) +
    geom_bar(position = "dodge", stat = "identity") +
    # geom_line(size = .75) +
    # geom_point(size = 2) +
    # scale_x_continuous(breaks = 2010:2019) +
    ylim(0, .3) +
    theme_minimal() +
    scale_fill_manual("", values=c("#F8766D", "#EDAE49", "#619CFF")) + # Keep default colors but change green to yellow
    labs(x = "IOU", y = "$/kWh") +
    theme(axis.text=element_text(size=14),
          axis.title=element_text(size=17),
          legend.text=element_text(size=17),
          legend.title=element_text(size=18),
          plot.title=element_text(size=20))

preferred.vs.price.grouped.bar

aspect.ratio <- .6
name <- "Graphs/preferred_avoided_cost_vs_price_grouped_bar.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# 
# Make time series graph of preferred method and care and non-care price for each iou
#
preferred.vs.price.plot.data  <- summary.main.result %>% 
    select(iou, year, preferred, noncare.price, care.price) %>%
    pivot_longer(cols = c(preferred, noncare.price, care.price)) %>%
    mutate(name = factor(name, levels = c("preferred", "noncare.price", "care.price"), labels = c("Primary Marginal Cost", "Non-CARE Price", "CARE Price")),
           iou.factor = factor(iou,
                        levels = c("pge", "sce", "sdge"),
                        labels = c("PG&E", "SCE", "SDG&E")))

preferred.vs.price.plot.data <- left_join(preferred.vs.price.plot.data, cpi) %>%
    mutate(value = value * end.year.cpi / year.cpi)



# All
preferred.vs.price.plot <- ggplot(preferred.vs.price.plot.data, aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    scale_y_continuous(limits = c(0, .3), breaks = seq(0, .3, .1)) +
    theme_minimal() +
    scale_color_manual("", values=c("#F8766D", "#EDAE49", "#619CFF")) + # Keep default colors but change green to yellow
    labs(x = "Year", y = "$/kWh (real $2019)") +
    theme(axis.text=element_text(size=14),
          axis.title=element_text(size=15),
          legend.text=element_text(size=15),
          legend.title=element_text(size=15),
          plot.title=element_text(size=18),
          strip.text = element_text(size=18),
          legend.position = "bottom") +
    facet_wrap(~iou.factor, ncol=1, scale = "free")
preferred.vs.price.plot

aspect.ratio <- 1.4
name <- "Graphs/preferred_avoided_cost_vs_price_time_series.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)


# PG&E
preferred.vs.price.plot.pge <- ggplot(preferred.vs.price.plot.data %>% filter(iou == "pge"), aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    ylim(0, NA) +
    theme_minimal() +
    scale_color_manual("", values=c("#F8766D", "#EDAE49", "#619CFF")) + # Keep default colors but change green to yellow
    labs(x = "Year", y = "$/kWh", title="PG&E") +
    theme(axis.text=element_text(size=11),
          legend.text=element_text(size=11),
          legend.title=element_text(size=13),
          plot.title=element_text(size=15))
preferred.vs.price.plot.pge

aspect.ratio <- .6
name <- "Graphs/preferred_avoided_cost_vs_price_time_series_pge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# SCE
preferred.vs.price.plot.sce <- ggplot(preferred.vs.price.plot.data %>% filter(iou == "sce"), aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    ylim(0, NA) +
    theme_minimal() +
    scale_color_manual("", values=c("#F8766D", "#EDAE49", "#619CFF")) + # Keep default colors but change green to yellow
    labs(x = "Year", y = "$/kWh", title="SCE") +
    theme(axis.text=element_text(size=11),
          legend.text=element_text(size=11),
          legend.title=element_text(size=13),
          plot.title=element_text(size=15))
preferred.vs.price.plot.sce

aspect.ratio <- .6
name <- "Graphs/preferred_avoided_cost_vs_price_time_series_sce.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# SDG&E
preferred.vs.price.plot.sdge <- ggplot(preferred.vs.price.plot.data %>% filter(iou == "sdge"), aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    ylim(0, NA) +
    theme_minimal() +
    scale_color_manual("", values=c("#F8766D", "#EDAE49", "#619CFF")) + # Keep default colors but change green to yellow
    labs(x = "Year", y = "$/kWh", title="SDG&E") +
    theme(axis.text=element_text(size=11),
          legend.text=element_text(size=11),
          legend.title=element_text(size=13),
          plot.title=element_text(size=15))
preferred.vs.price.plot.sdge

aspect.ratio <- .6
name <- "Graphs/preferred_avoided_cost_vs_price_time_series_sdge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# 
# Make time series graph of preferred method and care and non-care price for each iou
#
price.cost.gap.data  <- summary.main.result %>% 
    select(iou, year, preferred, noncare.price, care.price) %>%
    mutate(noncare.price.cost.gap = noncare.price - preferred,
           care.price.cost.gap = care.price - preferred) %>%
    pivot_longer(cols = c(noncare.price.cost.gap, care.price.cost.gap)) %>%
    mutate(name = factor(name, levels = c("noncare.price.cost.gap", "care.price.cost.gap"), labels = c("Non-CARE Price-Cost Gap", "CARE Price-Cost Gap")))

# PG&E
price.cost.gap.pge <- ggplot(price.cost.gap.data %>% filter(iou == "pge"), aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    theme_minimal() +
    scale_color_discrete("") +
    labs(x = "Year", y = "$/kWh", title="PG&E price-cost gap by year")
price.cost.gap.pge

aspect.ratio <- .6
name <- "Graphs/price_cost_gap_time_series_pge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# SCE
price.cost.gap.sce <- ggplot(price.cost.gap.data %>% filter(iou == "sce"), aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    theme_minimal() +
    scale_color_discrete("") +
    labs(x = "Year", y = "$/kWh", title="SCE price-cost gap by year")
price.cost.gap.sce

aspect.ratio <- .6
name <- "Graphs/price_cost_gap_time_series_sce.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# SDG&E
price.cost.gap.sdge <- ggplot(price.cost.gap.data %>% filter(iou == "sdge"), aes(x=year, y=value, group=name, color=name)) +
    geom_line(size = .75) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = 2010:2019) +
    theme_minimal() +
    scale_color_discrete("") +
    labs(x = "Year", y = "$/kWh", title="SDG&E price-cost gap by year")
price.cost.gap.sdge

aspect.ratio <- .6
name <- "Graphs/price_cost_gap_time_series_sdge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# 
# Graph with preferred with components in stacked bar chart
#
components.bar.graph.data <- master.comparison.table.year.iou.load.weighted %>%
    ungroup() %>%
    select(iou, year, mean.ra.gen.capacity, mean.t.capacity, mean.d.capacity, mean.energy, mean.ancillary.services, mean.ghg.adder.50, mean.cap.and.trade, mean.losses.preferred) %>%
    pivot_longer(cols = c(mean.ra.gen.capacity, mean.t.capacity, mean.d.capacity, mean.energy, mean.ancillary.services, mean.ghg.adder.50, mean.cap.and.trade, mean.losses.preferred)) %>%
    mutate(name = factor(name, 
                         levels = c("mean.losses.preferred", "mean.ancillary.services", "mean.cap.and.trade", "mean.ghg.adder.50", "mean.d.capacity", "mean.t.capacity", "mean.ra.gen.capacity", "mean.energy"), 
                         labels = c("Losses", "Ancillary services", "Cap and trade", "GHGs", "Distribution capacity", "Transmission capacity", "Generation capacity", "Energy")),
           iou.factor = factor(iou,
                        levels = c("pge", "sce", "sdge"),
                        labels = c("PG&E", "SCE", "SDG&E")),
           value = value / 1000)

component.bar.graph.data <- left_join(components.bar.graph.data, cpi %>% mutate(year = as.integer(year))) %>%
    mutate(value = value * end.year.cpi / year.cpi)


# Latex table of mdcc and mdtt
out <- component.bar.graph.data %>% 
    filter(name %in% c("Transmission capacity", "Distribution capacity")) %>%
    select(iou.factor, year, name, value) %>%
    pivot_wider(id_cols = c(iou.factor, year))
write_xlsx(out, paste0(dropbox.dir, "Tables/mdcc_mtcc.xlsx"))


p_load(RColorBrewer)
display.brewer.pal(n=8, name = "Set2")
custom.colors <- brewer.pal(n = 8, name = "Set2")
custom.colors[2] <- "#FFFFFF"

# All
avoided.cost.components <- ggplot(components.bar.graph.data, aes(fill = name, y = value, x = year)) +
       geom_bar(color = "black", position = "stack", stat = "identity") +
       scale_x_continuous(breaks = 2010:2019) + 
       scale_y_continuous(breaks = seq(0, .1, .025)) + 
       theme_minimal() +
        labs(x = "Year", y = "$/kWh (real $2019)") +
        scale_fill_manual("", values = custom.colors) +
    theme(axis.text=element_text(size=14),
          axis.title=element_text(size=15),
          legend.text=element_text(size=13),
          legend.title=element_text(size=15),
          plot.title=element_text(size=18),
          strip.text = element_text(size=18),
          legend.position = "bottom") +
    facet_wrap(~iou.factor, ncol=1, scales = "free")
avoided.cost.components

aspect.ratio <- 1.4
name <- "Graphs/avoided_cost_components.png"
ggsave(paste0(dropbox.dir, name), device = "png", width = 10 , height = 10 * aspect.ratio)

# PG&E
avoided.cost.components.pge <- ggplot(components.bar.graph.data %>% filter(iou == "pge"), aes(fill = name, y = value, x = year)) +
       geom_bar(color = "black", position = "stack", stat = "identity") +
       scale_x_continuous(breaks = 2010:2019) + 
       theme_minimal() +
        labs(x = "Year", y = "$/kWh (real $2019)", title="PG&E") +
        scale_fill_brewer("Component", palette = "Set2") +
    theme(axis.text=element_text(size=11),
          legend.text=element_text(size=11),
          legend.title=element_text(size=13),
          plot.title=element_text(size=15))
avoided.cost.components.pge

aspect.ratio <- .6
name <- "Graphs/avoided_cost_components_pge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# SCE
avoided.cost.components.sce <- ggplot(components.bar.graph.data %>% filter(iou == "sce"), aes(fill = name, y = value, x = year)) +
       geom_bar(color = "black", position = "stack", stat = "identity") +
       scale_x_continuous(breaks = 2010:2019) + 
       theme_minimal() +
        labs(x = "Year", y = "$/kWh (real $2019)", title="SCE") +
        scale_fill_brewer("Component", palette = "Set2") +
    theme(axis.text=element_text(size=11),
          legend.text=element_text(size=11),
          legend.title=element_text(size=13),
          plot.title=element_text(size=15))
avoided.cost.components.sce

aspect.ratio <- .6
name <- "Graphs/avoided_cost_components_sce.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# SDG&E
avoided.cost.components.sdge <- ggplot(components.bar.graph.data %>% filter(iou == "sdge"), aes(fill = name, y = value, x = year)) +
       geom_bar(color = "black", position = "stack", stat = "identity") +
       scale_x_continuous(breaks = 2010:2019) + 
       theme_minimal() +
        labs(x = "Year", y = "$/kWh (real $2019)", title="SDG&E") +

        scale_fill_brewer("Component", palette = "Set2") +
    theme(axis.text=element_text(size=11),
          legend.text=element_text(size=11),
          legend.title=element_text(size=13),
          plot.title=element_text(size=15))
avoided.cost.components.sdge

aspect.ratio <- .6
name <- "Graphs/avoided_cost_components_sdge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)


# 
# Waterfall graph
#

rates <- read_rds(paste0(dropbox.dir, "ModifiedData/care_and_noncare_rates_by_iou_and_major_component_for_2019.rds"))

# Get raw avoided cost component. These are not yet adjusted for losses, but the loss factor is in
# the table. 
# We adjust each component for losses
raw.acs <- read_rds(paste0(dropbox.dir, "ModifiedData/master.comparison.table.rds")) %>%
    mutate(ra.gen.capacity = ra.gen.capacity * loss.factor,
           t.capacity = t.capacity * loss.factor,
           d.capacity = d.capacity * loss.factor,
           energy = energy * loss.factor,
           ancillary.services = ancillary.services * loss.factor,
           ghg.50 = ghg.50 * loss.factor,
           cap.and.trade = cap.and.trade * loss.factor) %>%
    filter(!is.na(load.mw)) %>% 
    group_by(iou, year) %>% 
    summarize(
              mean.ra.gen.capacity = weighted.mean(ra.gen.capacity, load.mw), 
              mean.t.capacity = weighted.mean(t.capacity, load.mw, na.rm = T),
              mean.d.capacity = weighted.mean(d.capacity, load.mw, na.rm = T),
              mean.energy = weighted.mean(energy, load.mw, na.rm=T), 
              mean.ancillary.services = weighted.mean(ancillary.services, load.mw, na.rm = T),
              mean.ghg.adder.50 = weighted.mean(ghg.50, load.mw, na.rm =T), 
              mean.cap.and.trade = weighted.mean(cap.and.trade, load.mw, na.rm=T)) %>%
    filter(year == 2019) %>%
    pivot_longer(cols = starts_with("mean"), names_to = "component", values_to = "dollars.per.mw") %>%
    ungroup()

# Here we categorize cap and trade as "G" so we can merge it with the G rate category and subtract marginal cost from total G rate
# We later recategorize it as pollution
acs <- raw.acs %>%
    mutate(cat.minor = case_when(component == "mean.ra.gen.capacity" ~ "G capacity",
                                 component == "mean.t.capacity" ~ "T capacity",
                                 component == "mean.d.capacity" ~ "D capacity",
                                 component == "mean.energy" ~ "Energy",
                                 component == "mean.ancillary.services" ~ "",
                                 component == "mean.ghg.adder.50" ~ "Non-mkt GHGs",
                                 component == "mean.cap.and.trade" ~ "Cap and trade"),
           cat.major = case_when(component %in% c("mean.ra.gen.capacity", "mean.energy", "mean.ancillary.services", "mean.cap.and.trade") ~ "G",
                                 component %in% c("mean.t.capacity") ~ "T",
                                 component %in% c("mean.d.capacity") ~ "D",
                                 component %in% c("mean.ghg.adder.50") ~ "Pollution"))

# Aggregate acs to major categories so we can get residual from the rates data
acs.cat.major.aggregates <- acs %>% 
    group_by(iou, cat.major) %>%
    summarize(ac.dollars.per.kwh = sum(dollars.per.mw / 1000)) %>%
    ungroup()

# Join
rates <- left_join(rates, acs.cat.major.aggregates)
rates <- rates %>%
    group_by(iou) %>%
    mutate(total.res.noncare.rate = sum(noncare.rate),
           noncare.fc = if_else(is.na(ac.dollars.per.kwh),
                            noncare.rate,
                            noncare.rate - ac.dollars.per.kwh),
           care.fc = if_else(is.na(ac.dollars.per.kwh),
                            care.rate,
                            care.rate - ac.dollars.per.kwh)
           ) %>%
    select(iou, cat.major, total.res.noncare.rate, noncare.fc, care.fc) %>%
    ungroup()

# Add minor category
rates <- rates %>% 
    mutate(cat.minor = case_when(cat.major == "D" ~ "D fixed costs", 
                                 cat.major == "G" ~ "G fixed costs",
                                 cat.major == "PPP/Other" ~ "PPP/Other",
                                 cat.major == "T" ~ "T fixed costs"))

# Create total above the line
rates  <- rates %>% 
    group_by(iou) %>%
    mutate(total.above.the.line = sum(noncare.fc)) %>%
    ungroup()

# Grab care kwh
care.kwh <- read_excel(paste0(dropbox.dir, "OriginalData/care_reports/noncare_and_care_prices_by_iou_year.xlsx")) %>%
    filter(year == 2019)

# Grab number of res customers
ferc.f1 <- read_rds(paste0(dropbox.dir, "ModifiedData/ferc.f1.v2.clean.rds")) %>% 
    filter(ca.iou == T, year == 2019) %>%
    mutate(iou = case_when(utility == "pacific gas and electric company" ~ "pge",
                                 utility == "san diego gas & electric company" ~ "sdge",
                                 utility == "southern california edison company" ~ "sce")) %>%
    select(iou, total.res.customers)

kwh.by.care.status <- left_join(care.kwh, ferc.f1) %>%
    mutate(noncare.kwh.annual = noncare.kwh * 12 * (total.res.customers - care.customers),
           care.kwh.annual = care.kwh * 12 * care.customers) %>%
    select(iou, noncare.kwh.annual, care.kwh.annual)

# Save for Severin
write_xlsx(kwh.by.care.status, paste0(dropbox.dir, "Tables/care_and_noncare_2019_consumption_by_iou.xlsx"))

# Bring in PV gen
pv.gen <- read_csv(paste0(dropbox.dir, "ModifiedData/", "pv.prod.by.utility.year.csv")) %>%
    filter(panel.year == 2019) %>%
    mutate(annual.pv.gen.kwh = annual.gen.gwh * 1e6) %>%
    select(iou, annual.pv.gen.kwh)

# Bring in system kWh
c <- read_csv(paste0(dropbox.dir, "OriginalData/consumption_by_utility_year_sector/", "ElectricityByUtility.csv")) %>%
    rename(sys.gwh = "Total Usage", res.gwh = "Residential", utility = "Utility Name", year = Year) %>% 
    mutate(iou = if_else(utility == "Pacific Gas and Electric Company", "pge", if_else(utility == "San Diego Gas and Electric Company", "sdge", "sce")),
           sys.kwh = sys.gwh * 1e6) %>%
    filter(year == 2019) %>%
    select(iou, sys.kwh)

# Merge with rates
rates <- left_join(rates, kwh.by.care.status) %>%
    left_join(., pv.gen) %>%
    left_join(., c)

rates <- rates %>% 
    group_by(iou) %>%
    mutate(noncare.share.fc = noncare.kwh.annual / (sys.kwh - care.kwh.annual),
           nocare.fc = (noncare.fc * noncare.kwh.annual + care.fc * noncare.share.fc * care.kwh.annual) / (noncare.kwh.annual + noncare.share.fc * care.kwh.annual),
           # We have to use a different formula to find PP FC because CARE charge is already in PPP
           # First compute non-PPP care rate and nocare rate totals
           nonppp.care.fc = sum(care.fc * (cat.major != "PPP/Other")),
           nonppp.nocare.fc = sum(nocare.fc * (cat.major != "PPP/Other")),
           nocare.fc = if_else(
                                cat.major == "PPP/Other",
                                (noncare.share.fc * (nonppp.care.fc - nonppp.nocare.fc) * care.kwh.annual + noncare.fc * noncare.kwh.annual +  noncare.share.fc * care.fc * care.kwh.annual) / (noncare.kwh.annual + 2 * noncare.share.fc * care.kwh.annual),
                                nocare.fc),
           # Compute NEM subsidy
           nem.subsidy.per.component = nocare.fc * (annual.pv.gen.kwh / (care.kwh.annual + noncare.kwh.annual + annual.pv.gen.kwh)),
           # CARE is in the PPP box so subtract the true no PPP FC we just computed
           care.subsidy = (noncare.fc - nocare.fc) * (cat.major == "PPP/Other"),
           nocare.nonem.fc = noncare.fc - care.subsidy - nem.subsidy.per.component,
           care.subsidy = sum(care.subsidy),
           nem.subsidy = sum(nem.subsidy.per.component)
           ) %>%
    ungroup() %>%
    mutate(height = nocare.nonem.fc)

# Add CARE box
care.box.rows <- rates %>%
    filter(cat.major == "PPP/Other") %>%
    mutate(height = care.subsidy,
           cat.minor = "CARE")

# Add NEM box
nem.box.rows <- rates %>%
    filter(cat.major == "PPP/Other") %>%
    mutate(height = nem.subsidy,
           cat.minor = "BTM PV")

rates <- bind_rows(rates, nem.box.rows, care.box.rows) %>%
    arrange(iou) %>%
    select(iou, cat.major, cat.minor, total.res.noncare.rate, height)
# Save for exercise
# write_xlsx(rates, paste0(dropbox.dir, "Tables/waterfall_above_the_line_data.xlsx"))

# Make cap and trade's major category "Pollution" rather than G
acs <- acs %>%
    mutate(cat.major = if_else(cat.minor == "Cap and trade", "Pollution", cat.major),
           height = dollars.per.mw / 1000) %>%
    select(-component, -dollars.per.mw, -year)

waterfall.data.avoidable <- acs %>%
  # \_Set the factor levels in the order you want ----
  mutate(x.axis.var = factor(cat.major, levels = c("G", "T", "D", "Pollution")), 
         cat.var = factor(cat.minor)) %>%
    group_by(iou) %>%
  # \_Sort by Group
  arrange(x.axis.var, cat.var) %>%
  # \_Get the start and end points of the bars ----
  mutate(end.bar = cumsum(height),
         start.bar = c(0, head(end.bar, -1))) %>%
  ungroup()

waterfall.data.non.avoidable <- rates %>%
  # \_Set the factor levels in the order you want ----
  mutate( x.axis.var = factor(cat.major, levels = c("G", "T", "D", "PPP/Other"), labels = c("G", "T", "D", "PPP/Other")), 
         cat.var = factor(cat.minor)) %>%
    group_by(iou) %>%
  # \_Sort by Group
  arrange(x.axis.var, cat.var) %>%
  # \_Get the start and end points of the bars ----
  mutate(start.bar = total.res.noncare.rate - cumsum(height),
         end.bar = c(total.res.noncare.rate[1], head(start.bar, -1))) %>%
  ungroup()

waterfall.data <- bind_rows(waterfall.data.avoidable, waterfall.data.non.avoidable) %>%
  # \_Get numeric index for the groups ----
  mutate(x.axis.var = factor(x.axis.var, levels = c("G", "T", "D", "Pollution", "PPP/Other"), labels = c("Generation", "Transmission", "Distribution", "Pollution", "Public Purpose Programs and Other"))) %>%
  mutate(group.id = group_indices(., x.axis.var),
         shading.var = if_else(cat.var %in% c("CARE", "BTM PV"), "Transfer", as.character(x.axis.var))) %>%
  # \_Order the columns ----
  select(iou, x.axis.var, shading.var, cat.var, group.id, start.bar, height, end.bar) %>%
  arrange(iou, x.axis.var, cat.var)

# Output excel of waterfall data
write_xlsx(waterfall.data, paste0(dropbox.dir, "Tables/waterfall_data.xlsx"))


# remotes::install_github("coolbutuseless/ggpattern")
p_load(ggpattern)


# 
# PG&E
#

waterfall.data.pge  <- waterfall.data %>% filter(iou == "pge")

label.data.pge <- data.frame(
  group.id = c(0, 0), 
  y = c(as.numeric(waterfall.data.pge %>% filter(cat.var == "Cap and trade") %>% select(end.bar)), as.numeric(waterfall.data.pge %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar))),
  label = c("Private avoided cost", "Social avoided cost")
)

waterfall.pge <- ggplot(waterfall.data.pge, aes(x = group.id -0.5, fill = shading.var)) + 
  # Draw rectangles, all but GHGs which we do striped
  geom_rect(data = waterfall.data.pge %>% filter(cat.var != "Non-mkt GHGs"), aes(x = group.id - 0.5,
                xmin = group.id - 1, # control bar gap width
                xmax = group.id, 
                ymin = end.bar,
                ymax = start.bar),
            color="black", 
            alpha=0.95)  +
    # Striped GHGs
  geom_rect_pattern(data = waterfall.data.pge %>% filter(cat.var == "Non-mkt GHGs"), aes(x = group.id - 0.5,
                xmin = group.id - 1, # control bar gap width
                xmax = group.id, 
                ymin = end.bar,
                ymax = start.bar),
            pattern = 'stripe',
            pattern_fill = 'white',
            pattern_color = 'white',
            color="black", 
            alpha=0.95)  +
  # Label for each sub cat in bar
  geom_text(
    mapping = 
      aes(
        label = cat.var,
        y = (end.bar + start.bar) / 2
      ),
    color = "#4e4d47",
    fontface = "bold"
  ) +
  # Lines for private and social avoided cost
  geom_segment(aes(x=0, 
                   y= as.numeric(waterfall.data.pge %>% filter(cat.var == "Cap and trade") %>% select(end.bar)),
                   xend=4, 
                   yend= as.numeric(waterfall.data.pge %>% filter(cat.var == "Cap and trade") %>% select(end.bar))),
               colour="black") +
    # Line from bottom of D fixed cost to top of care
  geom_segment(aes(x=3, 
                   y= as.numeric(waterfall.data.pge %>% filter(cat.var == "BTM PV") %>% select(end.bar)),
                   xend=4, 
                   yend= as.numeric(waterfall.data.pge %>% filter(cat.var == "BTM PV") %>% select(end.bar))),
               colour="black") +
#  geom_text(aes(x=0.2, y=as.numeric(waterfall.data.pge %>% filter(cat.var == "Cap and trade") %>% select(end.bar)), label = "PMC")) +
  geom_segment(aes(x=0, 
                   y= as.numeric(waterfall.data.pge %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar)),
                   xend=4, 
                   yend= as.numeric(waterfall.data.pge %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar))),
               colour="black") +
      # \_Change colors ----
    scale_fill_brewer(palette = "Pastel1") +
      # \_Change y axis to same scale as original ----
      scale_y_continuous(
        expand=c(0,0),
        limits = c(0, .3),
        breaks = c(seq(0, .3, .05), as.numeric(waterfall.data.pge %>% filter(cat.var == "Cap and trade") %>% select(end.bar)), as.numeric(waterfall.data.pge %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar))),
        labels = c(seq(0, .3, .05), "PMC", "SMC")
      ) +
      # \_Add tick marks on x axis to look like the original plot ----
      scale_x_continuous(
        expand=c(0,0),
        limits = c(min(waterfall.data.pge$group.id)-1,max(waterfall.data.pge$group.id)),
        breaks = c(min(waterfall.data.pge$group.id)-1,
                   unique(waterfall.data.pge$group.id) - 0.5, 
                   unique(waterfall.data.pge$group.id)
                   ),
        labels = 
          str_wrap(c("", 
            as.character(unique(waterfall.data.pge$x.axis.var)), 
            rep(c(""), length(unique(waterfall.data.pge$x.axis.var)))), width = 15)
      ) +
      # \_Theme options to make it look like the original plot ----
      theme(
            legend.position = "none",
        text = element_text(size = 14, color = "#4e4d47"),
        axis.text.x = element_text(size = 14, color = "#4e4d47"),
        axis.text.y = element_text(size = 15, margin = margin(r = 0.3, unit = "cm")),
        axis.ticks.x =
          element_line(color =
                         c("black",
                           rep(NA, length(unique(waterfall.data.pge$x.axis.var))),
                           rep("black", length(unique(waterfall.data.pge$x.axis.var))-1)
                         )
                       ),
        axis.line = element_line(colour = "#4e4d47", size = 0.5),
        axis.ticks.length = unit(.15, "cm"),
        axis.title.x =       element_blank(),
        panel.background =   element_blank(),
        panel.grid.major.y = element_line(color = "grey90"),
        plot.margin =        unit(c(1, 1, 1, 1), "lines"),
      ) +
    labs(y = "$/kWh") +
    annotate("segment", x = 1.15, xend = 0.85, y = 0.0125, yend = as.numeric(waterfall.data.pge %>% filter(cat.var == "") %>% select(end.bar)) / 2, colour = "black", size = 1, alpha=0.75, arrow=arrow(angle = 15, length = unit(0.1, "inches"), type = "closed")) +
  geom_text(
      aes(x = 1.5,
          y = 0.0125,
        label = "Ancillary services",
      ),
    color = "#4e4d47",
    fontface = "bold"
  ) +
    ggtitle("PG&E")
waterfall.pge

aspect.ratio <- .6
name <- "Graphs/waterfall_pge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# 
# SCE
#

waterfall.data.sce  <- waterfall.data %>% filter(iou == "sce")

label.data.sce <- data.frame(
  group.id = c(0, 0), 
  y = c(as.numeric(waterfall.data.sce %>% filter(cat.var == "Cap and trade") %>% select(end.bar)), as.numeric(waterfall.data.sce %>% filter(cat.var == "GHGs") %>% select(end.bar))),
  label = c("Private avoided cost", "Social avoided cost")
)

waterfall.sce <- ggplot(waterfall.data.sce, aes(x = group.id -0.5, fill = shading.var)) + 
  # Draw rectangles, all but GHGs which we do striped
  geom_rect(data = waterfall.data.sce %>% filter(cat.var != "Non-mkt GHGs"), aes(x = group.id - 0.5,
                xmin = group.id - 1, # control bar gap width
                xmax = group.id, 
                ymin = end.bar,
                ymax = start.bar),
            color="black", 
            alpha=0.95)  +
    # Striped GHGs
  geom_rect_pattern(data = waterfall.data.sce %>% filter(cat.var == "Non-mkt GHGs"), aes(x = group.id - 0.5,
                xmin = group.id - 1, # control bar gap width
                xmax = group.id, 
                ymin = end.bar,
                ymax = start.bar),
            pattern = 'stripe',
            pattern_fill = 'white',
            pattern_color = 'white',
            color="black", 
            alpha=0.95)  +
  # \_Label for each sub cat in bar
  geom_text(
    mapping = 
      aes(
        label = cat.var,
        y = (end.bar + start.bar) / 2
      ),
    color = "#4e4d47",
    fontface = "bold"
  ) +
  # \_Lines for private and social avoided cost
  geom_segment(aes(x=0, 
                   y= as.numeric(waterfall.data.sce %>% filter(cat.var == "Cap and trade") %>% select(end.bar)),
                   xend=4, 
                   yend= as.numeric(waterfall.data.sce %>% filter(cat.var == "Cap and trade") %>% select(end.bar))),
               colour="black") +
    # Line from bottom of D fixed cost to top of care
  geom_segment(aes(x=3, 
                   y= as.numeric(waterfall.data.sce %>% filter(cat.var == "BTM PV") %>% select(end.bar)),
                   xend=4, 
                   yend= as.numeric(waterfall.data.sce %>% filter(cat.var == "BTM PV") %>% select(end.bar))),
               colour="black") +
#  geom_text(aes(x=0.2, y=as.numeric(waterfall.data.sce %>% filter(cat.var == "Cap and trade") %>% select(end.bar)), label = "PMC")) +
  geom_segment(aes(x=0, 
                   y= as.numeric(waterfall.data.sce %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar)),
                   xend=4, 
                   yend= as.numeric(waterfall.data.sce %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar))),
               colour="black") +
      # \_Change colors ----
    scale_fill_brewer(palette = "Pastel1") +
      # \_Change y axis to same scale as original ----
      scale_y_continuous(
        expand=c(0,0),
        limits = c(0, .3),
        breaks = c(seq(0, .3, .05), as.numeric(waterfall.data.sce %>% filter(cat.var == "Cap and trade") %>% select(end.bar)), as.numeric(waterfall.data.sce %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar))),
        labels = c(seq(0, .3, .05), "PMC", "SMC")
      ) +
      # \_Add tick marks on x axis to look like the original plot ----
      scale_x_continuous(
        expand=c(0,0),
        limits = c(min(waterfall.data.sce$group.id)-1,max(waterfall.data.sce$group.id)),
        breaks = c(min(waterfall.data.sce$group.id)-1,
                   unique(waterfall.data.sce$group.id) - 0.5, 
                   unique(waterfall.data.sce$group.id)
                   ),
        labels = 
          str_wrap(c("", 
            as.character(unique(waterfall.data.sce$x.axis.var)), 
            rep(c(""), length(unique(waterfall.data.sce$x.axis.var)))), width = 15)
      ) +
      # \_Theme options to make it look like the original plot ----
      theme(
            legend.position = "none",
        text = element_text(size = 14, color = "#4e4d47"),
        axis.text.x = element_text(size = 14, color = "#4e4d47"),
        axis.text.y = element_text(size = 15, margin = margin(r = 0.3, unit = "cm")),
        axis.title.y = element_text(size = 15),
        axis.ticks.x =
          element_line(color =
                         c("black",
                           rep(NA, length(unique(waterfall.data.sce$x.axis.var))),
                           rep("black", length(unique(waterfall.data.sce$x.axis.var))-1)
                         )
                       ),
        axis.line = element_line(colour = "#4e4d47", size = 0.5),
        axis.ticks.length = unit(.15, "cm"),
        axis.title.x =       element_blank(),
        panel.background =   element_blank(),
        panel.grid.major.y = element_line(color = "grey90"),
        plot.margin =        unit(c(1, 1, 1, 1), "lines"),
      ) +
    labs(y = "$/kWh") +
    annotate("segment", x = 1.15, xend = 0.85, y = 0.0125, yend = as.numeric(waterfall.data.sce %>% filter(cat.var == "") %>% select(end.bar)) / 2, colour = "black", size = 1, alpha=0.75, arrow=arrow(angle = 15, length = unit(0.1, "inches"), type = "closed")) +
  geom_text(
      aes(x = 1.5,
          y = 0.0125,
        label = "Ancillary services",
      ),
    color = "#4e4d47",
    fontface = "bold"
  ) +
ggtitle("SCE")
waterfall.sce

aspect.ratio <- .6
name <- "Graphs/waterfall_sce.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# 
# SDG&E
#

waterfall.data.sdge  <- waterfall.data %>% filter(iou == "sdge")

label.data.sdge <- data.frame(
  group.id = c(0, 0), 
  y = c(as.numeric(waterfall.data.sdge %>% filter(cat.var == "Cap and trade") %>% select(end.bar)), as.numeric(waterfall.data.sdge %>% filter(cat.var == "GHGs") %>% select(end.bar))),
  label = c("Private avoided cost", "Social avoided cost")
)

waterfall.sdge <- ggplot(waterfall.data.sdge, aes(x = group.id -0.5, fill = shading.var)) + 
  # Draw rectangles, all but GHGs which we do striped
  geom_rect(data = waterfall.data.sdge %>% filter(cat.var != "Non-mkt GHGs"), aes(x = group.id - 0.5,
                xmin = group.id - 1, # control bar gap width
                xmax = group.id, 
                ymin = end.bar,
                ymax = start.bar),
            color="black", 
            alpha=0.95)  +
    # Striped GHGs
  geom_rect_pattern(data = waterfall.data.sdge %>% filter(cat.var == "Non-mkt GHGs"), aes(x = group.id - 0.5,
                xmin = group.id - 1, # control bar gap width
                xmax = group.id, 
                ymin = end.bar,
                ymax = start.bar),
            pattern = 'stripe',
            pattern_fill = 'white',
            pattern_color = 'white',
            color="black", 
            alpha=0.95)  +
  # \_Label for each sub cat in bar
  geom_text(
    mapping = 
      aes(
        label = cat.var,
        y = (end.bar + start.bar) / 2
      ),
    color = "#4e4d47",
    fontface = "bold"
  ) +
  # \_Lines for private and social avoided cost
  geom_segment(aes(x=0, 
                   y= as.numeric(waterfall.data.sdge %>% filter(cat.var == "Cap and trade") %>% select(end.bar)),
                   xend=4, 
                   yend= as.numeric(waterfall.data.sdge %>% filter(cat.var == "Cap and trade") %>% select(end.bar))),
               colour="black") +
    # Line from bottom of D fixed cost to top of care
  geom_segment(aes(x=3, 
                   y= as.numeric(waterfall.data.sdge %>% filter(cat.var == "BTM PV") %>% select(end.bar)),
                   xend=4, 
                   yend= as.numeric(waterfall.data.sdge %>% filter(cat.var == "BTM PV") %>% select(end.bar))),
               colour="black") +
#  geom_text(aes(x=0.2, y=as.numeric(waterfall.data.sdge %>% filter(cat.var == "Cap and trade") %>% select(end.bar)), label = "PMC")) +
  geom_segment(aes(x=0, 
                   y= as.numeric(waterfall.data.sdge %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar)),
                   xend=4, 
                   yend= as.numeric(waterfall.data.sdge %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar))),
               colour="black") +
      # \_Change colors ----
    scale_fill_brewer(palette = "Pastel1") +
      # \_Change y axis to same scale as original ----
      scale_y_continuous(
        expand=c(0,0),
        limits = c(0, .3),
        breaks = c(seq(0, .3, .05), as.numeric(waterfall.data.sdge %>% filter(cat.var == "Cap and trade") %>% select(end.bar)), as.numeric(waterfall.data.sdge %>% filter(cat.var == "Non-mkt GHGs") %>% select(end.bar))),
        labels = c(seq(0, .3, .05), "PMC", "SMC")
      ) +
      # \_Add tick marks on x axis to look like the original plot ----
      scale_x_continuous(
        expand=c(0,0),
        limits = c(min(waterfall.data.sdge$group.id)-1,max(waterfall.data.sdge$group.id)),
        breaks = c(min(waterfall.data.sdge$group.id)-1,
                   unique(waterfall.data.sdge$group.id) - 0.5, 
                   unique(waterfall.data.sdge$group.id)
                   ),
        labels = 
          str_wrap(c("", 
            as.character(unique(waterfall.data.sdge$x.axis.var)), 
            rep(c(""), length(unique(waterfall.data.sdge$x.axis.var)))), width = 15)
      ) +
      # \_Theme options to make it look like the original plot ----
      theme(
            legend.position = "none",
        text = element_text(size = 14),
        axis.text.x = element_text(size = 14),
        axis.text.y = element_text(size = 15, margin = margin(r = 0.3, unit = "cm")),
        axis.title.y = element_text(size = 15),
        axis.ticks.x =
          element_line(color =
                         c("black",
                           rep(NA, length(unique(waterfall.data.sdge$x.axis.var))),
                           rep("black", length(unique(waterfall.data.sdge$x.axis.var))-1)
                         )
                       ),
        axis.line = element_line(colour = "#4e4d47", size = 0.5),
        axis.ticks.length = unit(.15, "cm"),
        axis.title.x =       element_blank(),
        panel.background =   element_blank(),
        panel.grid.major.y = element_line(color = "grey90"),
        plot.margin =        unit(c(1, 1, 1, 1), "lines"),
      ) +
    labs(y = "$/kWh") +
    annotate("segment", x = 1.15, xend = 0.85, y = 0.0125, yend = as.numeric(waterfall.data.sdge %>% filter(cat.var == "") %>% select(end.bar)) / 2, colour = "black", size = 1, alpha=0.75, arrow=arrow(angle = 15, length = unit(0.1, "inches"), type = "closed")) +
  geom_text(
      aes(x = 1.5,
          y = 0.0125,
        label = "Ancillary services",
      ),
    color = "#4e4d47",
    fontface = "bold"
  ) +
    ggtitle("SDG&E")
waterfall.sdge

aspect.ratio <- .6
name <- "Graphs/waterfall_sdge.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

p_load(patchwork)

waterfall.all <- waterfall.pge+waterfall.sce+waterfall.sdge+plot_layout(ncol = 1)

aspect.ratio <- 1.6
name <- "Graphs/waterfall_all.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

# 
# Cost-shift analysis
#

# 
# Load revenue sources and make graph
#
revenues <- read_excel(paste0(dropbox.dir, "OriginalData/Census/StateTaxRevenuePieChart.xlsx"), range = "Sheet1!A6:C11") %>%
    rename(amt.2018 = starts_with("Sources"), amt.2019 = "...3") %>%
    mutate(cat = factor(cat, levels = c("Income tax", "Sales and use taxes", "Corporate taxes", "Motor vehicle excise taxes", "Other")))

pie <- ggplot(revenues, aes (x="", y = -amt.2019, fill = cat)) + 
  geom_bar(width = 1, stat = "identity") +
  geom_text(aes(label = paste0(round(amt.2019 / sum(amt.2019) * 100, 1), "%"), x = 1.3),
            position = position_stack(vjust = 0.5),
            fontface = "bold",
            size = 7) +
  theme_void() +
  theme(plot.title = element_text(hjust=0.5),
        axis.line = element_blank(),
        axis.text = element_blank(),
        axis.ticks = element_blank(),
        legend.text=element_text(size=20),
        legend.position = "top",
        legend.box.margin = margin(2, 2)) +
  labs(fill = "",
       x = NULL,
       y = NULL) +
  coord_polar("y") +
  scale_fill_brewer(palette = "Set2") +
    theme() +
    guides(fill=guide_legend(nrow=2,byrow=TRUE))
pie
aspect.ratio <- .8

name <- "Graphs/revenue_sources_pie.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

#
# Load income tables
# Just grabbed these from here https://www.bls.gov/cex/2017/research/income-ca.htm
#
spending <- read_excel(paste0(dropbox.dir, "OriginalData/SalesTax/CEX_TaxableFirstPass.xlsx"))

spending <- spending %>% 
    filter(sales.taxable == 1 | cat %in% c("Income before taxes", "Electricity", "Average annual expenditures", "Gasoline, other fuels, and motor oil")) %>%
    select(-notes)

spending.non.electricity.taxable  <- spending %>% 
    filter(sales.taxable == 1) %>%
    select(-sales.taxable) %>%
    summarize(total = sum(total),
              quintile.1 = sum(as.numeric(quintile.1)),
              quintile.2 = sum(quintile.2),
              quintile.3 = sum(quintile.3),
              quintile.4 = sum(quintile.4),
              quintile.5 = sum(quintile.5))

spending.other.cats <- spending %>%
    filter(cat %in% c("Income before taxes", "Electricity", "Average annual expenditures", "Gasoline, other fuels, and motor oil")) %>%
    select(-sales.taxable) %>%
    mutate(quintile.1 = as.numeric(quintile.1))

spending.clean <- tibble(quintile = c(1:5),
                         income = t(spending.other.cats %>% select(-cat, -total))[,1],
                         all = t(spending.other.cats %>% select(-cat, -total))[,2],
                         electricity = t(spending.other.cats %>% select(-cat, -total))[,3],
                         gasoline = t(spending.other.cats %>% select(-cat, -total))[,4],
                         non.electricity.taxable = t(spending.non.electricity.taxable %>% select(-total))[,1])

spending.clean <- spending.clean %>%
    mutate(non.electricity.all = all - electricity,
           income.relative = income / min(income),
           electricity.relative.spending = (electricity) / min(electricity),
           gasoline.relative.spending = (gasoline) / min(gasoline),
           non.electricity.all.relative.spending = (non.electricity.all) / min(non.electricity.all),
           non.electricity.taxable.relative.spending = (non.electricity.taxable) / min(non.electricity.taxable))

# output for Jim
write_xlsx(spending.clean, paste0(dropbox.dir, "Tables/cex_data_for_graph_cleaned.xlsx"))

income.n.spending.graph.data <- spending.clean %>%
    select(quintile, income.relative, electricity.relative.spending, gasoline.relative.spending, non.electricity.all.relative.spending, non.electricity.taxable.relative.spending) %>%
    pivot_longer(cols = c(income.relative, electricity.relative.spending, gasoline.relative.spending, non.electricity.all.relative.spending, non.electricity.taxable.relative.spending)) %>%
    mutate(name = factor(name, levels = c("electricity.relative.spending", "gasoline.relative.spending", "non.electricity.all.relative.spending", "non.electricity.taxable.relative.spending", "income.relative"), labels = c("Electricity expenditure", "Gasoline expenditure", "All expenditures except electricity", "All expenditures subject to sales tax", "Income")))


p_load(RColorBrewer)
my.colors <- brewer.pal(5,"Set1")
names(my.colors) <- levels(income.n.spending.graph.data$name)
col.scale <- scale_colour_manual(name = "",values = my.colors)

# Make plot with relative spending
relative.income.n.spending <- ggplot(income.n.spending.graph.data, aes(x=quintile, y=value, group = name)) +
  geom_line(aes(color=factor(name)), size = .75) +
  geom_point(aes(color=factor(name)), size = 2) +
  theme_minimal() + labs(x = "Income quintile", y = "Income/expenditure relative to 1st income quintile") +
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=15),
        legend.text=element_text(size=14),
        plot.title = element_text(size=15)) +
    col.scale
relative.income.n.spending
aspect.ratio <- .6

name <- "Graphs/cex_ca_relative_income_and_expenditure.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)


spending.graph.data <- spending.clean %>%
    select(quintile, electricity.relative.spending, gasoline.relative.spending, non.electricity.all.relative.spending, non.electricity.taxable.relative.spending) %>%
    pivot_longer(cols = c(electricity.relative.spending, gasoline.relative.spending, non.electricity.all.relative.spending, non.electricity.taxable.relative.spending)) %>%
    mutate(name = factor(name, levels = c("electricity.relative.spending", "gasoline.relative.spending", "non.electricity.all.relative.spending", "non.electricity.taxable.relative.spending"), labels = c("Electricity expenditure", "Gasoline expenditure", "All expenditures except electricity", "All expenditures subject to sales tax")))

# Make plot with relative spending
relative.spending <- ggplot(income.n.spending.graph.data %>% filter(name != "Income"), aes(x=quintile, y=value, group = name)) +
  geom_line(aes(color=factor(name)), size = 0.75) +
  geom_point(aes(color=factor(name)), size = 2) +
  theme_minimal() + labs(x = "Income quintile", y = "Expenditure relative to 1st income quintile") + 
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=15),
        legend.text=element_text(size=14),
        plot.title = element_text(size=15)) +
    col.scale
relative.spending
aspect.ratio <- .6

name <- "Graphs/cex_ca_relative_expenditure.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

income.graph.data <- spending.clean %>%
    select(quintile, income.relative) %>%
    pivot_longer(cols = c(income.relative)) %>%
    mutate(name = factor(name, levels = c("income.relative"), labels = c("Income")))

# Make plot with relative spending
income.graph <- ggplot(income.graph.data, aes(x=quintile, y=value, group = name)) +
  geom_line(aes(color=factor(name))) +
  geom_point(aes(color=factor(name))) +
  scale_color_discrete("") +
  theme_minimal() + labs(x = "Income quintile", y = "Income relative to 1st income quintile", title = "CEX 2017-2018 avg. income for CA relative to 1st income quintile")
income.graph
aspect.ratio <- 1

name <- "Graphs/cex.ca.relative.income.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 7 * aspect.ratio , width = 7)

iou.territory.income.distribution.analysis <- function(run = F) { 
    if (run == T) {
        p_load(sf, tigris)

        # 
        # Grab income distribution
        #
        p_load(tidycensus)
        v15 <- load_variables(2018, "acs5", cache = TRUE) #
        View(v15[grep("INCOME", v15$concept),])
        View(v15[grep("B19001", v15$name),])

        census.api.key <- "6d8d7e4ad5be0123f2d81212f797b50b623e4f17"

        # Just household income and population for now
        income.dist.vars <- c("B00001_001", # Unweighted sample count of pop
                         "B00002_001", # Unweighted sample count of houses
                         "B19001_001", # Total, household income
                         "B19001_002", # Total, household income
                         "B19001_003", # Total, household income
                         "B19001_004", # Total, household income
                         "B19001_005", # Total, household income
                         "B19001_006", # Total, household income
                         "B19001_007", # Total, household income
                         "B19001_008", # Total, household income
                         "B19001_009", # Total, household income
                         "B19001_010", # Total, household income
                         "B19001_011", # Total, household income
                         "B19001_012", # Total, household income
                         "B19001_013", # Total, household income
                         "B19001_014", # Total, household income
                         "B19001_015", # Total, household income
                         "B19001_016", # Total, household income
                         "B19001_017")

        income.dist.acs <-get_acs(geography = "block group", state = "CA", county = NULL, year = 2018,
                          variables = income.dist.vars, keep_geo_vars = T, key = census.api.key, output = "wide")

        income.dist.acs <- income.dist.acs %>% mutate(block.group.fips = str_sub(GEOID, 1, 12))

        income.dist.acs <- income.dist.acs %>% 
            select(-starts_with("B0")) %>% 
            pivot_longer(cols = starts_with("B19001"), names_pattern = "^(B19001_0[0-9]{2})([EM])$", names_to = c("name", ".value"))


        # Use a 90% CI for margin of error, so we just divide
        # by t stat to get back the se
        census.t.stat <- 1.645

        labels <- v15[grep("^B19001_0[0-9]{2}$", v15$name),] %>% 
            select(name, label) %>%
            mutate(label = str_replace(label, "Estimate!!Total", "")) %>%
            mutate(label = str_replace(label, "!!", ""),
                   label = if_else(label == "", "Total", label))
        labels <- labels %>%
            mutate(bucket = 0:(nrow(labels)-1))

        income.dist.acs <- inner_join(income.dist.acs, labels)

        # Get se
        income.dist.acs <- income.dist.acs %>%
            mutate(estimate = E, se = M / census.t.stat) %>%
            rename(cbg = GEOID) %>%
            select(cbg, estimate, se, label)

        # Save
        write_rds(income.dist.acs, paste0(dropbox.dir, "ModifiedData/income.dist.by.cbg.rds"))


        # Get number of households across income bins in IOU territory
#
        cbgs <- block_groups(state = "CA", year = 2019, cb=TRUE)

        # 
        # Load shapefiles for ioiu boundary and match get cbg locations
        #

        # This has IOUs, munis, irrigation districts, CCAs
        utilities <- st_read(dsn = paste0(dropbox.dir, "OriginalData/California_Electric_Utility_Service_Areas-shp/", "c8bc4098-866b-455f-903d-f677dc2542212020330-1-oi9jv4.funsk.shp"))

        # Get same datum as cbgs
        utilities <- st_transform(utilities, crs = st_crs(cbgs))

        # Get intersection
        intersect <-  st_intersects(cbgs, utilities, sparse=F)

        # Grab lists of CBGs by IOU
        iou.indices <- which(utilities$Utility %in% c("Southern California Edison", "Pacific Gas & Electric Company", "San Diego Gas & Electric"))

        # Now get which cbgs cross which territories
        cbg.names <- as.tibble(cbgs$GEOID) %>%
            rename(cbg = value)
        iou.cbgs <- intersect[,iou.indices]
        iou.cbgs <- bind_rows(cbg.names[iou.cbgs[,1],] %>% mutate(iou = "sce"),
                  cbg.names[iou.cbgs[,2],] %>% mutate(iou = "pge"),
                  cbg.names[iou.cbgs[,3],] %>% mutate(iou = "sdge"))

        # Save
        write_rds(iou.cbgs, paste0(dropbox.dir, "ModifiedData/iou.cbgs.rds"))

        income.dist <- read_rds(paste0(dropbox.dir, "ModifiedData/income.dist.by.cbg.rds"))

        iou.cbg.income.bucket.level <- inner_join(iou.cbgs, income.dist)

        income.bucket.iou.level <- iou.cbg.income.bucket.level %>%
            group_by(iou, label, bucket) %>%
            summarize(tot.households = sum(estimate)) %>%
            ungroup() %>%
            arrange(iou, bucket)

        # Get households in thousands
        income.bucket.iou.level <- income.bucket.iou.level %>%
            mutate(tot.households.thousands = tot.households / 1000)

        labels <- income.bucket.iou.level %>%
            filter(bucket != 0 & iou == "pge") %>%
            select(label) %>%
            as_vector()
        ggplot(income.bucket.iou.level %>% filter(bucket != 0), aes(x = bucket, y = tot.households.thousands, fill = iou)) +
          geom_col(position = position_dodge()) +
          labs(
            x = "Household income",
            y = "Number of households (thousands)"
            ) +
        theme_minimal() +
        scale_x_continuous(breaks=1:16, labels = labels) +
          theme(axis.text.x = element_text(angle = 75, size = 8))               # Rotate axis labels

        aspect.ratio <- .6

        name <- "Graphs/household_income_distribution_by_iou.png"
        ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

        # Don't write over this
        # write_xlsx(income.bucket.iou.level, paste0(dropbox.dir, "Tables/household_income_distribution_by_iou_data.xlsx"))
        p_unload(sf)
    }
}

iou.territory.income.distribution.analysis(run = F)

# 
# Compute non-avoidable cost for residential sector
#
# Use our load-weighted avoided cost number, residential consumption, and revenue fro
#

c <- read_csv(paste0(dropbox.dir, "OriginalData/consumption_by_utility_year_sector/", "ElectricityByUtility.csv"))

# Tidy consumption data and make some useful variables
c <- c %>% select(-"Utility Type")
c <- c %>% rename(total.gwh = "Total Usage", res.gwh = "Residential", utility = "Utility Name", year = Year) %>% select(utility, year, res.gwh, total.gwh)
c <- c %>% mutate(iou = if_else(utility == "Pacific Gas and Electric Company", "pge", if_else(utility == "San Diego Gas and Electric Company", "sdge", "sce")),
                  res.share = res.gwh / total.gwh)

c <- c %>% 
    filter(year == 2019) %>%
    select(iou, year, res.gwh)

care.dat <- read_excel(paste0(dropbox.dir, "OriginalData/care_reports/noncare_and_care_prices_by_iou_year.xlsx"))

care.dat <- care.dat %>% 
    filter(year == 2019) %>%
    mutate(annual.care.kwh = care.kwh * care.customers * 12) %>%
    select(iou, year, annual.care.kwh)

usage <- inner_join(c, care.dat) %>%
    mutate(annual.non.care.kwh = (res.gwh * 1e6) - annual.care.kwh)

p.and.c <- read_xlsx(paste0(dropbox.dir, "Tables/summary_main_result.xlsx")) %>%
    filter(year == 2019) %>%
    select(iou, year, preferred, noncare.price, care.price) %>%
    rename(ferc.noncare.price = noncare.price, ferc.care.price = care.price)

waterfall.rates.2019 <- read_rds(paste0(dropbox.dir, "ModifiedData/care_and_noncare_rates_by_iou_and_major_component_for_2019.rds")) %>%
    group_by(iou) %>%
    summarize(noncare.price = sum(noncare.rate),
              care.price = sum(care.rate))

p.and.c <- left_join(p.and.c, waterfall.rates.2019)

avoidable.cost.dat <- inner_join(usage, p.and.c)

avoidable.cost.dat <- avoidable.cost.dat %>% 
    mutate(non.care.nonavoidable.cost.dollars = annual.non.care.kwh * (noncare.price - preferred),
           ferc.non.care.nonavoidable.cost.dollars = annual.non.care.kwh * (ferc.noncare.price - preferred),
           total.nonavoidable.cost.dollars = non.care.nonavoidable.cost.dollars + (annual.care.kwh * (care.price - preferred)),
           ferc.total.nonavoidable.cost.dollars = ferc.non.care.nonavoidable.cost.dollars + (annual.care.kwh * (ferc.care.price - preferred))
           ) 
# Look and see
avoidable.cost.dat

avoidable.cost.dat <- avoidable.cost.dat %>%
select(iou, year, non.care.nonavoidable.cost.dollars, total.nonavoidable.cost.dollars)

# This gets copied into Jim's spreadsheet which calculates fixed charge schedules
write_xlsx(avoidable.cost.dat, paste0(dropbox.dir, "Tables/2019_noncare_and_total_nonavoidable_cost_by_iou.xlsx"))

# 
# Load excel data and add income distribution to graph
#
# These data come from Jim's spreadsheet Tables\incomebasedfixedcharges_post.xlsx
# I just pull out the rates and some other information and format it in a new spreadsheet before reading in
#
pge.graph.dat <- read_excel(paste0(dropbox.dir, "ModifiedData/sallee_income_based_fixed_charges_graph_data.xlsx"), range = "Sheet1!B28:F38") %>% 
    pivot_longer(cols = c("Uniform Fixed Charge", "As Progressive as Sales Tax", "As Progressive as Income")) %>%
    rename(accts = "Number Accounts") %>%
    mutate(iou = "pge")

sce.graph.dat <- read_excel(paste0(dropbox.dir, "ModifiedData/sallee_income_based_fixed_charges_graph_data.xlsx"), range = "Sheet1!B54:F64") %>% 
    pivot_longer(cols = c("Uniform Fixed Charge", "As Progressive as Sales Tax", "As Progressive as Income")) %>%
    rename(accts = "Number Accounts") %>%
    mutate(iou = "sce")

sdge.graph.dat <- read_excel(paste0(dropbox.dir, "ModifiedData/sallee_income_based_fixed_charges_graph_data.xlsx"), range = "Sheet1!B41:F51") %>% 
    pivot_longer(cols = c("Uniform Fixed Charge", "As Progressive as Sales Tax", "As Progressive as Income")) %>%
    rename(accts = "Number Accounts") %>%
    mutate(iou = "sdge")

rates.dat <- bind_rows(pge.graph.dat, sce.graph.dat, sdge.graph.dat) %>%
    mutate(Income = Income / 1000,
           iou.factor = factor(iou,
                        levels = c("pge", "sce", "sdge"),
                        labels = c("PG&E", "SCE", "SDG&E")),
            name = factor(name,
            levels = c("Uniform Fixed Charge", "As Progressive as Sales Tax", "As Progressive as Income"),
            labels = c("Uniform Fixed Charge", "As Progressive as Sales Tax", "As Progressive as Income")))

income.dist.dat <- rates.dat %>% 
    filter(name == "Uniform Fixed Charge" & (row_number()%%2 == 1 | Income == 200)) %>%
    group_by(iou) %>%
    mutate(accts.pct = accts / sum(accts * (Income < 200)),
           width = lead(Income, n = 1, default = NA) - Income,
            pos = 0.5 * (cumsum(width) + cumsum(c(0, width[-length(width)]))),
           iou.factor = factor(iou,
                        levels = c("pge", "sce", "sdge"),
                        labels = c("PG&E", "SCE", "SDG&E"))) %>%
    filter(Income < 200) %>%
    select(accts.pct, iou, iou.factor, width, pos) %>%
    ungroup()

# All
fixed.charges <- ggplot() + 
    geom_line( data = rates.dat, aes(x = Income, y = value, group = name, color = name), size = 1.5) +
    geom_bar(data = income.dist.dat, aes(x = pos, width = width, y = accts.pct * 200), color = "grey", stat = "identity", alpha = 0.2) +
  scale_color_discrete("") +
  scale_x_continuous(breaks = seq(0, 200, 50), labels = c(0, 50, 100, 150, ">200")) +
  scale_y_continuous(limits = c(0, 200), sec.axis = sec_axis(~ . / 200, breaks = c(0, .125, .25), labels = c(0, 0.125, .25), name = "Proportion of Accounts")) +
  theme_minimal() + labs(x = "Household income (thousands $)", y = "Monthly fixed charge ($)") + 
    theme(axis.text=element_text(size=14),
          axis.title=element_text(size=15),
          legend.text=element_text(size=15),
          legend.title=element_text(size=15),
          plot.title=element_text(size=18),
          strip.text = element_text(size=18),
          legend.position = "bottom",
          legend.direction =  "horizontal") +
    facet_wrap(~iou.factor, ncol=1, scale = "free")
fixed.charges
aspect.ratio <- 1.4
name <- "Graphs/fixed_charge_schedules.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

pge <- ggplot() + 
    geom_line( data = rates.dat %>% filter(iou == "pge"), aes(x = Income, y = value, group = name, color = name), size = 1.5) +
    geom_bar(data = income.dist.dat %>% filter(iou == "pge"), aes(x = pos, width = width, y = accts.pct * 200), color = "grey", stat = "identity", alpha = 0.2) +
  scale_color_discrete("") +
  scale_x_continuous(breaks = seq(0, 200, 50), labels = c(0, 50, 100, 150, ">200")) +
  scale_y_continuous(limits = c(0, 200), sec.axis = sec_axis(~ . / 200, breaks = c(0, .125, .25), labels = c(0, 0.125, .25), name = "Proportion of Accounts")) +
  theme_minimal() + labs(x = "Household income (thousands USD)", y = "Monthly fixed charge (USD)", title = "PG&E") + 
  theme(axis.text=element_text(size=12),
        legend.text=element_text(size=12),
        axis.title=element_text(size=13),
        plot.title = element_text(size=15),
        legend.position="bottom",
        legend.direction = "vertical")
pge
aspect.ratio <- .6
name <- "Graphs/pge_fixed_charge_schedules.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

sce <- ggplot() + 
    geom_line( data = rates.dat %>% filter(iou == "sce"), aes(x = Income, y = value, group = name, color = name), size = 1.5) +
    geom_bar(data = income.dist.dat %>% filter(iou == "sce"), aes(x = pos, width = width, y = accts.pct * 200), color = "grey", stat = "identity", alpha = 0.2) +
  scale_color_discrete("") +
  scale_x_continuous(breaks = seq(0, 200, 50), labels = c(0, 50, 100, 150, ">200")) +
  scale_y_continuous(limits = c(0, 200), sec.axis = sec_axis(~ . / 200, breaks = c(0, .125, .25), labels = c(0, 0.125, .25), name = "Proportion of Accounts")) +
  theme_minimal() + labs(x = "Household income (thousands USD)", y = "Monthly fixed charge (USD)", title = "SCE") + 
  theme(axis.text=element_text(size=12),
        legend.text=element_text(size=12),
        axis.title=element_text(size=13),
        plot.title = element_text(size=15),
        legend.position="bottom",
        legend.direction = "vertical")
sce
aspect.ratio <- .6
name <- "Graphs/sce_fixed_charge_schedules.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

sdge <- ggplot() + 
    geom_line( data = rates.dat %>% filter(iou == "sdge"), aes(x = Income, y = value, group = name, color = name), size = 1.5) +
    geom_bar(data = income.dist.dat %>% filter(iou == "sdge"), aes(x = pos, width = width, y = accts.pct * 200), color = "grey", stat = "identity", alpha = 0.2) +
  scale_color_discrete("") +
  scale_x_continuous(breaks = seq(0, 200, 50), labels = c(0, 50, 100, 150, ">200")) +
  scale_y_continuous(limits = c(0, 200), sec.axis = sec_axis(~ . / 200, breaks = c(0, .125, .25), labels = c(0, 0.125, .25), name = "Proportion of Accounts")) +
  theme_minimal() + labs(x = "Household income (thousands USD)", y = "Monthly fixed charge (USD)", title = "SDG&E") + 
  theme(axis.text=element_text(size=12),
        legend.text=element_text(size=12),
        axis.title=element_text(size=13),
        plot.title = element_text(size=15),
        legend.position="bottom",
        legend.direction = "vertical")
sdge
aspect.ratio <- .6
name <- "Graphs/sdge_fixed_charge_schedules.png"
ggsave(paste0(dropbox.dir, name), device = "png", height = 10 * aspect.ratio , width = 10)

#
# Output graphs data to spreadsheet
#
figures.dat <- list(
     figES1 = preferred.vs.price.grouped.bar.data,
     figES2 = waterfall.data %>% mutate(cat.var = if_else(cat.var == "", "Ancillary Services", cat.var)),
     figES3 = income.n.spending.graph.data,
     fig1 = bundled.res.price.scatter.data,
     fig2 = component.bar.graph.data %>% select(iou, year, name, value),
     fig3 = preferred.vs.price.plot.data %>% select(iou, year, name, value),
     fig4 = waterfall.data %>% mutate(cat.var = if_else(cat.var == "", "Ancillary Services", cat.var)),
     # fig5 =, # This is Meredith's
     fig6 = revenues %>% select(cat, amt.2019),
     fig7 = income.n.spending.graph.data %>% filter(name != "Income"),
     fig8 = income.n.spending.graph.data,
     fig9rates = rates.dat,
     fig9histogram = income.dist.dat
         )

# write_xlsx(figures.dat, paste0(dropbox.dir, "ModifiedData/FigsData/report1_figures_data.xlsx"))

