# Packages
cli::cli_h1("CAMS forecast data download routine")
cli::cli_h2("Environment setup")
cli::cli_alert_info("Loading packages...")
library(ecmwfr)
library(lubridate)
library(glue)
library(cli)
library(retry)
library(fs)
library(dplyr)
library(readr)
library(terra)
library(sf)
library(tibble)
library(stringr)
library(purrr)
library(exactextractr)
library(DBI)
library(duckdb)

# Bounding box
bbox <- c(33, -118, -56, -30)

# Download directory
dir_data <- "/dados/home/rfsaldanha/camsdata/forecast_data/"
# dir_data <- "forecast_data/"

# Forecast range, in hours
leadtime_hour <- as.character(0:120)
leadtime_hour_level <- as.character(seq(0, 120, 3))

# Set update reference time
if (
  now(tzone = "UTC") < as_datetime(today(tzone = "UTC") + duration("18 hours"))
) {
  date <- today()
  time <- "00:00"
} else {
  date <- today()
  time <- "12:00"
}

cli_alert_info("Update refence: {date} {time}")

# File names
file_name_pm25 <- glue(
  "cams_forecast_pm25.nc"
)
file_name_o3 <- glue(
  "cams_forecast_o3.nc"
)
file_name_co <- glue(
  "cams_forecast_co.nc"
)
file_name_no2 <- glue(
  "cams_forecast_no2.nc"
)
file_name_so2 <- glue(
  "cams_forecast_so2.nc"
)
file_name_temp <- glue(
  "cams_forecast_temp.nc"
)
file_name_uv <- glue(
  "cams_forecast_uv.nc"
)

# Remove old forecast files
file_delete(list.files(path(dir_data), full.names = TRUE, pattern = "*.nc"))

# Municipalities
cli_alert_info("Reading geometries file...")
# mun <- geobr::read_municipality(year = 2010, simplified = TRUE)
# mun <- st_transform(x = mun, crs = 4326)
# saveRDS(mun, "mun_epsg4326.rds")
mun <- readRDS(path(dir_data, "mun_epsg4326.rds"))

# Declare requests
## PM2.5
request_pm25 <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "particulate_matter_2.5um",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_pm25
)

## O3
request_o3 <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "ozone",
  pressure_level = "1000",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour_level,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_o3
)

## CO
request_co <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "carbon_monoxide",
  pressure_level = "1000",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour_level,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_co
)

## NO2
request_no2 <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "nitrogen_dioxide",
  pressure_level = "1000",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour_level,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_no2
)

## SO2
request_so2 <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "sulphur_dioxide",
  pressure_level = "1000",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour_level,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_so2
)

## Temperature
request_temp <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "2m_temperature",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_temp
)

## UV
request_uv <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "uv_biologically_effective_dose",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_uv
)

# Token
cli::cli_alert_info("Retrieving access token...")
wf_set_key(key = Sys.getenv("era5_API_Key"))

cli_h2("Request forecasts from CAMS")

# Download files with retry
cli_h3("PM 2.5")
retry(
  expr = {
    wf_request(
      request = request_pm25,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = 1,
  max_tries = 100,
  until = ~ is_file(as.character(.))
)

cli_h3("O3")
retry(
  expr = {
    wf_request(
      request = request_o3,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = 1,
  max_tries = 100,
  until = ~ is_file(as.character(.))
)

cli_h3("CO")
retry(
  expr = {
    wf_request(
      request = request_co,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = 1,
  max_tries = 100,
  until = ~ is_file(as.character(.))
)

cli_h3("NO2")
retry(
  expr = {
    wf_request(
      request = request_no2,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = 1,
  max_tries = 100,
  until = ~ is_file(as.character(.))
)

cli_h3("SO2")
retry(
  expr = {
    wf_request(
      request = request_so2,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = 1,
  max_tries = 100,
  until = ~ is_file(as.character(.))
)

cli_h3("Temperature")
retry(
  expr = {
    wf_request(
      request = request_temp,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = 1,
  max_tries = 100,
  until = ~ is_file(as.character(.))
)

cli_h3("UV")
retry(
  expr = {
    wf_request(
      request = request_uv,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = 1,
  max_tries = 100,
  until = ~ is_file(as.character(.))
)

cli_h2("Update forecasts database")

# Database connection
cli_alert_info("Deleting old database...")
if (file_exists(path(dir_data, "cams_forecast.duckdb"))) {
  file_delete(path(dir_data, "cams_forecast.duckdb"))
}
cli_alert_info("Connecting to database...")
con <- dbConnect(duckdb(), path(dir_data, "cams_forecast.duckdb"))
tb_name_pm25 <- "pm25_mun_forecast"
tb_name_o3 <- "o3_mun_forecast"
tb_name_co <- "co_mun_forecast"
tb_name_no2 <- "no2_mun_forecast"
tb_name_so2 <- "so2_mun_forecast"
tb_name_temp <- "temp_mun_forecast"
tb_name_uv <- "uv_mun_forecast"

cli_h3("PM 2.5")

# Read CAMS file
cli_alert_info("Reading forecast file...")
rst_pm25 <- terra::rast(path(dir_data, file_name_pm25))
cli_alert_info("Projecting raster file...")
rst_pm25 <- project(x = rst_pm25, "EPSG:4326")

# Zonal statistic function
agg_pm25 <- function(rst, x, fun) {
  # Zonal statistic computation
  tmp <- exact_extract(x = rst[[x]], y = mun, fun = fun, progress = FALSE)

  # Date and time
  seq_dates <- seq(
    from = as_datetime(paste(date, time), format = "%Y-%m-%d %H:%M"),
    to = as_datetime(date + duration(120, "hours")),
    by = "1 hours"
  )

  # Table output with unit conversion and rounding
  res <- tibble(
    code_muni = mun$code_muni,
    date = seq_dates[x],
    value = round(x = tmp * 1000000000, digits = 2), # kg/m3 to μg/m3
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_pm25, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert_info("Computing zonal mean...")
res_mean_pm25 <- map(
  .x = 1:121,
  .f = agg_pm25,
  rst = rst_pm25,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert_info("Checking data...")
tbl(con, tb_name_pm25) |> tally()
tbl(con, tb_name_pm25) |> head()

cli_h3("O3")

# Read CAMS file
cli_alert_info("Reading forecast file...")
rst_o3 <- terra::rast(path(dir_data, file_name_o3))
cli_alert_info("Projecting raster file...")
rst_o3 <- project(x = rst_o3, "EPSG:4326")

# Zonal statistic function
agg_o3 <- function(rst, x, fun) {
  # Zonal statistic computation
  tmp <- exact_extract(x = rst[[x]], y = mun, fun = fun, progress = FALSE)

  # Date and time
  seq_dates <- seq(
    from = as_datetime(paste(date, time), format = "%Y-%m-%d %H:%M"),
    to = as_datetime(date + duration(120, "hours")),
    by = "3 hours"
  )

  # Table output with unit conversion and rounding
  res <- tibble(
    code_muni = mun$code_muni,
    date = seq_dates[x],
    value = round(x = tmp * 28.9644 / 47.9982 * 1e9, digits = 2), # kg/kg to μg/m3
    # https://forum.ecmwf.int/t/convert-mass-mixing-ratio-mmr-to-mass-concentration-or-to-volume-mixing-ratio-vmr/1253/2
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_o3, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert_info("Computing zonal mean...")
res_mean_o3 <- map(
  .x = 1:41,
  .f = agg_o3,
  rst = rst_o3,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert_info("Checking data...")
tbl(con, tb_name_o3) |> tally()
tbl(con, tb_name_o3) |> head()


cli_h3("CO")

# Read CAMS file
cli_alert_info("Reading forecast file...")
rst_co <- terra::rast(path(dir_data, file_name_co))
cli_alert_info("Projecting raster file...")
rst_co <- project(x = rst_co, "EPSG:4326")

# Zonal statistic function
agg_co <- function(rst, x, fun) {
  # Zonal statistic computation
  tmp <- exact_extract(x = rst[[x]], y = mun, fun = fun, progress = FALSE)

  # Date and time
  seq_dates <- seq(
    from = as_datetime(paste(date, time), format = "%Y-%m-%d %H:%M"),
    to = as_datetime(date + duration(120, "hours")),
    by = "3 hours"
  )

  # Table output with unit conversion and rounding
  res <- tibble(
    code_muni = mun$code_muni,
    date = seq_dates[x],
    value = round(x = tmp * 28.9644 / 28.0101 * 1e9, digits = 2), # kg/kg to μg/m3
    # https://forum.ecmwf.int/t/convert-mass-mixing-ratio-mmr-to-mass-concentration-or-to-volume-mixing-ratio-vmr/1253/2
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_co, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert_info("Computing zonal mean...")
res_mean_co <- map(
  .x = 1:41,
  .f = agg_co,
  rst = rst_co,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert_info("Checking data...")
tbl(con, tb_name_co) |> tally()
tbl(con, tb_name_co) |> head()


cli_h3("NO2")

# Read CAMS file
cli_alert_info("Reading forecast file...")
rst_no2 <- terra::rast(path(dir_data, file_name_co))
cli_alert_info("Projecting raster file...")
rst_no2 <- project(x = rst_no2, "EPSG:4326")

# Zonal statistic function
agg_no2 <- function(rst, x, fun) {
  # Zonal statistic computation
  tmp <- exact_extract(x = rst[[x]], y = mun, fun = fun, progress = FALSE)

  # Date and time
  seq_dates <- seq(
    from = as_datetime(paste(date, time), format = "%Y-%m-%d %H:%M"),
    to = as_datetime(date + duration(120, "hours")),
    by = "3 hours"
  )

  # Table output with unit conversion and rounding
  res <- tibble(
    code_muni = mun$code_muni,
    date = seq_dates[x],
    value = round(x = tmp * 28.9644 / 46.0055 * 1e9, digits = 2), # kg/kg to μg/m3
    # https://forum.ecmwf.int/t/convert-mass-mixing-ratio-mmr-to-mass-concentration-or-to-volume-mixing-ratio-vmr/1253/2
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_no2, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert_info("Computing zonal mean...")
res_mean_no2 <- map(
  .x = 1:41,
  .f = agg_no2,
  rst = rst_no2,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert_info("Checking data...")
tbl(con, tb_name_no2) |> tally()
tbl(con, tb_name_no2) |> head()


cli_h3("SO2")

# Read CAMS file
cli_alert_info("Reading forecast file...")
rst_so2 <- terra::rast(path(dir_data, file_name_co))
cli_alert_info("Projecting raster file...")
rst_so2 <- project(x = rst_so2, "EPSG:4326")

# Zonal statistic function
agg_so2 <- function(rst, x, fun) {
  # Zonal statistic computation
  tmp <- exact_extract(x = rst[[x]], y = mun, fun = fun, progress = FALSE)

  # Date and time
  seq_dates <- seq(
    from = as_datetime(paste(date, time), format = "%Y-%m-%d %H:%M"),
    to = as_datetime(date + duration(120, "hours")),
    by = "3 hours"
  )

  # Table output with unit conversion and rounding
  res <- tibble(
    code_muni = mun$code_muni,
    date = seq_dates[x],
    value = round(x = tmp * 28.9644 / 64.0638 * 1e9, digits = 2), # kg/kg to μg/m3
    # https://forum.ecmwf.int/t/convert-mass-mixing-ratio-mmr-to-mass-concentration-or-to-volume-mixing-ratio-vmr/1253/2
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_so2, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert_info("Computing zonal mean...")
res_mean_so2 <- map(
  .x = 1:41,
  .f = agg_so2,
  rst = rst_so2,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert_info("Checking data...")
tbl(con, tb_name_so2) |> tally()
tbl(con, tb_name_so2) |> head()


cli_h3("Temperature")

# Read CAMS file
cli_alert_info("Reading forecast file...")
rst_temp <- terra::rast(path(dir_data, file_name_temp))
cli_alert_info("Projecting raster file...")
rst_temp <- project(x = rst_temp, "EPSG:4326")

# Zonal statistic function
agg_temp <- function(rst, x, fun) {
  # Zonal statistic computation
  tmp <- exact_extract(x = rst[[x]], y = mun, fun = fun, progress = FALSE)

  # Date and time
  seq_dates <- seq(
    from = as_datetime(paste(date, time), format = "%Y-%m-%d %H:%M"),
    to = as_datetime(date + duration(120, "hours")),
    by = "1 hours"
  )

  # Table output with unit conversion and rounding
  res <- tibble(
    code_muni = mun$code_muni,
    date = seq_dates[x],
    value = round(x = tmp - 272.15, digits = 2), # K to °C
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_temp, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert_info("Computing zonal mean...")
res_mean_temp <- map(
  .x = 1:121,
  .f = agg_temp,
  rst = rst_temp,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert_info("Checking data...")
tbl(con, tb_name_temp) |> tally()
tbl(con, tb_name_temp) |> head()

cli_h3("UV")

# Read CAMS file
cli_alert_info("Reading forecast file...")
rst_uv <- terra::rast(path(dir_data, file_name_uv))
cli_alert_info("Projecting raster file...")
rst_uv <- project(x = rst_uv, "EPSG:4326")

# Zonal statistic function
agg_uv <- function(rst, x, fun) {
  # Zonal statistic computation
  tmp <- exact_extract(x = rst[[x]], y = mun, fun = fun, progress = FALSE)

  # Date and time
  seq_dates <- seq(
    from = as_datetime(paste(date, time), format = "%Y-%m-%d %H:%M"),
    to = as_datetime(date + duration(120, "hours")),
    by = "1 hours"
  )

  # Table output with unit conversion and rounding
  res <- tibble(
    code_muni = mun$code_muni,
    date = seq_dates[x],
    value = round(x = tmp * 40, digits = 2), # Wm2 to UVI
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_uv, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert_info("Computing zonal mean...")
res_mean_uv <- map(
  .x = 1:121,
  .f = agg_uv,
  rst = rst_uv,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert_info("Checking data...")
tbl(con, tb_name_uv) |> tally()
tbl(con, tb_name_uv) |> head()

# Database disconnect
cli_alert_info("Disconnecting database...")
dbDisconnect(conn = con)

# Fetch INPE BD Queimadas data
bdq_base_url <- "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/America_Sul/"
bdq_file_names <- paste0(
  "focos_diario_",
  format(seq.Date(date - 2, date, by = "day"), "%Y%m%d"), # Today and last two days
  ".csv"
)
bdq_urls <- paste0(bdq_base_url, bdq_file_names)

bdq_focos <- data.frame()
for (i in bdq_urls) {
  # Try and retry download
  bdq_focos <- retry(
    expr = {
      tmp <- read_csv(file = i) |>
        filter(satelite %in% c("AQUA_M-M", "AQUA_M-T")) |>
        select(id, lat, lon, data_hora_gmt)
    },
    interval = 1,
    max_tries = 5,
    until = ~ is.data.frame(.)
  )

  bdq_focos <- bind_rows(bdq_focos, tmp)
  rm(tmp)
}
saveRDS(object = bdq_focos, file = path(dir_data, "bdq_focos.rds"))

cli_h1("END")
