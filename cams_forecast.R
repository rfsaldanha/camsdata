# Packages
cli::cli_h1("CAMS forecast data download routine")
cli::cli_alert_info("Job start: {lubridate::now()}")
cli::cli_h2("Environment setup")
cli::cli_alert("Loading packages...")
suppressMessages({
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
})
cli_alert_success("Done!")

cli_alert("Setting environment...")
# Bounding box
bbox <- c(13.49, -83.15, -56.69, -32.20)

# Download directory
# dir_data <- "/dados/home/rfsaldanha/camsdata/forecast_data/"
dir_data <- "forecast_data/"

# Forecast range, in hours
leadtime_hour <- as.character(0:120)
leadtime_hour_level <- as.character(seq(0, 120, 3))

# Retry
retry_max_tries <- 100
retry_times <- 1

# Parallel cores
parallel_cores <- 4

# Set update reference time
if (
  now(tzone = "UTC") < as_datetime(today(tzone = "UTC") + duration("10 hours"))
) {
  date <- today() - 1
  time <- "12:00"
} else {
  date <- today()
  time <- "00:00"
}

cli_alert_info("Update reference: {date} {time}")

# File names
file_name_pm25 <- glue(
  "cams_forecast_pm25.nc"
)
file_name_pm10 <- glue(
  "cams_forecast_pm10.nc"
)
file_name_sp <- glue(
  "cams_forecast_sp.nc"
)
file_name_o3 <- glue(
  "cams_forecast_o3.nc"
)
file_name_o3_mc <- glue(
  "cams_forecast_o3_mc.nc"
)
file_name_co <- glue(
  "cams_forecast_co.nc"
)
file_name_co_mc <- glue(
  "cams_forecast_co_mc.nc"
)
file_name_no2 <- glue(
  "cams_forecast_no2.nc"
)
file_name_no2_mc <- glue(
  "cams_forecast_no2_mc.nc"
)
file_name_so2 <- glue(
  "cams_forecast_so2.nc"
)
file_name_so2_mc <- glue(
  "cams_forecast_so2_mc.nc"
)
file_name_temp <- glue(
  "cams_forecast_temp.nc"
)
file_name_uv <- glue(
  "cams_forecast_uv.nc"
)
file_name_wind_u <- glue(
  "cams_forecast_wind_u.nc"
)
file_name_wind_v <- glue(
  "cams_forecast_wind_v.nc"
)
file_name_wind_speed <- glue(
  "cams_forecast_wind_speed.nc"
)
file_name_aerosol <- glue(
  "cams_forecast_aerosol.nc"
)
file_name_prec <- glue(
  "cams_forecast_prec.nc"
)
file_name_iqar <- glue(
  "iqar.nc"
)

# Remove old forecast files
file_delete(list.files(path(dir_data), full.names = TRUE, pattern = "*.nc"))

# Municipalities
cli_alert("Reading geometries file...")
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

## PM10
request_pm10 <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "particulate_matter_10um",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_pm10
)

## Surface pressure
request_sp <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "surface_pressure",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_sp
)

## O3
request_o3 <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "ozone",
  model_level = "137",
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
  model_level = "137",
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
  model_level = "137",
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
  model_level = "137",
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

## Wind U
request_wind_u <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "10m_u_component_of_wind",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_wind_u
)

## Wind V
request_wind_v <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "10m_v_component_of_wind",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_wind_v
)

## Aerosol
request_aerosol <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "organic_matter_aerosol_optical_depth_550nm",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_aerosol
)

## Precipitation
request_prec <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "total_precipitation",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_prec
)

# Token
cli_alert("Retrieving access token...")
wf_set_key(key = Sys.getenv("era5_API_Key"))

cli_alert_success("Done!")

cli_h2("Request forecasts from CAMS")

# Download files with retry
cli_alert("PM 2.5")
retry(
  expr = {
    wf_request(
      request = request_pm25,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_alert("PM 10")
retry(
  expr = {
    wf_request(
      request = request_pm10,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_alert("Surface pressure")
retry(
  expr = {
    wf_request(
      request = request_sp,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_alert("O3")
retry(
  expr = {
    wf_request(
      request = request_o3,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_alert("CO")
retry(
  expr = {
    wf_request(
      request = request_co,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_alert("NO2")
retry(
  expr = {
    wf_request(
      request = request_no2,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_alert("SO2")
retry(
  expr = {
    wf_request(
      request = request_so2,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_alert("Temperature")
retry(
  expr = {
    wf_request(
      request = request_temp,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_h3("UV")
retry(
  expr = {
    wf_request(
      request = request_uv,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_h3("Wind U")
retry(
  expr = {
    wf_request(
      request = request_wind_u,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_h3("Wind V")
retry(
  expr = {
    wf_request(
      request = request_wind_v,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_h3("Aerosol")
retry(
  expr = {
    wf_request(
      request = request_aerosol,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_h3("Precipitation")
retry(
  expr = {
    wf_request(
      request = request_prec,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_h2("Compute gas indicators to different units")
# https://forum.ecmwf.int/t/convert-mass-mixing-ratio-mmr-to-mass-concentration-or-to-volume-mixing-ratio-vmr/1253
# https://teesing.com/en/tools/ppm-mg3-converter

sp <- rast(x = path(dir_data, file_name_sp))
temp <- rast(x = path(dir_data, file_name_temp))
o3 <- rast(x = path(dir_data, file_name_o3))
co <- rast(x = path(dir_data, file_name_co))
no2 <- rast(x = path(dir_data, file_name_no2))
so2 <- rast(x = path(dir_data, file_name_so2))

cli_alert("O3 (kg/kg to kg/m3)")
o3_mc <- o3 * (sp[[seq(1, 121, 3)]] / (260.2 * temp[[seq(1, 121, 3)]]))
writeCDF(
  x = o3_mc,
  filename = path(dir_data, file_name_o3_mc),
  overwrite = TRUE
)
cli_alert_success("Done!")

cli_alert("CO (kg/kg to PPM)")
co_mc <- co * (sp[[seq(1, 121, 3)]] / (296.84 * temp[[seq(1, 121, 3)]])) # kg/kg to kg/m3
co_mc <- co_mc * 1e6 # kg/m3 to mg/m3
co_mc <- 24.45 * co_mc / 28.01 # mg/m3 to PPM
writeCDF(
  x = co_mc,
  filename = path(dir_data, file_name_co_mc),
  overwrite = TRUE
)
cli_alert_success("Done!")

cli_alert("NO2 (kg/kg to kg/m3)")
no2_mc <- no2 * (sp[[seq(1, 121, 3)]] / (180.73 * temp[[seq(1, 121, 3)]]))
writeCDF(
  x = no2_mc,
  filename = path(dir_data, file_name_no2_mc),
  overwrite = TRUE
)
cli_alert_success("Done!")

cli_alert("SO2 (kg/kg to kg/m3)")
so2_mc <- so2 * (sp[[seq(1, 121, 3)]] / (129.78 * temp[[seq(1, 121, 3)]]))
writeCDF(
  x = so2_mc,
  filename = path(dir_data, file_name_so2_mc),
  overwrite = TRUE
)
cli_alert_success("Done!")

cli_h2("Compute IQAr")
rst_pm25 <- terra::rast(path(dir_data, file_name_pm25))
rst_pm10 <- terra::rast(path(dir_data, file_name_pm10))
rst_o3 <- terra::rast(path(dir_data, file_name_o3_mc))
rst_co <- terra::rast(path(dir_data, file_name_co_mc))
rst_no2 <- terra::rast(path(dir_data, file_name_no2_mc))
rst_so2 <- terra::rast(path(dir_data, file_name_so2_mc))

f_iqar <- function(x, pol) {
  sapply(X = x, FUN = riqar::iqar_pol, pol = pol)
}
cli_h3("Compute pollutants specific IQAr")
cli_alert("PM 2.5")
iqar_pm25 <- app(
  x = rst_pm25 * 1e9,
  fun = f_iqar,
  pol = "pm2.5",
  cores = parallel_cores
)
cli_alert_success("Done!")

cli_alert("PM 10")
iqar_pm10 <- app(
  x = rst_pm10 * 1e9,
  fun = f_iqar,
  pol = "pm10",
  cores = parallel_cores
)
cli_alert_success("Done!")

cli_alert("O3")
iqar_o3 <- app(
  x = rst_o3 * 1e9,
  fun = f_iqar,
  pol = "o3",
  cores = parallel_cores
)
cli_alert_success("Done!")

cli_alert("CO")
iqar_co <- app(
  x = rst_co,
  fun = f_iqar,
  pol = "co",
  cores = parallel_cores
)
cli_alert_success("Done!")

cli_alert("NO2")
iqar_no2 <- app(
  x = rst_no2 * 1e9,
  fun = f_iqar,
  pol = "no2",
  cores = parallel_cores
)
cli_alert_success("Done!")

cli_alert("SO2")
iqar_so2 <- app(
  x = rst_so2 * 1e9,
  fun = f_iqar,
  pol = "so2",
  cores = parallel_cores
)
cli_alert_success("Done!")

cli_h3("Compute general IQAr")
iqar <- NULL
for (i in 1:41) {
  tmp <- terra::app(
    x = c(
      iqar_pm25[[seq(1, 121, 3)]][[i]],
      iqar_pm10[[seq(1, 121, 3)]][[i]],
      iqar_o3[[i]],
      iqar_co[[i]],
      iqar_no2[[i]],
      iqar_so2[[i]]
    ),
    fun = max
  )

  iqar <- c(iqar, tmp)
  rm(tmp)
}
iqar <- terra::rast(iqar)

writeCDF(x = iqar, filename = path(dir_data, file_name_iqar), overwrite = TRUE)
cli_alert_success("Done!")

cli_h2("Compute wind speed from U and V vectors...")
wind_u <- rast(path(dir_data, file_name_wind_u))
wind_v <- rast(path(dir_data, file_name_wind_v))
wind_speed <- sqrt(wind_u^2 + wind_v^2) * 3.6 # km/2
writeCDF(
  x = wind_speed,
  filename = path(dir_data, file_name_wind_speed),
  overwrite = TRUE
)
cli_alert_success("Done!")

cli_h2("Update municipal forecasts database")

# Database connection
cli_alert("Deleting old database...")
if (file_exists(path(dir_data, "cams_forecast.duckdb"))) {
  file_delete(path(dir_data, "cams_forecast.duckdb"))
}
cli_alert("Connecting to database...")
con <- dbConnect(duckdb(), path(dir_data, "cams_forecast.duckdb"))
# Table names
tb_name_iqar <- "iqar_forecast"
tb_name_pm25 <- "pm25_mun_forecast"
tb_name_pm10 <- "pm10_mun_forecast"
tb_name_o3 <- "o3_mun_forecast"
tb_name_co <- "co_mun_forecast"
tb_name_no2 <- "no2_mun_forecast"
tb_name_so2 <- "so2_mun_forecast"
tb_name_temp <- "temp_mun_forecast"
tb_name_uv <- "uv_mun_forecast"
tb_name_wind_speed <- "uv_mun_wind_speed"
tb_name_aerosol <- "uv_mun_aerosol"
tb_name_prec <- "uv_mun_prec"

cli_h3("IQAr")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_iqar <- terra::rast(path(dir_data, file_name_iqar))
cli_alert("Projecting raster file...")
rst_iqar <- project(x = rst_iqar, "EPSG:4326")

# Zonal statistic function
agg_iqar <- function(rst, x, fun) {
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
    value = round(x = tmp, digits = 2),
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_iqar, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert("Computing zonal mean...")
res_mean_iqar <- map(
  .x = 1:41,
  .f = agg_iqar,
  rst = rst_iqar,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_iqar) |> tally()
tbl(con, tb_name_iqar) |> head()

cli_h3("PM 2.5")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_pm25 <- terra::rast(path(dir_data, file_name_pm25))
cli_alert("Projecting raster file...")
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
    value = round(x = tmp * 1e9, digits = 2), # kg/m3 to μg/m3
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_pm25, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert("Computing zonal mean...")
res_mean_pm25 <- map(
  .x = 1:121,
  .f = agg_pm25,
  rst = rst_pm25,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_pm25) |> tally()
tbl(con, tb_name_pm25) |> head()

cli_h3("PM 10")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_pm10 <- terra::rast(path(dir_data, file_name_pm10))
cli_alert("Projecting raster file...")
rst_pm10 <- project(x = rst_pm10, "EPSG:4326")

# Zonal statistic function
agg_pm10 <- function(rst, x, fun) {
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
    value = round(x = tmp * 1e9, digits = 2), # kg/m3 to μg/m3
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_pm10, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert("Computing zonal mean...")
res_mean_pm10 <- map(
  .x = 1:121,
  .f = agg_pm10,
  rst = rst_pm10,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_pm10) |> tally()
tbl(con, tb_name_pm10) |> head()

cli_h3("O3")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_o3 <- terra::rast(path(dir_data, file_name_o3_mc))
cli_alert("Projecting raster file...")
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
    value = round(x = tmp * 1e9, digits = 2), # kg/m3 to μg/m3
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_o3, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert("Computing zonal mean...")
res_mean_o3 <- map(
  .x = 1:41,
  .f = agg_o3,
  rst = rst_o3,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_o3) |> tally()
tbl(con, tb_name_o3) |> head()


cli_h3("CO")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_co <- terra::rast(path(dir_data, file_name_co_mc))
cli_alert("Projecting raster file...")
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
    value = round(x = tmp, digits = 2), # PPM
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_co, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert("Computing zonal mean...")
res_mean_co <- map(
  .x = 1:41,
  .f = agg_co,
  rst = rst_co,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_co) |> tally()
tbl(con, tb_name_co) |> head()


cli_h3("NO2")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_no2 <- terra::rast(path(dir_data, file_name_no2_mc))
cli_alert("Projecting raster file...")
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
    value = round(x = tmp * 1e9, digits = 2), # kg/m3 to μg/m3
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_no2, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert("Computing zonal mean...")
res_mean_no2 <- map(
  .x = 1:41,
  .f = agg_no2,
  rst = rst_no2,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_no2) |> tally()
tbl(con, tb_name_no2) |> head()


cli_h3("SO2")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_so2 <- terra::rast(path(dir_data, file_name_so2_mc))
cli_alert("Projecting raster file...")
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
    value = round(x = tmp * 1e9, digits = 2), # kg/m3 to μg/m3
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name_so2, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert("Computing zonal mean...")
res_mean_so2 <- map(
  .x = 1:41,
  .f = agg_so2,
  rst = rst_so2,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_so2) |> tally()
tbl(con, tb_name_so2) |> head()


cli_h3("Temperature")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_temp <- terra::rast(path(dir_data, file_name_temp))
cli_alert("Projecting raster file...")
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
cli_alert("Computing zonal mean...")
res_mean_temp <- map(
  .x = 1:121,
  .f = agg_temp,
  rst = rst_temp,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_temp) |> tally()
tbl(con, tb_name_temp) |> head()

cli_h3("UV")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_uv <- terra::rast(path(dir_data, file_name_uv))
cli_alert("Projecting raster file...")
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
cli_alert("Computing zonal mean...")
res_mean_uv <- map(
  .x = 1:121,
  .f = agg_uv,
  rst = rst_uv,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_uv) |> tally()
tbl(con, tb_name_uv) |> head()

# Database disconnect
cli_alert("Disconnecting database...")
dbDisconnect(conn = con)

cli_h3("Wind speed")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_wind_speed <- terra::rast(path(dir_data, file_name_wind_speed))
cli_alert("Projecting raster file...")
rst_wind_speed <- project(x = rst_wind_speed, "EPSG:4326")

# Zonal statistic function
agg_wind_speed <- function(rst, x, fun) {
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
    value = round(x = tmp * 3.6, digits = 2), # m/s to km/h
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(
    conn = con,
    name = tb_name_wind_speed,
    value = res,
    append = TRUE
  )

  return(TRUE)
}

# Compute zonal mean
cli_alert("Computing zonal mean...")
res_mean_wind_speed <- map(
  .x = 1:121,
  .f = agg_wind_speed,
  rst = rst_wind_speed,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_wind_speed) |> tally()
tbl(con, tb_name_wind_speed) |> head()


cli_h3("Aerosol")

# Read CAMS file
cli_alert("Reading forecast file...")
rst_aerosol <- terra::rast(path(dir_data, file_name_aerosol))
cli_alert("Projecting raster file...")
rst_aerosol <- project(x = rst_aerosol, "EPSG:4326")

# Zonal statistic function
agg_aerosol <- function(rst, x, fun) {
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
    value = round(x = tmp, digits = 2),
  ) |>
    mutate(
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(
    conn = con,
    name = tb_name_aerosol,
    value = res,
    append = TRUE
  )

  return(TRUE)
}

# Compute zonal mean
cli_alert("Computing zonal mean...")
res_mean_aerosol <- map(
  .x = 1:121,
  .f = agg_aerosol,
  rst = rst_aerosol,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert("Checking data...")
tbl(con, tb_name_aerosol) |> tally()
tbl(con, tb_name_aerosol) |> head()

cli_h3("Precipitation")


# Database disconnect
cli_alert("Disconnecting database...")
dbDisconnect(conn = con)

# Fetch INPE BD Queimadas data
cli_h2("Fetch BDQueimadas / INPE data")
bdq_base_url <- "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/America_Sul/"
bdq_file_names <- paste0(
  "focos_diario_",
  format(seq.Date(date - 2, date, by = "day"), "%Y%m%d"), # Today and last two days
  ".csv"
)
bdq_urls <- paste0(bdq_base_url, bdq_file_names)

cli_alert("Fetching data...")
bdq_focos <- data.frame()
for (i in bdq_urls) {
  # Try and retry download
  bdq_focos <- retry(
    expr = {
      tmp <- read_csv(file = i) |>
        filter(satelite %in% c("AQUA_M-M", "AQUA_M-T")) |>
        select(id, lat, lon, data_hora_gmt)
    },
    interval = retry_times,
    max_tries = retry_max_tries,
    until = ~ is.data.frame(.)
  )

  bdq_focos <- bind_rows(bdq_focos, tmp)
  rm(tmp)
}
cli_alert("Saving results...")
saveRDS(object = bdq_focos, file = path(dir_data, "bdq_focos.rds"))
cli_alert_success("Done!")

cli_alert_info("Job end: {now()}")
cli_h1("END")
