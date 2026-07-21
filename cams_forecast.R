# Validate the complete file before starting any costly downloads. Rscript can
# evaluate top-level expressions incrementally, which otherwise lets a syntax
# error near the end appear only after hours of processing.
script_argument <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
if (length(script_argument)) {
  script_file <- normalizePath(
    sub("^--file=", "", script_argument[[1]]),
    mustWork = TRUE
  )
  invisible(parse(file = script_file))
}

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
  library(ntfy)
})
cli_alert_success("Done!")

# ntfy alert
ntfy_topic <- "ocs_update_monitorarsaude"
ntfy_send(
  message = glue("Update job start. {now()}"),
  tags = tags$robot,
  topic = ntfy_topic
)

cli_alert("Setting environment...")

# Bounding box
bbox <- c(13.49, -83.15, -56.69, -32.20)

# Forecast output directory. CAMS_FORECAST_DATA_DIR can point to a shared
# production directory; otherwise retain the historical server path when it
# exists and use this repository's forecast_data directory during development.
script_dir <- if (length(script_argument)) {
  path_dir(path_abs(sub("^--file=", "", script_argument[[1]])))
} else {
  path_abs(".")
}
configured_data_dir <- Sys.getenv("CAMS_FORECAST_DATA_DIR", unset = "")
production_data_dir <- "/dados/home/rfsaldanha/camsdata/forecast_data"
default_data_dir <- if (dir_exists(production_data_dir)) {
  production_data_dir
} else {
  path(script_dir, "forecast_data")
}
app_data <- path_abs(if (nzchar(configured_data_dir)) configured_data_dir else default_data_dir)
dir_data <- path(app_data, "update_data")
mun_geo <- path(app_data, "mun_epsg4326.rds")
dir_create(c(app_data, dir_data), recurse = TRUE)
if (!file_exists(mun_geo)) stop("Municipality geometry file not found: ", mun_geo)

# Forecast range, in hours
leadtime_hour <- as.character(0:120)
leadtime_hour_level <- as.character(seq(0, 120, 3))

# Retry
retry_max_tries <- 100
retry_times <- 1

# Parallel cores
parallel_cores <- 4

fetch_bdq_focos <- function(output_file, reference_date = as_date(now(tzone = "UTC"))) {
  bdq_base_url <- "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/America_Sul/"
  file_names <- paste0(
    "focos_diario_",
    format(seq.Date(reference_date - 2, reference_date, by = "day"), "%Y%m%d"),
    ".csv"
  )
  urls <- paste0(bdq_base_url, file_names)
  cli_h2("Fetch BDQueimadas / INPE data")
  cli_alert("Fetching data...")
  fires <- map_dfr(urls, function(url) {
    tryCatch(
      retry(
        expr = {
          read_csv(file = url, show_col_types = FALSE) |>
            filter(satelite %in% c("AQUA_M-M", "AQUA_M-T")) |>
            select(id, lat, lon, data_hora_gmt)
        },
        interval = retry_times,
        max_tries = min(retry_max_tries, 5),
        until = ~ is.data.frame(.)
      ),
      error = function(error) {
        cli_alert_warning("Could not fetch {url}: {conditionMessage(error)}")
        data.frame()
      }
    )
  }) |>
    filter(
      is.finite(lat), is.finite(lon),
      between(lat, -90, 90), between(lon, -180, 180)
    ) |>
    distinct(id, .keep_all = TRUE) |>
    arrange(data_hora_gmt)
  if (!nrow(fires)) stop("BDQueimadas returned no valid active-fire records.")

  dir_create(path_dir(output_file), recurse = TRUE)
  temporary_file <- tempfile(
    pattern = ".bdq_focos-", tmpdir = path_dir(output_file), fileext = ".rds"
  )
  on.exit(if (file_exists(temporary_file)) file_delete(temporary_file), add = TRUE)
  saveRDS(fires, temporary_file)
  file_move(temporary_file, output_file)
  cli_alert_success("Saved {nrow(fires)} unique active-fire records.")
  invisible(fires)
}

# Set update reference time
# https://confluence.ecmwf.int/display/CKB/CAMS%3A+Global+atmospheric+composition+forecast+data+documentation#heading-DataavailabilityHHMM
# 00 UTC forecast data availability guaranteed by 10:00 UTC -> update at ~7am BR
# 12 UTC forecast data availability guaranteed by 22:00 UTC -> update at ~7pm BR
select_cams_cycle <- function(reference_now = now(tzone = "UTC")) {
  reference_now <- with_tz(reference_now, "UTC")
  reference_date <- as_date(reference_now, tz = "UTC")
  day_start <- as_datetime(reference_date, tz = "UTC")

  if (reference_now < day_start + hours(10)) {
    return(list(date = reference_date - 1, time = "12:00"))
  }
  if (reference_now < day_start + hours(22)) {
    return(list(date = reference_date, time = "00:00"))
  }
  list(date = reference_date, time = "12:00")
}

reference_now <- now(tzone = "UTC")
forecast_cycle <- select_cams_cycle(reference_now)
date <- forecast_cycle$date
time <- forecast_cycle$time

cli_alert_info("Update reference: {date} {time}")

cycle_id <- paste(date, time, sep = "T")
generation_marker <- path(app_data, ".cams_generation")
published_cycle <- if (file_exists(generation_marker)) {
  trimws(readLines(generation_marker, n = 1L, warn = FALSE))
} else {
  character()
}
force_update <- tolower(Sys.getenv("CAMS_FORCE_UPDATE", unset = "false")) %in%
  c("1", "true", "yes")
if (!force_update && identical(published_cycle, cycle_id)) {
  fetch_bdq_focos(path(app_data, "bdq_focos.rds"))
  cli_alert_success("Forecast cycle {cycle_id} is already published; nothing to download.")
  ntfy_send(
    message = glue("Update skipped: CAMS cycle {cycle_id} is already published."),
    tags = tags$white_check_mark,
    topic = ntfy_topic
  )
  quit(save = "no", status = 0L)
}

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
file_name_temp_ml137 <- glue(
  "cams_forecast_temp_ml137.nc"
)
file_name_q_ml137 <- glue(
  "cams_forecast_q_ml137.nc"
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

# Remove old staging files. Published files remain available to the dashboard
# until the complete new generation is ready.
staging_files <- list.files(dir_data, full.names = TRUE, all.files = TRUE, no.. = TRUE)
if (length(staging_files)) file_delete(staging_files)

# Municipalities
cli_alert("Reading geometries file...")
# mun <- geobr::read_municipality(year = 2010, simplified = TRUE)
# mun <- st_transform(x = mun, crs = 4326)
# saveRDS(mun, "mun_epsg4326.rds")
mun <- readRDS(mun_geo)

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

## Temperature and specific humidity at model level 137
request_temp_ml137 <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "temperature",
  model_level = "137",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour_level,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_temp_ml137
)

request_q_ml137 <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "specific_humidity",
  model_level = "137",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour_level,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = bbox,
  target = file_name_q_ml137
)

## 2 m temperature shown in the dashboard
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

cli_alert("Temperature at model level 137")
retry(
  expr = {
    wf_request(
      request = request_temp_ml137,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_alert("Specific humidity at model level 137")
retry(
  expr = {
    wf_request(
      request = request_q_ml137,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = retry_times,
  max_tries = retry_max_tries,
  until = ~ is_file(as.character(.))
)
cli_alert_success("Done!")

cli_alert("2 m temperature")
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

cli_alert("UV")
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

cli_alert("Wind U")
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

cli_alert("Wind V")
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

cli_alert("Aerosol")
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

cli_alert("Precipitation")
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

cli_h2("Validate model-level raster geometry and layer counts")

pm25 <- rast(path(dir_data, file_name_pm25))
sp <- rast(path(dir_data, file_name_sp))
o3 <- rast(path(dir_data, file_name_o3))
co <- rast(path(dir_data, file_name_co))
no2 <- rast(path(dir_data, file_name_no2))
so2 <- rast(path(dir_data, file_name_so2))
temp_ml137 <- rast(path(dir_data, file_name_temp_ml137))
q_ml137 <- rast(path(dir_data, file_name_q_ml137))

model_level_rasters <- list(
  O3 = o3, CO = co, NO2 = no2, SO2 = so2,
  temperature = temp_ml137, specific_humidity = q_ml137
)
geometry_matches <- vapply(
  model_level_rasters,
  function(x) {
    compareGeom(
      pm25, x, lyrs = FALSE, crs = TRUE, ext = TRUE, rowcol = TRUE,
      res = TRUE, stopOnError = FALSE
    )
  },
  logical(1)
)
if (any(!geometry_matches)) {
  cli_alert_warning(
    "Geometry differs for {paste(names(geometry_matches)[!geometry_matches], collapse = ', ')}; these rasters will be aligned before conversion."
  )
}
model_level_counts <- vapply(model_level_rasters, nlyr, numeric(1))
if (nlyr(pm25) != 121 || nlyr(sp) != 121 || any(model_level_counts != 41)) {
  cli_abort("Unexpected number of forecast layers.")
}
cli_alert_success("Done!")

cli_h2("Compute gas indicators to different units")
# CAMS gas fields are mass mixing ratios per kg of dry air. Temperature and
# specific humidity are taken at the same model level as the gases. Pressure at
# full model level 137 is derived from the adjacent IFS L137 half levels.

align_to_surface_grid <- function(x, reference, field) {
  geometry_matches <- compareGeom(
    x, reference, lyrs = FALSE, crs = TRUE, ext = TRUE, rowcol = TRUE,
    res = TRUE, stopOnError = FALSE
  )
  if (!geometry_matches) {
    cli_alert_warning("Aligning {field} to the surface-field grid.")
    x <- resample(x, reference, method = "bilinear")
  }
  x
}

met_layers <- seq(1, 121, 3)
sp_ml137_reference <- sp[[met_layers]]
temp_ml137 <- align_to_surface_grid(temp_ml137, sp_ml137_reference, "temperature")
q_ml137 <- align_to_surface_grid(q_ml137, sp_ml137_reference, "specific humidity")
o3 <- align_to_surface_grid(o3, sp_ml137_reference, "O3")
co <- align_to_surface_grid(co, sp_ml137_reference, "CO")
no2 <- align_to_surface_grid(no2, sp_ml137_reference, "NO2")
so2 <- align_to_surface_grid(so2, sp_ml137_reference, "SO2")

# IFS L137 half-level coefficients: level 136 has A=0, B=0.997630;
# level 137 (surface) has A=0, B=1. Full-level pressure is their mean.
p_half_above <- 0.997630 * sp_ml137_reference
p_half_below <- sp_ml137_reference
pressure_ml137 <- (p_half_above + p_half_below) / 2

# q is water-vapour mass divided by moist-air mass. Convert total moist-air
# density to dry-air density because CAMS gas MMR uses kg of dry air.
dry_air_gas_constant <- 287.058 # J/(kg K)
epsilon <- 0.621981 # ratio of dry-air to water-vapour gas constants
virtual_temperature <- temp_ml137 * (1 + (1 / epsilon - 1) * q_ml137)
moist_air_density <- pressure_ml137 / (dry_air_gas_constant * virtual_temperature)
dry_air_density <- (1 - q_ml137) * moist_air_density

# Use forecast periods as the layer dimension in converted gas files
gas_forecast_period <- depth(pressure_ml137)
depth(o3) <- gas_forecast_period
depth(co) <- gas_forecast_period
depth(no2) <- gas_forecast_period
depth(so2) <- gas_forecast_period
depthName(o3) <- "forecast_period"
depthName(co) <- "forecast_period"
depthName(no2) <- "forecast_period"
depthName(so2) <- "forecast_period"

cli_alert("O3 (kg/kg to kg/m3)")
o3_mc <- o3 * dry_air_density
writeCDF(
  x = o3_mc,
  filename = path(dir_data, file_name_o3_mc),
  varname = "o3",
  longname = "Ozone mass concentration",
  unit = "kg m-3",
  overwrite = TRUE
)
cli_alert_success("Done!")

cli_alert("CO (kg/kg to PPM)")
dry_air_molar_mass <- 28.96546 # g/mol
co_molar_mass <- 28.0101 # g/mol
co_mc <- co * (dry_air_molar_mass / co_molar_mass) * 1e6
writeCDF(
  x = co_mc,
  filename = path(dir_data, file_name_co_mc),
  varname = "co",
  longname = "Carbon monoxide volume mixing ratio",
  unit = "ppm",
  overwrite = TRUE
)
cli_alert_success("Done!")

cli_alert("NO2 (kg/kg to kg/m3)")
no2_mc <- no2 * dry_air_density
writeCDF(
  x = no2_mc,
  filename = path(dir_data, file_name_no2_mc),
  varname = "no2",
  longname = "Nitrogen dioxide mass concentration",
  unit = "kg m-3",
  overwrite = TRUE
)
cli_alert_success("Done!")

cli_alert("SO2 (kg/kg to kg/m3)")
so2_mc <- so2 * dry_air_density
writeCDF(
  x = so2_mc,
  filename = path(dir_data, file_name_so2_mc),
  varname = "so2",
  longname = "Sulfur dioxide mass concentration",
  unit = "kg m-3",
  overwrite = TRUE
)
cli_alert_success("Done!")

cli_h2("Compute instantaneous air-quality indicator")
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
tb_name_iqar <- "iqar_mun_forecast"
tb_name_pm25 <- "pm25_mun_forecast"
tb_name_pm10 <- "pm10_mun_forecast"
tb_name_o3 <- "o3_mun_forecast"
tb_name_co <- "co_mun_forecast"
tb_name_no2 <- "no2_mun_forecast"
tb_name_so2 <- "so2_mun_forecast"
tb_name_temp <- "temp_mun_forecast"
tb_name_uv <- "uv_mun_forecast"
tb_name_wind_speed <- "wind_speed_mun_forecast"
tb_name_aerosol <- "aerosol_mun_forecast"
tb_name_prec <- "prec_mun_forecast"

forecast_start <- as_datetime(
  paste(date, time), format = "%Y-%m-%d %H:%M", tz = "UTC"
)
forecast_sequence <- function(step_hours) {
  forecast_start + hours(seq.int(0L, 120L, by = step_hours))
}

aggregate_municipal_forecast <- function(label, filename, table, step, scale = 1, offset = 0) {
  cli_h3(label)
  cli_alert("Reading and aggregating forecast file...")
  rst <- terra::rast(path(dir_data, filename))
  if (!terra::same.crs(rst, mun)) {
    cli_alert("Projecting raster file...")
    rst <- terra::project(rst, terra::crs(mun))
  }
  layer_dates <- forecast_sequence(step)
  if (terra::nlyr(rst) != length(layer_dates)) {
    cli_abort("Unexpected layer count for {label}: {terra::nlyr(rst)}.")
  }
  means <- as.matrix(exact_extract(rst, mun, "mean", progress = FALSE))
  result <- tibble(
    code_muni = rep(mun$code_muni, times = terra::nlyr(rst)),
    date = rep(with_tz(layer_dates, "America/Sao_Paulo"), each = nrow(mun)),
    value = round(as.vector(means) * scale + offset, digits = 2)
  )
  dbWriteTable(con, table, result, overwrite = TRUE)
  quoted_table <- as.character(dbQuoteIdentifier(con, table))
  quoted_index <- as.character(dbQuoteIdentifier(con, paste0(table, "_code_date_idx")))
  dbExecute(con, paste0("CREATE INDEX ", quoted_index, " ON ", quoted_table, " (code_muni, date)"))
  cli_alert_success("Done: {nrow(result)} rows.")
  invisible(NULL)
}

forecast_tables <- tribble(
  ~label, ~filename, ~table, ~step, ~scale, ~offset,
  "Instantaneous air-quality indicator", file_name_iqar, "iqar_mun_forecast", 3L, 1, 0,
  "PM 2.5", file_name_pm25, "pm25_mun_forecast", 1L, 1e9, 0,
  "PM 10", file_name_pm10, "pm10_mun_forecast", 1L, 1e9, 0,
  "O3", file_name_o3_mc, "o3_mun_forecast", 3L, 1e9, 0,
  "CO", file_name_co_mc, "co_mun_forecast", 3L, 1, 0,
  "NO2", file_name_no2_mc, "no2_mun_forecast", 3L, 1e9, 0,
  "SO2", file_name_so2_mc, "so2_mun_forecast", 3L, 1e9, 0,
  "Temperature", file_name_temp, "temp_mun_forecast", 1L, 1, -273.15,
  "UV", file_name_uv, "uv_mun_forecast", 1L, 40, 0,
  "Wind speed", file_name_wind_speed, "wind_speed_mun_forecast", 1L, 1, 0,
  "Aerosol", file_name_aerosol, "aerosol_mun_forecast", 1L, 1, 0,
  "Precipitation", file_name_prec, "prec_mun_forecast", 1L, 1e3, 0
)
pwalk(forecast_tables, aggregate_municipal_forecast)

cli_h3("Wind vectors")
cli_alert("Computing wind vectors...")

wind2json <- function(rst_u, rst_v, depth, reference_time, forecast_hour,
                      n_round = 2, path) {
  # Verify compatibility between rst files
  if (!compareGeom(
    rst_u, rst_v, lyrs = TRUE, crs = TRUE, ext = TRUE, rowcol = TRUE,
    res = TRUE, stopOnError = FALSE
  )) {
    stop(
      "The rst_u and rst_v files must have the same dimensions and resolution (same number of columns, number of rows, spatial resolution and boundaries extend)."
    )
  }

  # Header variables
  nx <- terra::ncol(rst_u) # Number of rows
  ny <- terra::nrow(rst_u) # Number of columns
  dx <- terra::res(rst_u)[1] # X spatial resolution, in degrees
  dy <- terra::res(rst_u)[2] # Y spatial resolution, in degrees
  tot <- nx * ny # Number of observations
  la1 <- terra::ext(rst_u)[4] # Bounding box
  lo1 <- terra::ext(rst_u)[1]
  la2 <- terra::ext(rst_u)[3]
  lo2 <- terra::ext(rst_u)[2]
  parameterUnit <- "m.s-1"

  # Data
  data_u <- round(as.vector(t(terra::as.matrix(rst_u[[depth]]))), n_round)
  data_v <- round(as.vector(t(terra::as.matrix(rst_v[[depth]]))), n_round)

  # List
  wind_list <- list(
    list(
      "header" = list(
        "parameterNumberName" = "eastward_wind",
        "parameterUnit" = parameterUnit,
        "parameterNumber" = 2,
        "parameterCategory" = 2,
        "nx" = nx,
        "ny" = ny,
        "numberPoints" = tot,
        "dx" = dx,
        "dy" = dy,
        "la1" = la1,
        "lo1" = lo1,
        "la2" = la2,
        "lo2" = lo2,
        "refTime" = reference_time,
        "forecastTime" = forecast_hour
      ),
      "data" = data_u
    ),
    list(
      "header" = list(
        "parameterNumberName" = "northward_wind",
        "parameterUnit" = parameterUnit,
        "parameterNumber" = 3,
        "parameterCategory" = 2,
        "nx" = nx,
        "ny" = ny,
        "numberPoints" = tot,
        "dx" = dx,
        "dy" = dy,
        "la1" = la1,
        "lo1" = lo1,
        "la2" = la2,
        "lo2" = lo2,
        "refTime" = reference_time,
        "forecastTime" = forecast_hour
      ),
      "data" = data_v
    )
  )

  # Json
  wind_json <- jsonlite::toJSON(x = wind_list, auto_unbox = TRUE)

  # Write
  write(x = wind_json, file = path)

  # Return
  return(TRUE)
}


rst_u <- terra::rast(path(dir_data, "cams_forecast_wind_u.nc"))
rst_v <- terra::rast(path(dir_data, "cams_forecast_wind_v.nc"))

for (i in 1:121) {
  res <- wind2json(
    rst_u = rst_u,
    rst_v = rst_v,
    depth = i,
    reference_time = format(forecast_start, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    forecast_hour = i - 1L,
    n_round = 2,
    path = path(dir_data, paste0("wind_", i, ".json"))
  )
}

cli_alert_success("Done!")

# Database disconnect
cli_alert("Disconnecting database...")
dbDisconnect(conn = con)


fetch_bdq_focos(path(dir_data, "bdq_focos.rds"))

# Move updated files. The generation marker is written last; consumers use it
# as a commit signal and never reload a partially published generation.
file_move(path = list.files(dir_data, full.names = TRUE), new_path = app_data)
writeLines(cycle_id, generation_marker, useBytes = TRUE)

# Message
ntfy_send(
  message = glue("Update job end. {now()}"),
  tags = tags$white_check_mark,
  topic = ntfy_topic
)

cli_alert_info("Job end: {now()}")
cli_h1("END")
