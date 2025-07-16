# Packages
cli::cli_h1("CAMS forecast PM2.5 update database routine")
cli::cli_alert_info("Loading packages...")
library(dplyr)
library(lubridate)
library(glue)
library(cli)
library(terra)
library(sf)
library(fs)
library(tibble)
library(stringr)
library(purrr)
library(exactextractr)
library(DBI)
library(duckdb)
library(ggplot2)

# Reference forecast file
# file <- "~/Downloads/cams_forecast_pm25_20250715.nc"
dir_data <- "camsdata/"
file <- path(dir_data, "cams_forecast_pm25.nc")

# Read CAMS file
cli_alert_info("Reading CAMS forecast file...")
rst <- terra::rast(file)
cli_alert_info("Projecting raster file...")
rst <- project(x = rst, "EPSG:4326")

# Database
cli_alert_info("Connecting to database...")
con <- dbConnect(duckdb(), path(dir_data, "cams_forecast.duckdb"))
tb_name <- "pm25_mun_forecast"
if (dbExistsTable(con, "pm25_mun_forecast")) {
  cli_alert_info("Deleting old table...")
  dbRemoveTable(con, "pm25_mun_forecast")
}

# Municipalities
cli_alert_info("Reading geometries file...")
# mun <- geobr::read_municipality(year = 2010, simplified = TRUE)
# mun <- st_transform(x = mun, crs = 4326)
# saveRDS(mun, "mun_epsg4326.rds")
mun <- readRDS(path(dir_data, "mun_epsg4326.rds"))

# Zonal statistic function
agg <- function(rst, x, fun) {
  # Zonal statistic computation
  tmp <- exact_extract(x = rst[[x]], y = mun, fun = fun, progress = FALSE)

  # Table output with unit conversion and rounding
  res <- tibble(
    code_muni = mun$code_muni,
    date = as_date(
      x = paste0(
        str_sub(string = basename(file), start = 20, end = 27)
      ),
      format = "%Y%m%d"
    ),
    value = round(x = tmp * 1000000000, digits = 2), # kg/m3 to μg/m3
  ) |>
    mutate(
      date = date + duration(x - 1, "hour"),
      date = with_tz(date, "America/Sao_Paulo")
    )

  # Write to database
  dbWriteTable(conn = con, name = tb_name, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert_info("Computing zonal mean...")
res_mean <- map(
  .x = 1:121,
  .f = agg,
  rst = rst,
  fun = "mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Check data
cli_alert_info("Checking data...")
tbl(con, tb_name) |> tally()
tbl(con, tb_name) |> head()

# Database disconnect
cli_alert_info("Disconnecting database...")
dbDisconnect(conn = con)
cli_h1("END")
