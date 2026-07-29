# Packages
library(terra)
library(geobr)
library(sf)
library(tibble)
library(stringr)
library(purrr)
library(exactextractr)
library(DBI)
library(duckdb)
library(cli)

cli::cli_h1("CAMS PM 10 zonal statistics routine")

# Database
cli_alert_info("Database connection...")
con <- dbConnect(duckdb(), "cams.duckdb")
cli_alert_success("Done!")

cli_alert_info("Checking tables...")
if (dbExistsTable(con, "pm10_mean_mean")) {
  dbRemoveTable(con, "pm10_mean_mean")
}
if (dbExistsTable(con, "pm10_max_mean")) {
  dbRemoveTable(con, "pm10_max_mean")
}
if (dbExistsTable(con, "pm10_min_mean")) {
  dbRemoveTable(con, "pm10_min_mean")
}
cli_alert_success("Done!")

dbListTables(con)

# Folders
cli_alert_info("Listing files...")
daily_data_folder <- "/dados2/data/cams/cams_2003_2024/cams_pm10_daily_agg/"

# List files
files_min <- list.files(
  daily_data_folder,
  full.names = TRUE,
  pattern = "min.nc$"
)

files_max <- list.files(
  daily_data_folder,
  full.names = TRUE,
  pattern = "max.nc$"
)

files_mean <- list.files(
  daily_data_folder,
  full.names = TRUE,
  pattern = "mean.nc$"
)
cli_alert_success("Done!")

# Municipalities
mun <- read_municipality(year = 2010, simplified = TRUE)
mun <- st_transform(x = mun, crs = 4326)

# Function
agg <- function(x, fun, tb_name) {
  # Read raster and project
  rst <- rast(x)
  rst <- project(x = rst, "EPSG:4326")

  # Zonal statistic computation
  tmp <- exact_extract(x = rst, y = mun, fun = fun, progress = FALSE)

  # Table output with unit conversion and rounding
  res <- tibble(
    code_muni = mun$code_muni,
    date = as.Date(
      x = str_sub(string = basename(x), start = 11, end = 19),
      format = "%Y%m%d"
    ),
    value = round(x = tmp * 1e9, digits = 2), # kg/m3 to μg/m3
  )

  # Write to database
  dbWriteTable(conn = con, name = tb_name, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
cli_alert_info("Computing average mean...")
res_mean <- map(
  .x = files_mean,
  .f = agg,
  fun = "mean",
  tb_name = "pm10_mean_mean",
  .progress = TRUE
)
cli_alert_success("Done!")

cli_alert_info("Computing average max...")
res_max <- map(
  .x = files_max,
  .f = agg,
  fun = "mean",
  tb_name = "pm10_max_mean",
  .progress = TRUE
)
cli_alert_success("Done!")

cli_alert_info("Computing average min...")
res_min <- map(
  .x = files_min,
  .f = agg,
  fun = "mean",
  tb_name = "pm10_min_mean",
  .progress = TRUE
)
cli_alert_success("Done!")

# Export parquet file
cli_alert_info("Exporting files...")
dbExecute(
  con,
  "COPY (SELECT * FROM 'pm10_mean_mean') TO 'pm10_mean_mean.parquet' (FORMAT 'PARQUET')"
)

dbExecute(
  con,
  "COPY (SELECT * FROM 'pm10_max_mean') TO 'pm10_max_mean.parquet' (FORMAT 'PARQUET')"
)

dbExecute(
  con,
  "COPY (SELECT * FROM 'pm10_min_mean') TO 'pm10_min_mean.parquet' (FORMAT 'PARQUET')"
)


# Export CSV file
dbExecute(
  con,
  "COPY (SELECT * FROM 'pm10_mean_mean') TO 'pm10_mean_mean.csv' (FORMAT 'CSV')"
)

dbExecute(
  con,
  "COPY (SELECT * FROM 'pm10_max_mean') TO 'pm10_max_mean.csv' (FORMAT 'CSV')"
)

dbExecute(
  con,
  "COPY (SELECT * FROM 'pm10_min_mean') TO 'pm10_min_mean.csv' (FORMAT 'CSV')"
)
cli_alert_success("Done!")

# Database disconnect
cli_alert_info("Database disconnect...")
dbDisconnect(conn = con)
cli_alert_success("Done!")

cli_h1("END")
