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

# Database
con <- dbConnect(duckdb(), "cams.duckdb")

if (dbExistsTable(con, "co_mean_mean")) {
  dbRemoveTable(con, "co_mean_mean")
}
if (dbExistsTable(con, "co_max_mean")) {
  dbRemoveTable(con, "co_max_mean")
}
if (dbExistsTable(con, "co_min_mean")) {
  dbRemoveTable(con, "co_min_mean")
}

dbListTables(con)

# Folders
daily_data_folder <- "/media/raphaelsaldanha/lacie/cams_co_daily_agg/"

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
    value = round(x = tmp * 1000000000, digits = 2), # kg/m3 to μg/m3
  )

  # Write to database
  dbWriteTable(conn = con, name = tb_name, value = res, append = TRUE)

  return(TRUE)
}

# Compute zonal mean
res_mean <- map(
  .x = files_mean,
  .f = agg,
  fun = "mean",
  tb_name = "co_mean_mean",
  .progress = TRUE
)

res_max <- map(
  .x = files_max,
  .f = agg,
  fun = "mean",
  tb_name = "co_max_mean",
  .progress = TRUE
)

res_min <- map(
  .x = files_min,
  .f = agg,
  fun = "mean",
  tb_name = "co_min_mean",
  .progress = TRUE
)

# Export parquet file
dbExecute(
  con,
  "COPY (SELECT * FROM 'co_mean_mean') TO 'co_mean_mean.parquet' (FORMAT 'PARQUET')"
)

dbExecute(
  con,
  "COPY (SELECT * FROM 'co_max_mean') TO 'co_max_mean.parquet' (FORMAT 'PARQUET')"
)

dbExecute(
  con,
  "COPY (SELECT * FROM 'co_min_mean') TO 'co_min_mean.parquet' (FORMAT 'PARQUET')"
)

# Export CSV file
dbExecute(
  con,
  "COPY (SELECT * FROM 'co_mean_mean') TO 'co_mean_mean.csv' (FORMAT 'CSV')"
)

dbExecute(
  con,
  "COPY (SELECT * FROM 'co_max_mean') TO 'co_max_mean.csv' (FORMAT 'CSV')"
)

dbExecute(
  con,
  "COPY (SELECT * FROM 'co_min_mean') TO 'co_min_mean.csv' (FORMAT 'CSV')"
)

# Database disconnect
dbDisconnect(conn = con)
