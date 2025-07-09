# Packages
library(dplyr)
library(lubridate)
library(glue)
library(cli)
library(terra)
library(geobr)
library(sf)
library(tibble)
library(stringr)
library(purrr)
library(exactextractr)
library(DBI)
library(duckdb)
library(ggplot2)

# Reference forecast file
file <- "/media/raphaelsaldanha/lacie/cams_forecast_pm25/cams_forecast_pm25_20250709.nc"

# Database
con <- dbConnect(duckdb(), "cams_forecast.duckdb")
tb_name <- "pm25_mun_forecast"
if (dbExistsTable(con, "pm25_mun_forecast")) {
  dbRemoveTable(con, "pm25_mun_forecast")
}

# Municipalities
mun <- read_municipality(year = 2010, simplified = TRUE)
mun <- st_transform(x = mun, crs = 4326)

# Read file
rst <- terra::rast(file)
rst <- project(x = rst, "EPSG:4326")

# Function
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
res_mean <- map(
  .x = 1:121,
  .f = agg,
  rst = rst,
  fun = "mean",
  .progress = TRUE
)

# Retrieve data
tbl(con, tb_name) |>
  filter(code_muni == 5107602) |>
  arrange(date) |>
  collect() |>
  ggplot(aes(x = date, y = value)) +
  ylim(0, NA) +
  geom_line() +
  labs(
    title = "Previsão de PM2.5",
    # subtitle = "Rio de Janeiro, RJ",
    caption = "CAMS/Copernicus"
  )

# Database disconnect
dbDisconnect(conn = con)
