library(ecmwfr)
library(lubridate)
library(glue)
library(terra)
library(fs)

date <- today()
time <- "00:00"
leadtime_hour <- as.character(0:120)
dir_data <- "forecast_data/"

bbox <- c(13.49, -83.15, -56.69, -32.20)

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
  target = "teste_prec.nc"
)

wf_request(
  request = request_prec,
  transfer = TRUE,
  path = dir_data
)

teste <- rast(path(dir_data, "teste_prec.nc"))

plot(teste[[120]])

plot(teste[[c(80, 90, 100, 120)]])
