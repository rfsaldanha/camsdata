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
  target = "teste_wind_u.nc"
)

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
  target = "teste_wind_v.nc"
)

wf_request(
  request = request_wind_u,
  transfer = TRUE,
  path = dir_data
)

wf_request(
  request = request_wind_v,
  transfer = TRUE,
  path = dir_data
)

wind_u <- rast(path(dir_data, "teste_wind_u.nc"))
wind_v <- rast(path(dir_data, "teste_wind_v.nc"))

wind_speed <- sqrt(wind_u^2 + wind_v^2) * 3.6 # km/2

plot(wind_speed[[1:10]])

plot(wind_speed[[120]])
