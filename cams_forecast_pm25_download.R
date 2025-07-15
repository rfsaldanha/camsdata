# Packages
cli::cli_h1("CAMS forecast PM2.5 data download routine")
cli::cli_alert_info("Loading packages...")
library(ecmwfr)
library(lubridate)
library(glue)
library(cli)
library(retry)
library(fs)

# Parameters
# dir_data <- "/media/raphaelsaldanha/lacie/cams_forecast_pm25"
dir_data <- "~/Downloads/"
date <- today() - 1
time <- "00:00"
leadtime_hour <- as.character(0:120)

# File name
file_name <- glue(
  "cams_forecast_pm25_{substr(date,0,4)}{substr(date,6,7)}{substr(date,9,10)}.nc"
)

# Declare request
request <- list(
  dataset_short_name = "cams-global-atmospheric-composition-forecasts",
  variable = "particulate_matter_2.5um",
  date = glue("{date}/{date}"),
  time = time,
  leadtime_hour = leadtime_hour,
  type = "forecast",
  data_format = "netcdf",
  download_format = "unarchived",
  area = c(33, -118, -56, -30),
  target = file_name
)

# Token
cli::cli_alert_info("Getting access token...")
wf_set_key(key = Sys.getenv("era5_API_Key"))

# Download file with retry
cli::cli_alert_info("Requesting file...")
retry(
  expr = {
    wf_request(
      request = request,
      transfer = TRUE,
      path = dir_data
    )
  },
  interval = 1,
  until = ~ is_file(as.character(.))
)

cli_h1("END")
