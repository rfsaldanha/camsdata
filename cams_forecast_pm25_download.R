# Packages
cli::cli_h1("CAMS forecast PM2.5 data download routine")
cli::cli_alert_info("Loading packages...")
library(ecmwfr)
library(lubridate)
library(glue)
library(cli)
library(retry)
library(fs)

# Download directory
# dir_data <- "~/Downloads/"
dir_data <- "camsdata/"

# Forecast range, in hours
leadtime_hour <- as.character(0:120)

if (am(now(tzone = "UTC"))) {
  # If am, select date time as yesterday 12 hour
  date <- today() - 1
  time <- "12:00"
} else {
  # Else, select date time as today 0 hour
  date <- today()
  time <- "00:00"
}

# File name
file_name <- glue(
  "cams_forecast_pm25.nc"
)

# Remove old file
if (file_exists(path(dir_data, file_name))) {
  file_delete(path(dir_data, file_name))
}

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
  max_tries = 100,
  until = ~ is_file(as.character(.))
)

# Save datestamp
file_delete(list.files(path = "dir_data", pattern = "datestamp_"))
datestamp <- path(dir_data, glue("datestamp_{format(date, '%Y%m%d')}"))
system(glue("touch {datestamp}_{time}"))

cli_h1("END")
