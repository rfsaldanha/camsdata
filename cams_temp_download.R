# Packages
library(ecmwfr)
library(lubridate)
library(glue)
library(cli)
library(retry)
library(fs)

# Token
wf_set_key(key = Sys.getenv("era5_API_Key"))

# Parameters
# dir_data <- "/media/raphaelsaldanha/lacie/cams_temp"
# dir_data <- "/dados2/data/cams/cams_temp"
dir_data <- "/dados2/data/cams/cams_2003_2024/cams_temp"
dates <- as.character(seq(ymd("2003-01-01"), ymd("2024-12-31"), by = "1 day"))
dates <- rev(dates)
times <- c(
  "00:00",
  "03:00",
  "06:00",
  "09:00",
  "12:00",
  "15:00",
  "18:00",
  "21:00"
)

# Download loop
for (d in dates) {
  cli_h1(glue("CAMS temperature {d}"))

  # File name
  file_name <- glue(
    "cams_temp_{substr(d,0,4)}{substr(d,6,7)}{substr(d,9,10)}.nc"
  )

  # Check if file is already available
  if (file.exists(paste0(dir_data, "/", file_name))) {
    cli_alert_warning("File already exists. Going for next.")
    next
  }

  # Declare request
  request <- list(
    dataset_short_name = "cams-global-reanalysis-eac4",
    variable = "2m_temperature",
    date = glue("{d}/{d}"),
    time = times,
    data_format = "netcdf",
    download_format = "unarchived",
    # area = c(33.28, -118.47, -56.65, -34.1),
    target = file_name
  )

  # Download file with retry
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
}
