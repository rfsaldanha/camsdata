# Packages
library(ecmwfr)
library(lubridate)
library(glue)
library(cli)

# Token
wf_set_key(key = Sys.getenv("era5_API_Key"))

# Parameters
dir_data <- "/media/raphaelsaldanha/lacie/latam_cams_pm25"
dates <- as.character(seq(ymd("2024-01-01"), ymd("2024-12-31"), by = "1 day"))
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

for (d in dates) {
  for (t in times) {
    cli_h1(glue("CAMS PM2.5 {d} {t}"))

    file_name <- glue(
      "latam_cams_pm25_{substr(d,0,4)}{substr(d,6,7)}{substr(d,9,10)}_{substr(t,0,2)}{substr(t,4,5)}.nc"
    )

    if (file.exists(paste0(dir_data, "/", file_name))) {
      cli_alert_warning("File already exists. Going for next.")
      next
    }

    request <- list(
      dataset_short_name = "cams-global-reanalysis-eac4",
      variable = "particulate_matter_2.5um",
      date = glue("{d}/{d}"),
      time = "00:00",
      data_format = "netcdf",
      download_format = "unarchived",
      area = c(33.28, -118.47, -56.65, -34.1),
      target = file_name
    )

    file <- wf_request(
      request = request,
      transfer = TRUE,
      path = dir_data
    )
  }
}
