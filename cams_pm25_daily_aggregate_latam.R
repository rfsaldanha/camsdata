# Packages
library(terra)
library(tibble)
library(stringr)
library(glue)
library(lubridate)
library(cli)

# Parameters
hourly_data_folder <- "/media/raphaelsaldanha/lacie/latam_cams_pm25"
daily_data_folder <- "/media/raphaelsaldanha/lacie/latam_cams_pm25_daily_agg"

# List files
files <- list.files(hourly_data_folder, full.names = TRUE, pattern = "*.nc")
df <- tibble(
  files = files,
  date = as_date(str_sub(files, -16, -9))
)

for (d in unique(df$date)) {
  cli_h1(glue("{as.Date(d)}"))

  tmp <- subset(df, date == d)

  nc <- rast(x = tmp$files)

  cli_alert_info("Aggregating...")
  nc_agg <- app(
    x = nc,
    fun = sum,
    filename = glue(
      "{daily_data_folder}/latam_cams_pm25_{substr(d,0,4)}{substr(d,6,7)}{substr(d,9,10)}_sum.nc"
    ),
    overwrite = TRUE
  )

  d <- as.Date(d)

  # cli_alert_info("Writing to disk...")
  # writeCDF(
  #   x = nc_agg,
  #   filename = glue(
  #     "{daily_data_folder}/latam_cams_pm25_{substr(d,0,4)}{substr(d,6,7)}{substr(d,9,10)}_sum.nc"
  #   ),
  #   overwrite = TRUE
  # )

  cli_alert_success("Done!")
}
