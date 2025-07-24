# Packages
library(terra)
library(tibble)
library(stringr)
library(glue)
library(lubridate)
library(cli)
library(tools)
library(purrr)

# Folders
hourly_data_folder <- "/media/raphaelsaldanha/lacie/cams_co/"
daily_data_folder <- "/media/raphaelsaldanha/lacie/cams_co_daily_agg/"

# List files
files <- list.files(hourly_data_folder, full.names = TRUE, pattern = "*.nc")

# Functions
agg <- function(x, fun) {
  res <- app(
    x = rast(x),
    fun = fun,
    filename = glue(
      "{daily_data_folder}/{file_path_sans_ext(basename(x))}_{fun}.nc"
    ),
    overwrite = TRUE
  )

  return(res)
}

res_mean <- map(.x = files, .f = agg, fun = "mean", .progress = TRUE)
res_max <- map(.x = files, .f = agg, fun = "max", .progress = TRUE)
res_min <- map(.x = files, .f = agg, fun = "min", .progress = TRUE)
