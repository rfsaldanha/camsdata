# Packages
library(terra)
library(tibble)
library(dplyr)
library(fs)
library(stringr)
library(glue)
library(lubridate)
library(cli)
library(tools)
library(furrr)

# Folders
original_unit_folder <- "/media/raphaelsaldanha/lacie/cams_co/"
new_unit_folder <- "/media/raphaelsaldanha/lacie/cams_co_mc/"

temp_folder <- "/media/raphaelsaldanha/lacie/cams_temp/"
sp_folder <- "/media/raphaelsaldanha/lacie/cams_sp/"

# Files list
gas_files <- list.files(
  original_unit_folder,
  full.names = TRUE,
  pattern = "*.nc"
)
temp_files <- list.files(
  temp_folder,
  full.names = TRUE,
  pattern = "*.nc"
)
sp_files <- list.files(
  sp_folder,
  full.names = TRUE,
  pattern = "*.nc"
)

# Files table
gas_df <- tibble(
  gas_path = gas_files,
  date = ymd(str_sub(gas_files, -11, -4))
)
temp_df <- tibble(
  temp_path = temp_files,
  date = ymd(str_sub(temp_files, -11, -4))
)
sp_df <- tibble(
  sp_path = sp_files,
  date = ymd(str_sub(sp_files, -11, -4))
)

# Join data frames
df <- inner_join(gas_df, temp_df) |>
  inner_join(sp_df) |>
  arrange(date) |>
  relocate(date)

# Convert to list
df_list <- as.list(as.data.frame(t(df)))
names(df_list) <- NULL

# Unit conversion function
gas_fun <- function(df_list, dest) {
  # Read files
  date <- df_list[1]
  gas <- rast(df_list[2])
  temp <- rast(df_list[3])
  sp <- rast(df_list[4])

  # Unit conversion
  gas_mc <- 24.45 * ((gas * (sp / (296.84 * temp)) * 1e6)) / 28.01

  # Save
  writeCDF(
    x = gas_mc,
    filename = path(
      dest,
      paste0("cams_co_mc_", format(ymd(date), "%Y%m%d"), ".nc")
    )
  )

  return(TRUE)
}

# Execute
plan(multisession, workers = 6)
res <- future_map(
  .x = df_list,
  .f = gas_fun,
  dest = new_unit_folder,
  .progress = TRUE
)
