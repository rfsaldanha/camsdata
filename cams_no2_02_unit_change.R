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
original_unit_folder <- "/dados2/data/cams/cams_2003_2024/cams_no2/"
new_unit_folder <- "/dados2/data/cams/cams_2003_2024/cams_no2_mc/"

temp_folder <- "/dados2/data/cams/cams_2003_2024/cams_temp"
sp_folder <- "/dados2/data/cams/cams_2003_2024/cams_sp"

dir_create(new_unit_folder)

# Files list
gas_files <- list.files(
  original_unit_folder,
  full.names = TRUE,
  pattern = "\\.nc$"
)
temp_files <- list.files(
  temp_folder,
  full.names = TRUE,
  pattern = "\\.nc$"
)
sp_files <- list.files(
  sp_folder,
  full.names = TRUE,
  pattern = "\\.nc$"
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

  # Convert mass mixing ratio (kg/kg) to mass concentration (ug/m3)
  dry_air_gas_constant <- 287.058 # J/(kg K)
  gas_mc <- gas * (sp / (dry_air_gas_constant * temp)) * 1e9

  # Save
  writeCDF(
    x = gas_mc,
    filename = path(
      dest,
      paste0("cams_no2_mc_", format(ymd(date), "%Y%m%d"), ".nc")
    ),
    varname = "no2",
    longname = "Nitrogen dioxide mass concentration",
    unit = "ug m-3",
    overwrite = TRUE
  )

  return(TRUE)
}

# Execute
plan(multisession, workers = 4)
res <- future_map(
  .x = df_list,
  .f = gas_fun,
  dest = new_unit_folder,
  .progress = TRUE
)
