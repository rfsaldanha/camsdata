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
original_unit_folder <- "/dados2/data/cams/cams_2025/cams_co/"
new_unit_folder <- "/dados2/data/cams/cams_2025/cams_co_mc/"

dir_create(new_unit_folder)

# Files list
gas_files <- list.files(
  original_unit_folder,
  full.names = TRUE,
  pattern = "\\.nc$"
)

# Files table
gas_df <- tibble(
  gas_path = gas_files,
  date = ymd(str_sub(gas_files, -11, -4))
)

# Arrange files
df <- gas_df |>
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

  # Convert mass mixing ratio (kg/kg) to volume mixing ratio (ppm)
  dry_air_molar_mass <- 28.96546 # g/mol
  co_molar_mass <- 28.0101 # g/mol
  gas_mc <- gas * (dry_air_molar_mass / co_molar_mass) * 1e6

  # Save
  writeCDF(
    x = gas_mc,
    filename = path(
      dest,
      paste0("cams_co_mc_", format(ymd(date), "%Y%m%d"), ".nc")
    ),
    varname = "co",
    longname = "Carbon monoxide volume mixing ratio",
    unit = "ppm",
    overwrite = TRUE
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
