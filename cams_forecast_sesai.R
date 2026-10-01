# Aggregate only the published CAMS rasters. No downloads or notifications.
script_argument <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
if (!length(script_argument)) stop("Run this entrypoint with Rscript.")
script_file <- normalizePath(sub("^--file=", "", script_argument[[1]]), mustWork = TRUE)
invisible(parse(file = script_file))
script_dir <- dirname(script_file)
source(file.path(script_dir, "R", "forecast_catalog.R"))
source(file.path(script_dir, "R", "sesai_forecast.R"))

configured <- Sys.getenv("CAMS_FORECAST_DATA_DIR", unset = "")
production <- "/dados/home/rfsaldanha/camsdata/forecast_data"
data_dir <- if (nzchar(configured)) configured else if (dir.exists(production)) {
  production
} else file.path(script_dir, "forecast_data")
force <- tolower(Sys.getenv("CAMS_FORCE_UPDATE", unset = "false")) %in% c("1", "true", "yes")
update_sesai_forecast(
  normalizePath(data_dir, mustWork = TRUE), file.path(script_dir, "data", "sesai"), force = force
)
