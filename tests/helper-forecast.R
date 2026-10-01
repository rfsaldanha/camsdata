project_dir <- Sys.getenv("CAMS_TEST_PROJECT_DIR")
if (!nzchar(project_dir)) project_dir <- normalizePath("..")
.sesai <- new.env(parent = baseenv())
sys.source(file.path(project_dir, "R", "forecast_catalog.R"), envir = .sesai)
sys.source(file.path(project_dir, "R", "sesai_forecast.R"), envir = .sesai)
source_dir <- file.path(project_dir, "data", "sesai")
forecast_data <- Sys.getenv("CAMS_TEST_DATA_DIR", file.path(project_dir, "forecast_data"))
.integration <- new.env(parent = emptyenv())

real_forecast <- function() {
  files <- c(.sesai$forecast_catalog()$filename, ".cams_generation", "mun_epsg4326.rds")
  testthat::skip_if_not(all(file.exists(file.path(forecast_data, files))),
                        "Local real CAMS inputs are unavailable; set CAMS_TEST_DATA_DIR.")
  if (!exists("work", envir = .integration, inherits = FALSE)) {
    work <- tempfile("cams-sesai-tests-")
    dir.create(work)
    stopifnot(all(file.copy(file.path(forecast_data, files), work)))
    .integration$work <- work
    .integration$snapshot <- .sesai$sesai_snapshot(work, source_dir, .sesai$forecast_catalog())
    .integration$territories <- .sesai$sesai_read_territories(source_dir, .integration$snapshot)
  }
  .integration
}

with_sesai_binding <- function(name, replacement, code) {
  original <- get(name, envir = .sesai, inherits = FALSE)
  assign(name, replacement, envir = .sesai)
  on.exit(assign(name, original, envir = .sesai))
  eval(substitute(code), envir = parent.frame())
}

silence_raster_crs_warning <- function(expr) {
  # Several of the existing CAMS NetCDFs omit a CRS declaration. Preserve all
  # other warnings so regressions in the new code fail visibly in tests.
  withCallingHandlers(expr, warning = function(w) {
    known <- "^\\[rast\\] guessed crs|^GDAL Message 1: dimension #[0-9]+ \\((forecast_period|forecast_reference_time|time)\\) is not a "
    if (grepl(known, conditionMessage(w))) invokeRestart("muffleWarning")
  })
}

script_assignment <- function(file, name) {
  expressions <- parse(file)
  selected <- Filter(function(x) {
    is.call(x) && identical(x[[1]], as.name("<-")) && identical(x[[2]], as.name(name))
  }, as.list(expressions))
  stopifnot(length(selected) == 1L)
  selected[[1]]
}

real_sesai_database <- function() {
  ctx <- real_forecast()
  path <- file.path(ctx$work, "cams_forecast_sesai.duckdb")
  if (!file.exists(path)) {
    silence_raster_crs_warning(.sesai$update_sesai_forecast(ctx$work, source_dir))
  }
  .integration$database <- path
  ctx
}
