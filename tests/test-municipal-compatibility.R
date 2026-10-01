testthat::test_that("all 12 municipal outputs keep the previous contract and values", {
  ctx <- real_forecast()
  baseline <- new.env(parent = baseenv())
  sys.source(file.path(project_dir, "tests", "fixtures", "municipal-contract.R"), baseline)
  testthat::expect_equal(as.data.frame(.sesai$forecast_catalog()), as.data.frame(baseline$legacy_catalog))
  municipal <- readRDS(file.path(ctx$work, "mun_epsg4326.rds"))
  baseline_db <- file.path(ctx$work, "municipal-before.duckdb")
  updated_db <- file.path(ctx$work, "municipal-after.duckdb")
  before <- .sesai$sesai_connect(baseline_db)
  after <- .sesai$sesai_connect(updated_db)
  on.exit({DBI::dbDisconnect(before, shutdown = TRUE); DBI::dbDisconnect(after, shutdown = TRUE)})
  prepare <- function(con) {
    list2env(list(
      mun = municipal, con = con, dir_data = ctx$work,
      forecast_sequence = function(step) ctx$snapshot$start + seq.int(0L, 120L, by = step) * 3600,
      path = fs::path, cli_h3 = function(...) NULL, cli_alert = function(...) NULL,
      cli_alert_success = function(...) NULL, cli_abort = cli::cli_abort,
      exact_extract = exactextractr::exact_extract, tibble = tibble::tibble,
      with_tz = lubridate::with_tz, dbWriteTable = DBI::dbWriteTable,
      dbQuoteIdentifier = DBI::dbQuoteIdentifier, dbExecute = DBI::dbExecute
    ), parent = baseenv())
  }
  old <- baseline$legacy_municipal_aggregate
  environment(old) <- prepare(before)
  current_env <- prepare(after)
  eval(script_assignment(file.path(project_dir, "cams_forecast.R"), "aggregate_municipal_forecast"), current_env)
  current <- current_env$aggregate_municipal_forecast
  # The aggregation function itself must remain untouched by this extension.
  testthat::expect_identical(body(current), body(old))
  for (i in seq_len(nrow(baseline$legacy_catalog))) {
    cfg <- baseline$legacy_catalog[i, ]
    silence_raster_crs_warning(do.call(old, as.list(cfg)))
    silence_raster_crs_warning(do.call(current, as.list(.sesai$forecast_catalog()[i, ])))
    table <- cfg$table
    schema <- sprintf('DESCRIBE "%s"', table)
    testthat::expect_identical(DBI::dbGetQuery(after, schema), DBI::dbGetQuery(before, schema))
    query <- sprintf('SELECT * FROM "%s" ORDER BY code_muni, date', table)
    testthat::expect_identical(DBI::dbGetQuery(after, query), DBI::dbGetQuery(before, query))
    # Main truncates the municipality code to six digits; dev also supports
    # a direct seven-digit lookup and a six-digit BETWEEN lookup.
    code <- municipal$code_muni[[1]]
    prefix <- substr(as.character(code), 1, 6)
    main_sql <- sprintf('SELECT * FROM "%s" WHERE substr(CAST(code_muni AS VARCHAR), 1, 6) = ? ORDER BY date', table)
    dev_sql <- sprintf('SELECT * FROM "%s" WHERE code_muni = ? ORDER BY date', table)
    range_sql <- sprintf('SELECT * FROM "%s" WHERE code_muni BETWEEN ? AND ? ORDER BY date', table)
    testthat::expect_identical(DBI::dbGetQuery(before, main_sql, params = list(prefix)),
                              DBI::dbGetQuery(after, main_sql, params = list(prefix)))
    direct <- DBI::dbGetQuery(after, dev_sql, params = list(code))
    testthat::expect_equal(nrow(direct), length(seq.int(0, 120, by = cfg$step)))
    testthat::expect_identical(direct, DBI::dbGetQuery(before, dev_sql, params = list(code)))
    testthat::expect_identical(direct, DBI::dbGetQuery(after, range_sql,
                              params = list(as.numeric(prefix) * 10, as.numeric(prefix) * 10 + 9)))
  }
})

testthat::test_that("the main workflow isolates SESAI failures after municipal publication", {
  file <- file.path(project_dir, "cams_forecast.R")
  calls <- as.list(parse(file))
  published <- which(vapply(calls, function(x) identical(x, quote(writeLines(cycle_id, generation_marker, useBytes = TRUE))), logical(1)))
  integrated <- which(vapply(calls, function(x) identical(x, quote(run_sesai_update(force = force_update))), logical(1)))
  testthat::expect_length(published, 1L)
  testthat::expect_true(integrated > published)
  env <- new.env(parent = baseenv())
  env$script_dir <- tempfile("missing-sesai-module-")
  env$app_data <- real_forecast()$work
  env$path <- fs::path
  eval(script_assignment(file, "run_sesai_update"), env)
  testthat::expect_error(suppressWarnings(env$run_sesai_update()),
                        "Municipal generation remains published. SESAI update failed", fixed = TRUE)
})
