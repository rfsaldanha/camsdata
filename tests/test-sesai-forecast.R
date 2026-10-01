testthat::test_that("territorial sources retain separate records and audited exclusions", {
  ctx <- real_forecast()
  territories <- ctx$territories
  dsei <- territories$layers$dsei
  polo <- territories$layers$polo
  testthat::expect_equal(nrow(dsei), 36L)
  testthat::expect_equal(nrow(polo), 397L)
  testthat::expect_equal(length(unique(dsei$cod_dsei)), 34L)
  testthat::expect_equal(sum(grepl("em estudo", dsei$dsei)), 2L)
  testthat::expect_equal(sum(polo$cod_polo == 0), 8L)
  testthat::expect_gt(anyDuplicated(polo$cod_polo), 0L)
  testthat::expect_false(anyDuplicated(c(dsei$territory_id, polo$territory_id)) > 0L)
  testthat::expect_false(any(.sesai$sesai_normalize_name(polo$polo) == "TERRITORIO DE CONEXAO", na.rm = TRUE))
  testthat::expect_equal(sum(territories$exclusions$reason == "connection_territory"), 55L)
  testthat::expect_equal(sum(territories$exclusions$reason == "empty_geometry"), 1L)
  for (type in c("dsei", "polo")) {
    original <- sf::st_read(.sesai$sesai_source_paths(source_dir)[[type]], quiet = TRUE)
    actual <- territories$layers[[type]]
    fields <- names(sf::st_drop_geometry(original))
    testthat::expect_equal(sf::st_drop_geometry(actual)[fields],
                          sf::st_drop_geometry(original)[actual$source_row, fields],
                          ignore_attr = TRUE)
    testthat::expect_equal(actual$territory_id,
      paste(type, ctx$snapshot$source_versions[[type]], actual$source_row, sep = ":"))
    testthat::expect_true(all(sf::st_is_valid(sf::st_transform(actual, 5880))))
    testthat::expect_false(any(sf::st_is_empty(actual)))
  }
  testthat::expect_equal(.sesai$sesai_normalize_name(c("  território   de CONEXÃO ", "TERRITORIO DE CONEXAO", NA)),
                        c("TERRITORIO DE CONEXAO", "TERRITORIO DE CONEXAO", NA_character_))
})

testthat::test_that("all 24 series match independent extractions and preserve WKB registries", {
  ctx <- real_forecast()
  catalog <- .sesai$forecast_catalog()
  municipal_inputs <- file.path(ctx$work, c(catalog$filename, ".cams_generation", "mun_epsg4326.rds"))
  before <- tools::md5sum(municipal_inputs)
  result <- silence_raster_crs_warning(.sesai$update_sesai_forecast(ctx$work, source_dir, force = TRUE))
  testthat::expect_equal(result$status, "published")
  testthat::expect_identical(tools::md5sum(municipal_inputs), before)
  .integration$database <- result$path
  con <- .sesai$sesai_connect(result$path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  testthat::expect_length(grep("_forecast$", DBI::dbListTables(con)), 24L)
  for (type in c("dsei", "polo")) {
    registry <- DBI::dbReadTable(con, paste0(type, "_territories"))
    restored <- sf::st_as_sfc(structure(as.list(registry$geometry_wkb), class = "WKB"), crs = 4326)
    testthat::expect_equal(sf::st_as_binary(restored), sf::st_as_binary(sf::st_geometry(ctx$territories$layers[[type]])))
    sample <- ctx$territories$layers[[type]][c(1, nrow(registry)), ]
    for (i in seq_len(nrow(catalog))) {
      cfg <- catalog[i, ]
      table <- sub("_mun_", paste0("_", type, "_"), cfg$table, fixed = TRUE)
      rst <- silence_raster_crs_warning(terra::rast(file.path(ctx$work, cfg$filename)))
      expected <- as.matrix(exactextractr::exact_extract(rst, sample, "mean", progress = FALSE))
      dates <- as.numeric(ctx$snapshot$start) + seq.int(0, 120, by = cfg$step) * 3600
      for (j in seq_len(nrow(sample))) {
        series <- DBI::dbGetQuery(con, sprintf('SELECT date, value FROM "%s" WHERE territory_id = ? ORDER BY date', table),
                                 params = list(sample$territory_id[j]))
        testthat::expect_identical(as.numeric(series$date), dates)
        testthat::expect_equal(series$value, round(as.numeric(expected[j, ]) * cfg$scale + cfg$offset, 2))
      }
    }
  }
  testthat::expect_false(file.exists(file.path(ctx$work, "territories.rds")))
  testthat::expect_false(file.exists(file.path(ctx$work, "places.rds")))
})

testthat::test_that("missing coverage and missing pixels become SQL NULL", {
  ctx <- real_forecast()
  layer <- silence_raster_crs_warning(terra::rast(file.path(ctx$work, "cams_forecast_pm25.nc"))[[1]])
  sample <- ctx$territories$layers$polo[1, ]
  terra::values(layer) <- NA_real_
  result <- .sesai$sesai_zonal_values(layer, sample, .sesai$sesai_dates(ctx$snapshot$start, 1)[1], 1e9, 0)
  testthat::expect_true(is.na(result$value))
  # Crop a real raster to a region disjoint from the real territory.
  layer <- silence_raster_crs_warning(terra::rast(file.path(ctx$work, "cams_forecast_pm25.nc"))[[1]])
  layer <- terra::crop(layer, terra::ext(-82, -80, -55, -53))
  result <- .sesai$sesai_zonal_values(layer, sample, .sesai$sesai_dates(ctx$snapshot$start, 1)[1], 1e9, 0)
  testthat::expect_true(is.na(result$value))
  con <- .sesai$sesai_connect(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbWriteTable(con, "missing", result)
  testthat::expect_equal(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM missing WHERE value IS NULL")$n, 1)
})

testthat::test_that("same-cycle validation detects incomplete products and changed sources", {
  ctx <- real_sesai_database()
  testthat::expect_true(file.exists(.integration$database))
  before <- tools::md5sum(.integration$database)
  relative_sources <- withr::with_dir(project_dir, {
    .sesai$sesai_snapshot(ctx$work, "data/sesai", .sesai$forecast_catalog())
  })
  testthat::expect_identical(relative_sources$signature, ctx$snapshot$signature)
  testthat::expect_equal(.sesai$update_sesai_forecast(ctx$work, source_dir)$status, "current")
  testthat::expect_identical(tools::md5sum(.integration$database), before)
  partial <- tempfile(fileext = ".duckdb")
  file.copy(.integration$database, partial)
  con <- .sesai$sesai_connect(partial)
  DBI::dbExecute(con, "DELETE FROM pm25_polo_forecast WHERE date = (SELECT min(date) FROM pm25_polo_forecast)") |> invisible()
  DBI::dbDisconnect(con, shutdown = TRUE)
  testthat::expect_false(.sesai$sesai_is_current(partial, ctx$snapshot, .sesai$forecast_catalog()))
  # Restore the deliberately incomplete copy over the temporary published DB.
  file.copy(partial, .integration$database, overwrite = TRUE)
  testthat::expect_equal(silence_raster_crs_warning(.sesai$update_sesai_forecast(ctx$work, source_dir))$status, "published")
  testthat::expect_true(.sesai$sesai_is_current(.integration$database, ctx$snapshot, .sesai$forecast_catalog()))

  modified_sources <- tempfile("sesai-source-version-")
  dir.create(modified_sources)
  stopifnot(all(file.copy(list.files(source_dir, full.names = TRUE), modified_sources, recursive = TRUE)))
  cpg <- sub("\\.shp$", ".cpg", .sesai$sesai_source_paths(modified_sources)[["polo"]])
  cat("\n", file = cpg, append = TRUE)
  changed <- .sesai$sesai_snapshot(ctx$work, modified_sources, .sesai$forecast_catalog())
  testthat::expect_false(identical(changed$source_versions[["polo"]], ctx$snapshot$source_versions[["polo"]]))
  testthat::expect_identical(changed$source_versions[["dsei"]], ctx$snapshot$source_versions[["dsei"]])
  testthat::expect_false(.sesai$sesai_is_current(.integration$database, changed, .sesai$forecast_catalog()))
  fail <- function(...) stop("changed source triggers rebuild")
  with_sesai_binding("sesai_read_territories", fail, {
    testthat::expect_error(.sesai$update_sesai_forecast(ctx$work, modified_sources), "changed source triggers rebuild")
  })
  modified_version <- ctx$snapshot
  with_sesai_binding("sesai_processing_version", "new-version", {
    testthat::expect_false(.sesai$sesai_is_current(.integration$database, modified_version, .sesai$forecast_catalog()))
  })
})

testthat::test_that("failed builds and publication keep the previous product intact", {
  ctx <- real_sesai_database()
  db <- .integration$database
  before <- tools::md5sum(db)
  fail <- function(...) stop("injected failure")
  with_sesai_binding("sesai_read_territories", fail, {
    testthat::expect_error(.sesai$update_sesai_forecast(ctx$work, source_dir, force = TRUE), "injected failure")
  })
  with_sesai_binding("sesai_publish_database", fail, {
    testthat::expect_error(silence_raster_crs_warning(.sesai$update_sesai_forecast(ctx$work, source_dir, force = TRUE)), "injected failure")
  })
  testthat::expect_identical(tools::md5sum(db), before)
  testthat::expect_false(dir.exists(file.path(ctx$work, ".cams_forecast_sesai.lock")))
  testthat::expect_length(list.files(ctx$work, pattern = "^\\.cams_forecast_sesai-", all.files = TRUE), 0L)
})

testthat::test_that("input changes during processing cancel publication", {
  ctx <- real_sesai_database()
  before <- tools::md5sum(.integration$database)
  original <- .sesai$sesai_build_database
  builder <- function(...) {
    original(...)
    writeLines("2026-07-21T12:00", file.path(ctx$work, ".cams_generation"))
  }
  marker <- file.path(ctx$work, ".cams_generation")
  old_mtime <- file.info(marker)$mtime
  on.exit({writeLines(ctx$snapshot$cycle_id, marker); Sys.setFileTime(marker, old_mtime)})
  with_sesai_binding("sesai_build_database", builder, {
    testthat::expect_error(silence_raster_crs_warning(.sesai$update_sesai_forecast(ctx$work, source_dir, force = TRUE)),
                           "changed during processing")
  })
  testthat::expect_identical(tools::md5sum(.integration$database), before)
})
