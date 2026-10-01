testthat::test_that("unusable source geometry preserves the last published database", {
  ctx <- real_sesai_database()
  before <- tools::md5sum(.integration$database)
  modified <- tempfile("sesai-bad-geometry-")
  dir.create(modified)
  stopifnot(all(file.copy(list.files(source_dir, full.names = TRUE), modified, recursive = TRUE)))
  path <- .sesai$sesai_source_paths(modified)[["dsei"]]
  real_polygon <- sf::st_read(path, quiet = TRUE)[1, ]
  boundary <- sf::st_boundary(sf::st_transform(real_polygon, 5880))
  sf::st_write(boundary, path, delete_layer = TRUE, quiet = TRUE, layer_options = "ENCODING=UTF-8")
  testthat::expect_error(.sesai$update_sesai_forecast(ctx$work, modified), "Non-polygon source geometry")
  testthat::expect_identical(tools::md5sum(.integration$database), before)
  testthat::expect_false(dir.exists(file.path(ctx$work, ".cams_forecast_sesai.lock")))
})

testthat::test_that("unexpected layer counts and invalid cycle markers are rejected", {
  ctx <- real_sesai_database()
  raster <- silence_raster_crs_warning(terra::rast(file.path(ctx$work, "cams_forecast_pm25.nc"))[[1]])
  testthat::expect_error(.sesai$sesai_zonal_values(raster, ctx$territories$layers$dsei,
    .sesai$sesai_dates(ctx$snapshot$start, 1), 1e9, 0), "layer count")
  invalid <- tempfile("invalid-cycle-")
  dir.create(invalid)
  writeLines("2026-02-31T00:00", file.path(invalid, ".cams_generation"))
  testthat::expect_error(.sesai$sesai_snapshot(invalid, source_dir, .sesai$forecast_catalog()), "Invalid CAMS cycle date")
})

testthat::test_that("an existing SESAI writer lock is respected", {
  ctx <- real_sesai_database()
  lock <- file.path(ctx$work, ".cams_forecast_sesai.lock")
  dir.create(lock)
  on.exit(unlink(lock, recursive = TRUE))
  testthat::expect_error(.sesai$update_sesai_forecast(ctx$work, source_dir), "writer lock already exists")
  testthat::expect_true(dir.exists(lock))
})
