# This module has no top-level I/O. The municipal publisher loads/runs it only
# after committing its own generation (also on the already-published path).
sesai_processing_version <- "1"

sesai_source_paths <- function(source_dir) {
  c(
    dsei = file.path(source_dir, "36_DSEI", "36DSEI.shp"),
    polo = file.path(source_dir, "POLOS_2026", "POLOS_BASE_AGOSTO_2025.shp")
  )
}

sesai_normalize_name <- function(x) {
  trimws(gsub("[[:space:]]+", " ", toupper(iconv(x, to = "ASCII//TRANSLIT"))))
}

sesai_snapshot <- function(data_dir, source_dir, catalog) {
  marker <- file.path(data_dir, ".cams_generation")
  cycle <- trimws(readLines(marker, warn = FALSE))
  if (length(cycle) != 1L || !grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}T(00|12):00$", cycle)) {
    stop("Missing or invalid published CAMS cycle: ", marker)
  }
  start <- as.POSIXct(cycle, format = "%Y-%m-%dT%H:%M", tz = "UTC")
  if (is.na(start) || format(start, "%Y-%m-%dT%H:%M", tz = "UTC") != cycle) {
    stop("Invalid CAMS cycle date: ", cycle)
  }
  sources <- sesai_source_paths(source_dir)
  components <- unlist(lapply(sources, function(p) {
    paste0(tools::file_path_sans_ext(p), c(".shp", ".dbf", ".shx", ".prj", ".cpg"))
  }), use.names = FALSE)
  files <- c(marker, file.path(data_dir, catalog$filename), components)
  missing <- files[!file.exists(files)]
  if (length(missing)) stop("Missing SESAI input files: ", paste(missing, collapse = ", "))
  info <- file.info(files)
  inputs <- data.frame(
    kind = c("cycle", rep("raster", nrow(catalog)), rep(names(sources), each = 5L)),
    filename = basename(files),
    sha256 = vapply(files, digest::digest, character(1), algo = "sha256", file = TRUE),
    bytes = as.numeric(info$size),
    mtime = as.numeric(info$mtime),
    # vapply names its output with absolute/relative paths. Those names must
    # not become row names in the fingerprint of otherwise identical inputs.
    row.names = NULL, stringsAsFactors = FALSE
  )
  versions <- vapply(names(sources), function(type) {
    rows <- inputs[inputs$kind == type, ]
    digest::digest(paste(rows$filename, rows$sha256, collapse = "\n"),
                   algo = "sha256", serialize = FALSE)
  }, character(1))
  list(
    cycle_id = cycle, start = start, inputs = inputs, source_versions = versions,
    signature = digest::digest(list(inputs, catalog), algo = "sha256")
  )
}

sesai_read_territories <- function(source_dir, snapshot) {
  paths <- sesai_source_paths(source_dir)
  layers <- list()
  excluded <- list()
  sources <- list()
  for (type in names(paths)) {
    filename <- file.path(basename(dirname(paths[[type]])), basename(paths[[type]]))
    x <- sf::st_read(paths[[type]], quiet = TRUE, stringsAsFactors = FALSE)
    required <- if (type == "dsei") c("dsei", "cod_dsei") else c("polo", "cod_polo", "cod_dsei")
    if (!all(required %in% names(x))) stop("Missing source attributes: ", filename)
    if (is.na(sf::st_crs(x))) stop("Missing source CRS: ", filename)
    n_input <- nrow(x)
    x$source_row <- seq_len(n_input)
    x$source_file <- filename
    x$source_sha256 <- snapshot$source_versions[[type]]
    x$territory_id <- paste(type, x$source_sha256, x$source_row, sep = ":")
    connection <- rep(FALSE, n_input)
    if (type == "polo") {
      name <- sesai_normalize_name(x$polo)
      connection <- !is.na(name) & name == "TERRITORIO DE CONEXAO"
    }
    empty <- sf::st_is_empty(x) & !connection
    remove <- connection | empty
    excluded[[type]] <- data.frame(
      territory_type = rep(type, sum(remove)), territory_id = x$territory_id[remove],
      source_file = x$source_file[remove], source_row = x$source_row[remove],
      reason = ifelse(connection[remove], "connection_territory", "empty_geometry")
    )
    message(sprintf("SESAI %s: %d source features; excluded %d connection territories and %d empty geometries.",
                    type, n_input, sum(connection), sum(empty)))
    x <- x[!remove, , drop = FALSE]
    if (!nrow(x)) stop("No usable territories in ", filename)
    if (!all(sf::st_geometry_type(x) %in% c("POLYGON", "MULTIPOLYGON"))) {
      stop("Non-polygon source geometry in ", filename)
    }
    # Repair in a planar CRS, without simplifying boundaries or dissolving rows.
    x <- sf::st_make_valid(sf::st_transform(sf::st_zm(x, what = "ZM"), 5880))
    geometry <- sf::st_geometry(x)
    collections <- which(sf::st_geometry_type(geometry) == "GEOMETRYCOLLECTION")
    # Repair can yield collections. Extract each row individually so that a
    # multipart polygon can never become multiple territorial records.
    for (i in collections) {
      polygons <- sf::st_collection_extract(geometry[i], "POLYGON", warn = FALSE)
      geometry[i] <- sf::st_cast(sf::st_combine(polygons), "MULTIPOLYGON", warn = FALSE)
    }
    sf::st_geometry(x) <- sf::st_cast(geometry, "MULTIPOLYGON", warn = FALSE)
    if (nrow(x) != n_input - sum(remove) ||
        any(sf::st_is_empty(x)) || anyNA(sf::st_is_valid(x)) || !all(sf::st_is_valid(x))) {
      stop("Unrecoverable territory geometry in ", filename)
    }
    layers[[type]] <- sf::st_transform(x, 4326)
    sources[[type]] <- data.frame(
      territory_type = type, source_file = filename,
      source_sha256 = snapshot$source_versions[[type]], input_count = n_input,
      connection_excluded = sum(connection), empty_excluded = sum(empty),
      territory_count = nrow(x)
    )
  }
  list(layers = layers, sources = do.call(rbind, sources), exclusions = do.call(rbind, excluded))
}

sesai_dates <- function(start, step) {
  lubridate::with_tz(start + seq.int(0L, 120L, by = step) * 3600, "America/Sao_Paulo")
}

sesai_zonal_values <- function(rst, territories, dates, scale, offset) {
  if (terra::nlyr(rst) != length(dates)) stop("Unexpected forecast raster layer count.")
  if (!terra::same.crs(rst, territories)) {
    territories <- sf::st_transform(territories, sf::st_crs(terra::crs(rst)))
  }
  means <- as.matrix(exactextractr::exact_extract(rst, territories, "mean", progress = FALSE))
  values <- round(as.vector(means) * scale + offset, digits = 2)
  values[!is.finite(values)] <- NA_real_
  tibble::tibble(
    territory_id = rep(territories$territory_id, times = terra::nlyr(rst)),
    date = rep(dates, each = nrow(territories)), value = values
  )
}

sesai_connect <- function(path, read_only = FALSE) {
  DBI::dbConnect(duckdb::duckdb(), path, read_only = read_only)
}

sesai_validate_database <- function(con, snapshot, catalog) {
  quote <- function(x) as.character(DBI::dbQuoteIdentifier(con, x))
  metadata <- DBI::dbReadTable(con, "sesai_metadata")
  if (nrow(metadata) != 1L || metadata$cycle_id != snapshot$cycle_id ||
      metadata$processing_version != sesai_processing_version ||
      metadata$input_signature != snapshot$signature) stop("Outdated SESAI generation.")
  inputs <- DBI::dbReadTable(con, "sesai_inputs")
  if (!isTRUE(all.equal(inputs, snapshot$inputs, check.attributes = FALSE))) {
    stop("SESAI input inventory differs from the published rasters/sources.")
  }
  sources <- DBI::dbReadTable(con, "sesai_sources")
  exclusions <- DBI::dbReadTable(con, "sesai_exclusions")
  if (nrow(sources) != 2L || !setequal(sources$territory_type, c("dsei", "polo"))) {
    stop("Incomplete SESAI source inventory.")
  }
  indexes <- DBI::dbGetQuery(con, "SELECT table_name, is_unique FROM duckdb_indexes()")
  for (type in c("dsei", "polo")) {
    src <- sources[sources$territory_type == type, ]
    registry <- paste0(type, "_territories")
    territories <- DBI::dbReadTable(con, registry)
    n <- nrow(territories)
    required <- c("territory_id", "source_row", "source_file", "source_sha256", "geometry_wkb", "geometry_epsg")
    if (!all(required %in% names(territories)) || !n || n != src$territory_count ||
        anyNA(territories$territory_id) || anyDuplicated(territories$territory_id) ||
        any(territories$source_sha256 != snapshot$source_versions[[type]]) ||
        any(territories$geometry_epsg != 4326L) ||
        any(lengths(territories$geometry_wkb) == 0L)) stop("Invalid SESAI registry: ", registry)
    if (src$source_sha256 != snapshot$source_versions[[type]] ||
        n + src$connection_excluded + src$empty_excluded != src$input_count ||
        sum(exclusions$territory_type == type & exclusions$reason == "connection_territory") != src$connection_excluded ||
        sum(exclusions$territory_type == type & exclusions$reason == "empty_geometry") != src$empty_excluded) {
      stop("Invalid SESAI exclusion counts: ", type)
    }
    for (i in seq_len(nrow(catalog))) {
      cfg <- catalog[i, ]
      table <- sub("_mun_", paste0("_", type, "_"), cfg$table, fixed = TRUE)
      schema <- DBI::dbGetQuery(con, paste("DESCRIBE", quote(table)))
      if (!identical(schema$column_name, c("territory_id", "date", "value")) ||
          !identical(schema$column_type, c("VARCHAR", "TIMESTAMP", "DOUBLE"))) {
        stop("Invalid SESAI series schema: ", table)
      }
      dates <- sesai_dates(snapshot$start, cfg$step)
      counts <- DBI::dbGetQuery(con, paste0(
        "SELECT date, count(*) AS n, count(DISTINCT territory_id) AS ids FROM ",
        quote(table), " GROUP BY date ORDER BY date"
      ))
      if (nrow(counts) != length(dates) || any(counts$n != n) || any(counts$ids != n) ||
          !identical(as.numeric(counts$date), as.numeric(dates))) {
        stop("Incomplete SESAI series: ", table)
      }
      bad <- DBI::dbGetQuery(con, paste0(
        "SELECT count(*) AS n FROM ", quote(table), " s LEFT JOIN ", quote(registry),
        " t USING (territory_id) WHERE t.territory_id IS NULL OR ",
        "(s.value IS NOT NULL AND NOT isfinite(s.value))"
      ))$n
      if (bad != 0 || !any(indexes$table_name == table & indexes$is_unique)) {
        stop("Invalid SESAI values, keys or index: ", table)
      }
    }
  }
  invisible(TRUE)
}

sesai_is_current <- function(path, snapshot, catalog) {
  if (!file.exists(path)) return(FALSE)
  tryCatch({
    con <- sesai_connect(path, read_only = TRUE)
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    sesai_validate_database(con, snapshot, catalog)
    TRUE
  }, error = function(error) {
    message("SESAI database needs rebuilding: ", conditionMessage(error))
    FALSE
  })
}

sesai_build_database <- function(path, data_dir, territories, snapshot, catalog) {
  con <- sesai_connect(path)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "sesai_metadata", data.frame(
    cycle_id = snapshot$cycle_id, processing_version = sesai_processing_version,
    input_signature = snapshot$signature,
    created_at_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  ))
  DBI::dbWriteTable(con, "sesai_sources", territories$sources)
  DBI::dbWriteTable(con, "sesai_exclusions", territories$exclusions)
  DBI::dbWriteTable(con, "sesai_inputs", snapshot$inputs)
  for (type in names(territories$layers)) {
    x <- territories$layers[[type]]
    registry <- sf::st_drop_geometry(x)
    registry$geometry_wkb <- blob::as_blob(unclass(sf::st_as_binary(sf::st_geometry(x), EWKB = FALSE)))
    registry$geometry_epsg <- rep(4326L, nrow(x))
    table <- paste0(type, "_territories")
    DBI::dbWriteTable(con, table, registry)
    DBI::dbExecute(con, sprintf('CREATE UNIQUE INDEX "%s_id_idx" ON "%s" (territory_id)', table, table))
  }
  for (i in seq_len(nrow(catalog))) {
    cfg <- catalog[i, ]
    message("SESAI zonal means: ", cfg$label)
    rst <- terra::rast(file.path(data_dir, cfg$filename))
    dates <- sesai_dates(snapshot$start, cfg$step)
    for (type in names(territories$layers)) {
      table <- sub("_mun_", paste0("_", type, "_"), cfg$table, fixed = TRUE)
      result <- sesai_zonal_values(rst, territories$layers[[type]], dates, cfg$scale, cfg$offset)
      DBI::dbWriteTable(con, table, result)
      DBI::dbExecute(con, sprintf(
        'CREATE UNIQUE INDEX "%s_id_date_idx" ON "%s" (territory_id, date)', table, table
      ))
    }
  }
  sesai_validate_database(con, snapshot, catalog)
  DBI::dbExecute(con, "CHECKPOINT")
  invisible(NULL)
}

sesai_publish_database <- function(staged, destination) {
  # Same filesystem: never remove the previous database before replacement.
  if (!file.rename(staged, destination)) stop("Could not atomically publish the SESAI database.")
}

update_sesai_forecast <- function(data_dir, source_dir, catalog = forecast_catalog(), force = FALSE) {
  destination <- file.path(data_dir, "cams_forecast_sesai.duckdb")
  lock <- file.path(data_dir, ".cams_forecast_sesai.lock")
  if (!dir.create(lock, showWarnings = FALSE)) {
    stop("SESAI writer lock already exists (or data directory is not writable): ", lock)
  }
  on.exit(unlink(lock, recursive = TRUE), add = TRUE)
  snapshot <- sesai_snapshot(data_dir, source_dir, catalog)
  assert_unchanged <- function() {
    latest <- sesai_snapshot(data_dir, source_dir, catalog)
    if (!identical(snapshot$signature, latest$signature)) {
      stop("CAMS rasters, cycle or SESAI sources changed during processing; publication cancelled.")
    }
  }
  if (!force && sesai_is_current(destination, snapshot, catalog)) {
    assert_unchanged()
    message("SESAI forecast already complete for ", snapshot$cycle_id)
    return(invisible(list(status = "current", path = destination)))
  }
  territories <- sesai_read_territories(source_dir, snapshot)
  staged <- tempfile(".cams_forecast_sesai-", tmpdir = data_dir, fileext = ".duckdb")
  on.exit(unlink(c(staged, paste0(staged, ".wal"))), add = TRUE)
  sesai_build_database(staged, data_dir, territories, snapshot, catalog)
  # Reopen read-only to check the actual closed file that will be published.
  if (!sesai_is_current(staged, snapshot, catalog)) stop("SESAI staged database failed validation.")
  assert_unchanged()
  sesai_publish_database(staged, destination)
  message("Published SESAI forecast for ", snapshot$cycle_id, ": ", destination)
  invisible(list(status = "published", path = destination))
}
