# Regression baseline captured from cams_forecast.R before the SESAI change.
# Keep independent of R/forecast_catalog.R and the production aggregation.

legacy_municipal_aggregate <- function(label, filename, table,
    step, scale = 1, offset = 0) {
    cli_h3(label)
    cli_alert("Reading and aggregating forecast file...")
    rst <- terra::rast(path(dir_data, filename))
    if (!terra::same.crs(rst, mun)) {
        cli_alert("Projecting raster file...")
        rst <- terra::project(rst, terra::crs(mun))
    }
    layer_dates <- forecast_sequence(step)
    if (terra::nlyr(rst) != length(layer_dates)) {
        cli_abort("Unexpected layer count for {label}: {terra::nlyr(rst)}.")
    }
    means <- as.matrix(exact_extract(rst, mun, "mean", progress = FALSE))
    result <- tibble(code_muni = rep(mun$code_muni, times = terra::nlyr(rst)),
        date = rep(with_tz(layer_dates, "America/Sao_Paulo"),
            each = nrow(mun)), value = round(as.vector(means) *
            scale + offset, digits = 2))
    dbWriteTable(con, table, result, overwrite = TRUE)
    quoted_table <- as.character(dbQuoteIdentifier(con, table))
    quoted_index <- as.character(dbQuoteIdentifier(con, paste0(table,
        "_code_date_idx")))
    dbExecute(con, paste0("CREATE INDEX ", quoted_index, " ON ",
        quoted_table, " (code_muni, date)"))
    cli_alert_success("Done: {nrow(result)} rows.")
    invisible(NULL)
}

legacy_catalog <-
structure(list(label = c("Instantaneous air-quality indicator",
"PM 2.5", "PM 10", "O3", "CO", "NO2", "SO2", "Temperature", "UV",
"Wind speed", "Aerosol", "Precipitation"), filename = c("iqar.nc",
"cams_forecast_pm25.nc", "cams_forecast_pm10.nc", "cams_forecast_o3_mc.nc",
"cams_forecast_co_mc.nc", "cams_forecast_no2_mc.nc", "cams_forecast_so2_mc.nc",
"cams_forecast_temp.nc", "cams_forecast_uv.nc", "cams_forecast_wind_speed.nc",
"cams_forecast_aerosol.nc", "cams_forecast_prec.nc"), table = c("iqar_mun_forecast",
"pm25_mun_forecast", "pm10_mun_forecast", "o3_mun_forecast",
"co_mun_forecast", "no2_mun_forecast", "so2_mun_forecast", "temp_mun_forecast",
"uv_mun_forecast", "wind_speed_mun_forecast", "aerosol_mun_forecast",
"prec_mun_forecast"), step = c(3L, 1L, 1L, 3L, 3L, 3L, 3L, 1L,
1L, 1L, 1L, 1L), scale = c(1, 1e+09, 1e+09, 1e+09, 1, 1e+09,
1e+09, 1, 40, 1, 1, 1000), offset = c(0, 0, 0, 0, 0, 0, 0, -273.15,
0, 0, 0, 0)), row.names = c(NA, -12L), class = c("tbl_df", "tbl",
"data.frame"))
