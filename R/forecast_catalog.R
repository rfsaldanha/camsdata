# Shared by the unchanged municipal aggregation and the SESAI aggregation.
# Dates are forecast-cycle UTC instants; writers retain the historical
# America/Sao_Paulo POSIXct representation before sending them to DuckDB.
forecast_catalog <- function() {
  tibble::tribble(
    ~label, ~filename, ~table, ~step, ~scale, ~offset,
    "Instantaneous air-quality indicator", "iqar.nc", "iqar_mun_forecast", 3L, 1, 0,
    "PM 2.5", "cams_forecast_pm25.nc", "pm25_mun_forecast", 1L, 1e9, 0,
    "PM 10", "cams_forecast_pm10.nc", "pm10_mun_forecast", 1L, 1e9, 0,
    "O3", "cams_forecast_o3_mc.nc", "o3_mun_forecast", 3L, 1e9, 0,
    "CO", "cams_forecast_co_mc.nc", "co_mun_forecast", 3L, 1, 0,
    "NO2", "cams_forecast_no2_mc.nc", "no2_mun_forecast", 3L, 1e9, 0,
    "SO2", "cams_forecast_so2_mc.nc", "so2_mun_forecast", 3L, 1e9, 0,
    "Temperature", "cams_forecast_temp.nc", "temp_mun_forecast", 1L, 1, -273.15,
    "UV", "cams_forecast_uv.nc", "uv_mun_forecast", 1L, 40, 0,
    "Wind speed", "cams_forecast_wind_speed.nc", "wind_speed_mun_forecast", 1L, 1, 0,
    "Aerosol", "cams_forecast_aerosol.nc", "aerosol_mun_forecast", 1L, 1, 0,
    "Precipitation", "cams_forecast_prec.nc", "prec_mun_forecast", 1L, 1e3, 0
  )
}
