library(terra)

wind_u <- rast("forecast_data/cams_forecast_wind_u.nc")
wind_v <- rast("forecast_data/cams_forecast_wind_v.nc")

wind <- c(wind_u, wind_v)
writeCDF(x = wind, filename = "forecast_data/wind.nc", overwrite = TRUE)

teste <- rast("forecast_data/wind.nc")
