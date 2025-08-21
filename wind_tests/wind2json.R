rst_u <- terra::rast("forecast_data/cams_forecast_wind_u.nc")
rst_v <- terra::rast("forecast_data/cams_forecast_wind_v.nc")

wind2json <- function(rst_u, rst_v, depth, n_round = 2, path) {
  # Header variables
  nx <- terra::ncol(rst_u)
  ny <- terra::nrow(rst_u)
  dx <- terra::res(rst_u)[1]
  dy <- terra::res(rst_u)[2]
  tot <- nx * ny
  la1 <- terra::ext(rst_u)[4]
  lo1 <- terra::ext(rst_u)[1]
  la2 <- terra::ext(rst_u)[3]
  lo2 <- terra::ext(rst_u)[2]

  # Data
  data_u <- round(as.vector(t(terra::as.matrix(rst_u[[depth]]))), n_round)
  data_v <- round(as.vector(t(terra::as.matrix(rst_v[[depth]]))), n_round)

  # List
  wind_list <- list(
    list(
      "header" = list(
        "parameterNumberName" = "eastward_wind",
        "parameterUnit" = "m.s-1",
        "parameterNumber" = 2,
        "parameterCategory" = 2,
        "nx" = nx,
        "ny" = ny,
        "numberPoints" = tot,
        "dx" = dx,
        "dy" = dy,
        "la1" = la1,
        "lo1" = lo1,
        "la2" = la2,
        "lo2" = lo2,
        "refTime" = "2017-02-01 23:00:00"
      ),
      "data" = data_u
    ),
    list(
      "header" = list(
        "parameterNumberName" = "northward_wind",
        "parameterUnit" = "m.s-1",
        "parameterNumber" = 3,
        "parameterCategory" = 2,
        "nx" = nx,
        "ny" = ny,
        "numberPoints" = tot,
        "dx" = dx,
        "dy" = dy,
        "la1" = la1,
        "lo1" = lo1,
        "la2" = la2,
        "lo2" = lo2,
        "refTime" = "2017-02-01 23:00:00"
      ),
      "data" = data_v
    )
  )

  # Json
  # wind_json <- rjson::toJSON(x = wind_u_list)
  wind_json <- jsonlite::toJSON(x = wind_list, auto_unbox = TRUE)

  # Write
  write(x = wind_json, file = path)

  # Return
  return(TRUE)
}

wind2json(
  rst_u = rst_u,
  rst_v = rst_v,
  depth = 1,
  n_round = 2,
  path = "forecast_data/wind_teste2.json"
)
