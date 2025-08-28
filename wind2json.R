wind2json <- function(rst_u, rst_v, depth, n_round = 2, ms2kmh = TRUE, path) {
  # Verify compatibility between rst files
  if (
    !any(
      terra::ncol(rst_u) == terra::ncol(rst_v),
      terra::nrow(rst_u) == terra::nrow(rst_v),
      terra::res(rst_u) == terra::res(rst_v),
      terra::ext(rst_u) == terra::ext(rst_v)
    )
  ) {
    stop(
      "The rst_u and rst_v files must have the same dimensions and resolution (same number of columns, number of rows, spatial resolution and boundaries extend)."
    )
  }

  # Header variables
  nx <- terra::ncol(rst_u) # Number of rows
  ny <- terra::nrow(rst_u) # Number of columns
  dx <- terra::res(rst_u)[1] # X spatial resolution, in degrees
  dy <- terra::res(rst_u)[2] # Y spatial resolution, in degrees
  tot <- nx * ny # Number of observations
  la1 <- terra::ext(rst_u)[4] # Bounding box
  lo1 <- terra::ext(rst_u)[1]
  la2 <- terra::ext(rst_u)[3]
  lo2 <- terra::ext(rst_u)[2]
  parameterUnit <- "m.s-1"

  # Data
  data_u <- round(as.vector(t(terra::as.matrix(rst_u[[depth]]))), n_round)
  data_v <- round(as.vector(t(terra::as.matrix(rst_v[[depth]]))), n_round)

  # Speed conversion
  if (ms2kmh) {
    data_u <- data_u * 3.6
    data_v <- data_v * 3.6
    parameterUnit <- "km.h-1"
  }

  # List
  wind_list <- list(
    list(
      "header" = list(
        "parameterNumberName" = "eastward_wind",
        "parameterUnit" = parameterUnit,
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
        "parameterUnit" = parameterUnit,
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
  wind_json <- jsonlite::toJSON(x = wind_list, auto_unbox = TRUE)

  # Write
  write(x = wind_json, file = path)

  # Return
  return(TRUE)
}
