rst_pm25 <- terra::rast("../camsdata/forecast_data/cams_forecast_pm25.nc")
rst_pm10 <- terra::rast("../camsdata/forecast_data/cams_forecast_pm10.nc")
rst_o3 <- terra::rast("../camsdata/forecast_data/cams_forecast_o3_mc.nc")
rst_co <- terra::rast("../camsdata/forecast_data/cams_forecast_co_mc.nc")
rst_no2 <- terra::rast("../camsdata/forecast_data/cams_forecast_no2_mc.nc")
rst_so2 <- terra::rast("../camsdata/forecast_data/cams_forecast_so2_mc.nc")

f <- function(x, pol) {
  sapply(X = x, FUN = riqar::iqar_pol, pol = pol)
}

tictoc::tic()
iqar_pm25 <- terra::app(
  x = rst_pm25 * 1e9,
  fun = f,
  pol = "pm2.5",
  cores = 12
)
tictoc::toc()

iqar_pm10 <- terra::app(
  x = rst_pm10 * 1e9,
  fun = f,
  pol = "pm10",
  cores = 4
)

iqar_o3 <- terra::app(
  x = rst_o3 * 1e9,
  fun = f,
  pol = "o3",
  cores = 4
)

iqar_co <- terra::app(
  x = rst_co,
  fun = f,
  pol = "co",
  cores = 4
)

iqar_no2 <- terra::app(
  x = rst_no2 * 1e9,
  fun = f,
  pol = "no2",
  cores = 4
)

iqar_so2 <- terra::app(
  x = rst_so2 * 1e9,
  fun = f,
  pol = "so2",
  cores = 4
)
tictoc::toc()

terra::plot(iqar_pm25)
terra::plot(iqar_pm10)
terra::plot(iqar_o3)
terra::plot(iqar_co)
terra::plot(iqar_no2)
terra::plot(iqar_so2)

iqar <- terra::app(
  x = c(
    iqar_pm25[[seq(1, 121, 3)]],
    iqar_pm10[[seq(1, 121, 3)]],
    iqar_o3,
    iqar_co,
    iqar_no2,
    iqar_so2
  ),
  fun = max
)

iqar <- NULL
for (i in 1:41) {
  tmp <- terra::app(
    x = c(
      iqar_pm25[[seq(1, 121, 3)]][[i]],
      iqar_pm10[[seq(1, 121, 3)]][[i]],
      iqar_o3[[i]],
      iqar_co[[i]],
      iqar_no2[[i]],
      iqar_so2[[i]]
    ),
    fun = max
  )

  iqar <- c(iqar, tmp)
  rm(tmp)
}

iqar <- terra::rast(iqar)

terra::writeCDF(x = iqar, filename = "forecast_data/iqar.nc")

terra::plot(iqar)

library(leaflet)

pal <- colorBin(
  palette = c("green", "yellow", "orange", "red", "purple"),
  bins = c(0, 40, 80, 120, 200, Inf),
  na.color = NA,
  reverse = FALSE
)

leaflet() |>
  addTiles() |>
  addRasterImage(
    x = iqar[[1]],
    opacity = .7,
    colors = pal,
    layerId = "raster",
    project = FALSE,
    group = "raster"
  ) |>
  addLegend(
    colors = c("green", "yellow", "orange", "red", "purple"),
    layerId = "legend",
    title = paste0("IQAr - CONAMA"),
    labels = c("Boa", "Moderada", "Ruim", "Muito ruim", "Péssima")
  )
