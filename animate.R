# Packages
library(terra)
library(gifski)

# Reference forecast file
file <- "/media/raphaelsaldanha/lacie/cams_forecast_pm25/cams_forecast_pm25_20250709.nc"

# Read file
rst <- rast(file) * 1000000000
rst <- project(x = rst, "EPSG:4326")

save_gif(animate(rst, pause = .5), gif_file = "test.gif")

library(leaflet)
library(RColorBrewer)

mm <- minmax(rst)
pal <- colorNumeric(
  palette = "Spectral",
  domain = c(min(t(mm)[, 1]), max(t(mm)[, 2]))
)

leaflet() |>
  addTiles() |>
  addRasterImage(rst[[1]], opacity = .7, colors = pal, ) |>
  addLegend(pal = pal, values = c(min(t(mm)[, 1]), max(t(mm)[, 2])))
