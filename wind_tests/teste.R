# https://github.com/IrishMarineInstitute/erddap-leaflet-velocity-demo
# https://github.com/smlum/netcdf-vis/blob/master/app/data/netcdf2leaflet.py
# https://stackoverflow.com/questions/46699194/leaflet-velocity-cannot-read-property-data-of-null
# https://stackoverflow.com/questions/50661776/formatting-json-for-leaflet-wind-map
# https://stackoverflow.com/questions/65275217/data-from-erddap-to-leaflet-velocity-map
# https://github.com/cambecc/grib2json
# https://github.com/danwild/wind-js-server
# https://github.com/onaci/leaflet-velocity
# https://trafficonese.github.io/leaflet.extras2/reference/velocityOptions.html

library(leaflet)
library(leaflet.extras2)
library(terra)

rst_iqar <- rast("forecast_data/iqar.nc")
rst_iqar <- project(x = rst_iqar, "EPSG:3857")

rst_aerosol <- rast("forecast_data/cams_forecast_aerosol.nc")
rst_aerosol <- project(x = rst_aerosol, "EPSG:3857")

pal_iqar <- colorBin(
  palette = c("green", "yellow", "orange", "red", "purple"),
  bins = c(0, 40, 80, 120, 200, Inf),
  na.color = NA,
  reverse = FALSE
)

pal_aerosol <- colorBin(
  palette = "magma",
  bins = c(.1, .2, .3, .4, .6, .8, 1, 3, Inf),
  na.color = NA,
  reverse = TRUE
)

content <- "forecast_data/wind_teste2.json"

# opts <- velocityOptions(
#   speedUnit = "m/s",
#   colorScale = colorRampPalette(c("gray50", "black"), alpha = TRUE)(5),
#   minVelocity = 0,
#   maxVelocity = 36,
#   velocityScale = 0.01
# )

opts <- velocityOptions(
  speedUnit = "k/h",
  colorScale = colorRampPalette(c("gray50", "black"), alpha = TRUE)(5),
  minVelocity = 0,
  maxVelocity = 100,
  velocityScale = 0.002
)


leaflet() |>
  addTiles(group = "base") %>%
  addProviderTiles(providers$OpenTopoMap, group = "topo") %>%
  addVelocity(
    content = content,
    group = "velo",
    layerId = "veloid",
    options = opts
  ) |>
  addRasterImage(
    x = rst_iqar[[1]],
    opacity = .7,
    colors = pal_iqar,
    layerId = "iqar",
    project = FALSE,
    group = "iqar"
  ) |>
  addRasterImage(
    x = rst_aerosol[[1]],
    opacity = .7,
    colors = pal_aerosol,
    layerId = "aerosol",
    project = FALSE,
    group = "aerosol"
  ) %>%
  addLayersControl(
    baseGroups = c("topo", "base"),
    overlayGroups = c("velo", "iqar", "aerosol")
  )
