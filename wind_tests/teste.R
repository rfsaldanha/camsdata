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

# content <- "~/Downloads/leaflet-velocity-master/demo/wind-gbr.json"

content <- "forecast_data/wind.json"

opts <- velocityOptions(
  speedUnit = "m/s",
  colorScale = colorRampPalette(c("gray50", "red"), alpha = TRUE)(8),
  minVelocity = 0,
  maxVelocity = 36,
  velocityScale = 0.01
)

leaflet() %>%
  addTiles(group = "base") %>%
  addProviderTiles(providers$OpenTopoMap, group = "topo") %>%
  addVelocity(
    content = content,
    group = "velo",
    layerId = "veloid",
    options = opts
  ) %>%
  addLayersControl(baseGroups = c("topo", "base"), overlayGroups = "velo")
