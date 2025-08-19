# https://github.com/smlum/netcdf-vis/blob/master/app/data/netcdf2leaflet.py

from netCDF4 import Dataset, num2date
import numpy as np
import json

# import data
dataset_u = Dataset('forecast_data/cams_forecast_wind_u.nc')
dataset_v = Dataset('forecast_data/cams_forecast_wind_v.nc')

# u and v wind variables
u_var = 'u10'
v_var = 'v10'

# set header variables for wind
nx = dataset_u.variables[u_var].shape[3]
ny = dataset_u.variables[u_var].shape[2]
dx = .4
dy = .4
tot = nx * ny

# get data for u wind
a = dataset_u.variables[u_var][:][0]
A = np.matrix(a)
b = A.flatten()
c = np.ravel(b).T
u_data = c.tolist()
u_data = np.around(u_data, 2).tolist()

# get data for v wind
a = dataset_v.variables[v_var][:][0]
A = np.matrix(a)
b = A.flatten()
c = np.ravel(b).T
v_data = c.tolist()
v_data = np.around(v_data, 2).tolist()

wind_data = [{
  "header": {
    "parameterNumberName": "eastward_wind",
    "parameterUnit": "m.s-1",
    "parameterNumber": 2,
    "parameterCategory": 2,
    "nx": nx,
    "ny": ny,
    "numberPoints": tot,
    "dx": dx,
    "dy": dy,
    "la1": 13.51,
    "lo1": -83.35,
    "la2": -56.89,
    "lo2": -32.15,
    "refTime": "2017-02-01 23:00:00"
  },
  "data": u_data
}, {
  "header": {
    "parameterNumberName": "northward_wind",
    "parameterUnit": "m.s-1",
    "parameterNumber": 3,
    "parameterCategory": 2,
    "nx": nx,
    "ny": ny,
    "numberPoints": tot,
    "dx": dx,
    "dy": dy,
    "la1": 13.51,
    "lo1": -83.35,
    "la2": -56.89,
    "lo2": -32.15,
    "refTime": "2017-02-01 23:00:00"
  },
  "data": v_data
}]

# write JSON for leaflet-velocity input
with open('forecast_data/wind.json', 'w') as outfile:  
    json.dump(wind_data, outfile, separators=(',', ':'))
