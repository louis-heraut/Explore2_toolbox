# Copyright 2021-2023 Louis Héraut (louis.heraut@inrae.fr)*1,
#                     Éric Sauquet (eric.sauquet@inrae.fr)*1
#
# *1   INRAE, France
#
# This file is part of Explore2 R toolbox.
#
# Explore2 R toolbox is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# Explore2 R toolbox is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ash R toolbox.
# If not, see <https://www.gnu.org/licenses/>.


#  _          
# | | ___  ___
# | |/ -_)(_-<
# |_|\___|/__/
#                        _                                
#  __  ___  ___  _ _  __| | ___  _ _   _ _   ___  ___  ___
# / _|/ _ \/ _ \| '_|/ _` |/ _ \| ' \ | ' \ / -_)/ -_)(_-<
# \__|\___/\___/|_|  \__,_|\___/|_||_||_||_|\___|\___|/__/ ___________
# Les variables de coordonnées sont indispensables à la localisation
# des stations, elles sont donc unidimensionnelles fonction de
# l’élément ‘station’.
#
# Les stations doivent être incluses dans la couverture spatiale
# annoncée dans les métadonnées et nom du fichier.
#
# Paramètres :
#     standard_name : Un nom d’identification court de la coordonnée
#
#         long_name : Un nom d’identification long de la coordonnée
#
#             units : Spécifie l’unité de la variable coordonnée


WGS84_lon = c("WGS84_lon", "Lon", "lon")
WGS84_lon_model = c("WGS84_lon_model", "Lon_model", "lon_model")
WGS84_lat = c("WGS84_lat", "Lat", "lat")
WGS84_lat_model = c("WGS84_lat_model", "Lat_model", "lat_model")
L93_X = "L93_X"
L93_X_model = "L93_X_model"
L93_Y = "L93_Y"
L93_Y_model = "L93_Y_model"
LII_X = "LII_X"
LII_X_model = "LII_X_model"
LII_Y = "LII_Y"
LII_Y_model = "LII_Y_model"
topologicalSurface = c("topologicalSurface", "surface")
topologicalSurface_model = c("topologicalSurface_model", "surface_model")

V = c(
    "WGS84_lon", "WGS84_lon_model",
    "WGS84_lat", "WGS84_lat_model",
    "L93_X", "L93_X_model",
    "L93_Y", "L93_Y_model",
    "LII_X", "LII_X_model",
    "LII_Y", "LII_Y_model",
    "topologicalSurface", "topologicalSurface_model"
)

Code_nc = c()
for (v in V) {
    assign(paste0(v, "_data"), c())
}

for (x in data_paths) {
    NCx = ncdf4::nc_open(x)
    Code_nc = c(Code_nc,
                ncdf4::ncvar_get(NCx, "code"))

    for (v in V) {
        if (!any(get(v) %in% names(NC$var))) {
            next
        }
        ncvar = get(v)[get(v) %in% names(NCx$var)]
        ncdata = ncdf4::ncvar_get(NCx, ncvar)
        v_data = paste0(v, "_data")
        assign(v_data,
               c(get(v_data), ncdata))
    }
}

idCode_match = match(Code, Code_nc)
idCode_match = idCode_match[!is.na(idCode_match)]

for (v in V) {
    if (!any(get(v) %in% names(NC$var))) {
        next
    }
    v_data = paste0(v, "_data")
    assign(v_data,
           get(v_data)[idCode_match])
}


## 1. WGS 84 _____________________________________________________
### 1.1. WGS 84 lon ________________________________________________
#### 1.1.1. Real _____________________________________________________
if (any(WGS84_lon %in% names(NC$var))) {
    NCf$WGS84_lon.name = "WGS84_lon"
    NCf$WGS84_lon.dimension = "station"
    NCf$WGS84_lon.precision = "double"
    NCf$WGS84_lon.value = WGS84_lon_data
    NCf$WGS84_lon.01.standard_name = "longitude"
    NCf$WGS84_lon.02.long_name = "longitude coordinate in WGS84"
    NCf$WGS84_lon.03.units = "degrees_east"
} else {
    warning(paste0("no var in ",
                   paste0(WGS84_lon, collapse=" ")))
}
#### 1.1.2. Model _____________________________________________________
if (any(WGS84_lon_model %in% names(NC$var))) {
    NCf$WGS84_lon_model.name = "WGS84_lon_model"
    NCf$WGS84_lon_model.dimension = "station"
    NCf$WGS84_lon_model.precision = "double"
    NCf$WGS84_lon_model.value = WGS84_lon_model_data
    NCf$WGS84_lon_model.01.standard_name = "longitude model"
    NCf$WGS84_lon_model.02.long_name =
        "longitude coordinate in WGS84 in the model world"
    NCf$WGS84_lon_model.03.units = "degrees_east"
} else {
    warning(paste0("no var in ",
                   paste0(WGS84_lon_model, collapse=" ")))
}

### 1.2. WGS 84 lat ________________________________________________
#### 1.2.1. Real _____________________________________________________
if (any(WGS84_lat %in% names(NC$var))) {
    NCf$WGS84_lat.name = "WGS84_lat"
    NCf$WGS84_lat.dimension = "station"
    NCf$WGS84_lat.precision = "double"
    NCf$WGS84_lat.value = WGS84_lat_data
    NCf$WGS84_lat.01.standard_name = "latitude"
    NCf$WGS84_lat.02.long_name = "latitude coordinate in WGS84"
    NCf$WGS84_lat.03.units = "degrees_north"
} else {
    warning(paste0("no var in ",
                   paste0(WGS84_lat, collapse=" ")))
}
#### 1.2.2. Model ____________________________________________________
if (any(WGS84_lat_model %in% names(NC$var))) {
    NCf$WGS84_lat_model.name = "WGS84_lat_model"
    NCf$WGS84_lat_model.dimension = "station"
    NCf$WGS84_lat_model.precision = "double"
    NCf$WGS84_lat_model.value = WGS84_lat_model_data
    NCf$WGS84_lat_model.01.standard_name = "latitude model"
    NCf$WGS84_lat_model.02.long_name =
        "latitude coordinate in WGS84 in the model world"
    NCf$WGS84_lat_model.03.units = "degrees_north"
} else {
    warning(paste0("no var in ",
                   paste0(WGS84_lat_model, collapse=" ")))
}

## 2. LAMBERT-93 _____________________________________________________
### 2.1. Lambert-93 X ________________________________________________
#### 2.1.1. Real _____________________________________________________
if (any(L93_X %in% names(NC$var))) {
    NCf$L93_X.name = "L93_X"
    NCf$L93_X.dimension = "station"
    NCf$L93_X.precision = "double"
    NCf$L93_X.value = L93_X_data
    NCf$L93_X.01.standard_name = "X Lambert-93"
    NCf$L93_X.02.long_name = "horizontal coordinate in Lambert-93"
    NCf$L93_X.03.units = "m"
} else {
    warning("no var L93_X")
}
#### 2.1.2. Model _____________________________________________________
if (any(L93_X_model %in% names(NC$var))) {
    NCf$L93_X_model.name = "L93_X_model"
    NCf$L93_X_model.dimension = "station"
    NCf$L93_X_model.precision = "double"
    NCf$L93_X_model.value = L93_X_model_data
    NCf$L93_X_model.01.standard_name = "X Lambert-93 model"
    NCf$L93_X_model.02.long_name =
        "horizontal coordinate in Lambert-93 in the model world"
    NCf$L93_X_model.03.units = "m"
} else {
    warning("no var L93_X_model")
}

### 2.2. Lambert-93 Y ________________________________________________
#### 2.2.1. Real _____________________________________________________
if (any(L93_Y %in% names(NC$var))) {
    NCf$L93_Y.name = "L93_Y"
    NCf$L93_Y.dimension = "station"
    NCf$L93_Y.precision = "double"
    NCf$L93_Y.value = L93_Y_data
    NCf$L93_Y.01.standard_name = "Y Lambert-93"
    NCf$L93_Y.02.long_name = "vertical coordinate in Lambert-93"
    NCf$L93_Y.03.units = "m"
} else {
    warning("no var L93_Y")
}
#### 2.2.2. Model _____________________________________________________
if (any(L93_Y_model %in% names(NC$var))) {
    NCf$L93_Y_model.name = "L93_Y_model"
    NCf$L93_Y_model.dimension = "station"
    NCf$L93_Y_model.precision = "double"
    NCf$L93_Y_model.value = L93_Y_model_data
    NCf$L93_Y_model.01.standard_name = "Y Lambert-93 model"
    NCf$L93_Y_model.02.long_name =
        "vertical coordinate in Lambert-93 in the model world"
    NCf$L93_Y_model.03.units = "m"
} else {
    warning("no var L93_Y_model")
}

## 3. LAMBERT-II _____________________________________________________
### 3.1. Lambert-II X ________________________________________________
#### 3.1.1. Real _____________________________________________________
if (any(LII_X %in% names(NC$var))) {
    NCf$LII_X.name = "LII_X"
    NCf$LII_X.dimension = "station"
    NCf$LII_X.precision = "double"
    NCf$LII_X.value = LII_X_data
    NCf$LII_X.01.standard_name = "X Lambert-II"
    NCf$LII_X.02.long_name = "horizontal coordinate in Lambert-II"
    NCf$LII_X.03.units = "m"
} else {
    warning("no var LII_X")
}
#### 3.1.2. Model _____________________________________________________
if (any(LII_X_model %in% names(NC$var))) {
    NCf$LII_X_model.name = "LII_X_model"
    NCf$LII_X_model.dimension = "station"
    NCf$LII_X_model.precision = "double"
    NCf$LII_X_model.value = LII_X_model_data
    NCf$LII_X_model.01.standard_name = "X Lambert-II model"
    NCf$LII_X_model.02.long_name =
        "horizontal coordinate in Lambert-II in the model world"
    NCf$LII_X_model.03.units = "m"
} else {
    warning("no var LII_X_model")
}

### 3.2. Lambert-II Y ________________________________________________
#### 3.2.1. Real _____________________________________________________
if (any(LII_Y %in% names(NC$var))) {
    NCf$LII_Y.name = "LII_Y"
    NCf$LII_Y.dimension = "station"
    NCf$LII_Y.precision = "double"
    NCf$LII_Y.value = LII_Y_data
    NCf$LII_Y.01.standard_name = "Y Lambert-II"
    NCf$LII_Y.02.long_name = "vertical coordinate in Lambert-II"
    NCf$LII_Y.03.units = "m"
} else {
    warning("no var LII_Y")
}
#### 3.2.2. Model _____________________________________________________
if (any(LII_Y_model %in% names(NC$var))) {
    NCf$LII_Y_model.name = "LII_Y_model"
    NCf$LII_Y_model.dimension = "station"
    NCf$LII_Y_model.precision = "double"
    NCf$LII_Y_model.value = LII_Y_model_data
    NCf$LII_Y_model.01.standard_name = "Y Lambert-II model"
    NCf$LII_Y_model.02.long_name =
        "vertical coordinate in Lambert-II in the model world"
    NCf$LII_Y_model.03.units = "m"
} else {
    warning("no var LII_Y_model")
}
