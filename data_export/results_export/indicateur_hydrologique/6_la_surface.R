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


#  _                          __                
# | | __ _    ___ _  _  _ _  / _| __ _  __  ___ 
# | |/ _` |  (_-<| || || '_||  _|/ _` |/ _|/ -_)
# |_|\__,_|  /__/ \_,_||_|  |_|  \__,_|\__|\___| _____________________
# Aux variables de coordonnées s'ajoutent aussi les variables
# caractérisant la surface topologique des bassins versants associés à
# chaque station.
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


# 1. TOPOLOGICAL SURFACE OF STATION __________________________________
## 1.1. Real _________________________________________________________
if (any(topologicalSurface %in% names(NC$var))) {
    NCf$topologicalSurface.name = "topologicalSurface"
    NCf$topologicalSurface.dimension = "station"
    NCf$topologicalSurface.precision = "double"
    NCf$topologicalSurface.value = topologicalSurface_data
    NCf$topologicalSurface.01.long_name =
        "topological surface of the watershed"
    NCf$topologicalSurface.02.units = "km2"
}

## 1.2. Model ________________________________________________________
if (any(topologicalSurface_model %in% names(NC$var))) {
    NCf$topologicalSurface_model.name =
        "topologicalSurface_model"
    NCf$topologicalSurface_model.dimension = "station"
    NCf$topologicalSurface_model.precision = "double"
    NCf$topologicalSurface_model.value = topologicalSurface_model_data
    NCf$topologicalSurface_model.01.long_name =
        "topological surface of the watershed in the model world"
    NCf$topologicalSurface_model.02.units = "km2"
}
