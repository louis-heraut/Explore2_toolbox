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


#  _                            _           _    _            
# | | __ _    _ __  _ _  ___   (_) ___  __ | |_ (_) ___  _ _  
# | |/ _` |  | '_ \| '_|/ _ \  | |/ -_)/ _||  _|| |/ _ \| ' \ 
# |_|\__,_|  | .__/|_|  \___/ _/ |\___|\__| \__||_|\___/|_||_| _______
# La projecti|_|on de la gril|__/le doit être référencée par une
# variable de données afin de déclarer explicitement le système de
# référence des coordonnées (CRS) utilisé pour les valeurs des
# coordonnées spatiales horizontales. Si les coordonnées spatiales
# horizontales sont des abscisses et des ordonnées dans une projection
# cartographique, la variable de projection de la grille déclare la
# projection cartographique CRS utilisée et fournit les informations
# nécessaires pour calculer les coordonnées horizontale et verticale
# à partir des abscisses et des ordonnées.


## 1. WGS 84 _________________________________________________________
NCf$WGS84.name = "WGS84"
NCf$WGS84.dimension = ""
NCf$WGS84.precision = "float"
NCf$WGS84.01.standard_name = "WGS 84"
NCf$WGS84.02.EPSG = "4326"
NCf$WGS84.03.references =
    "https://spatialreference.org/ref/epsg/wgs-84/html/"

## 2. LAMBERT-93 _____________________________________________________
NCf$L93.name = "L93"
NCf$L93.dimension = ""
NCf$L93.precision = "integer"
NCf$L93.01.standard_name = "Lambert-93"
NCf$L93.02.long_name = "RGF93 / Lambert-93"
NCf$L93.03.grid_mapping_name = "Lambert_Conformal_Conic_2SP"
NCf$L93.04.standard_parallel_1 = "49"
NCf$L93.05.standard_parallel_2 = "44"
NCf$L93.06.latitude_of_origin = "46.5"
NCf$L93.07.central_meridian = "3"
NCf$L93.08.false_easting = "700000"
NCf$L93.09.false_northing = "6600000"
NCf$L93.10.EPSG = "2154"
NCf$L93.11.references =
    "https://spatialreference.org/ref/epsg/2154/html/"

## 3. LAMBERT-II _____________________________________________________
NCf$LII.name = "LII"
NCf$LII.dimension = ""
NCf$LII.precision = "integer"
NCf$LII.01.standard_name = "Lambert-II"
NCf$LII.02.long_name = "NTF (Paris) / Lambert zone II"
NCf$LII.03.grid_mapping_name = "Lambert_Conformal_Conic_1SP"
NCf$LII.04.latitude_of_origin = "52"
NCf$LII.05.central_meridian = "0"
NCf$LII.06.scale_factor = "0.99987742"
NCf$LII.07.false_easting = "600000"
NCf$LII.08.false_northing = "2200000"
NCf$LII.09.epsg = "27572"
NCf$LII.10.references =
    "https://spatialreference.org/ref/epsg/ntf-paris-lambert-zone-ii/html/"
