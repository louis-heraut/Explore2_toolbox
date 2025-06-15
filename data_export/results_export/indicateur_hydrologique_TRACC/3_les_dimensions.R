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


#  _                _  _                        _                
# | | ___  ___   __| |(_) _ __   ___  _ _   ___(_) ___  _ _   ___
# | |/ -_)(_-<  / _` || || '  \ / -_)| ' \ (_-<| |/ _ \| ' \ (_-<
# |_|\___|/__/  \__,_||_||_|_|_|\___||_||_|/__/|_|\___/|_||_|/__/ ____
# Les séries temporelles de chaque station ayant le même nombre
# d’instance et des valeurs temporelles identiques pour toutes les
# instances, la représentation en tableau bidimensionnel est idéale.
# Celle-ci comporte deux dimensions : celle du temps ‘time’ et celle
# des éléments ici appelé ‘station’. De préférence ces dimensions
# apparaissent dans l’ordre suivant ‘time’, puis ‘station’.
#
# Naturellement les valeurs des dimensions sont croissantes et n’ont
# pas de valeur manquante.


## 1. LE TEMPS _______________________________________________________
# L’axe temporel est défini à partir d’une variable
# unidimensionnelle : time(time), la période couverte coïncide à celle
# annoncée par les métadonnées. Le nombre de valeur vérifie
# l’information sur la fréquence temporelle des données. Attention
# toute fois au type de calendrier et aux variations de début et de
# fin de période, des problèmes récurrents qui sont issus des modèles
# eux-mêmes. D’où l’importance d’une bonne documentation des attributs.
#
# L’axe temporel doit être défini comme « UNLIMITED », c’est-à-dire de
# dimension 1 et sans restriction. Cela permet de pouvoir concaténer
# des fichiers NetCDF si besoin. Pour cela, il est possible de les
# générer sans dimension fixée (record) et de les « degenerate ».
# Une commande ncks existe pour cela :
#
# ncks -O --mk_rec_dmn time in.nc out.nc # Change "time" to record
#                                        # dimension
#
# Le temps doit toujours inclure explicitement l’attribut "units" ; il
# n'y a pas de valeur par défaut. L’unité de temps attendue est :
# "days since YYYY-MM-DD hh:mm:ss" ; où YYYY définit l’année, MM le
# mois, DD le jour, hh l'heure, mm les minutes et ss les secondes.
# L’attribut "units" prend une valeur selon de codage suivant : "days
# since 1950-01-01 00:00:00" qui indique les jours depuis le 1er
# janvier 1950.
# La chaîne date/heure de référence (qui apparaît après
# l’identifiant since) est obligatoire. Elle peut inclure la date
# seule, ou la date et l’heure, ou la date, l’heure et le fuseau
# horaire. Si le fuseau horaire est omis, la valeur par défaut est
# UTC, et si l’heure et le fuseau horaire sont omis, la valeur par
# défaut est 00:00:00 UTC.
#
# Le choix du calendrier définit l’ensemble des dates (combinaisons
# année-mois-jour) qui sont autorisées. Il spécifie donc le nombre de
# jours entre deux dates quelconques. Le calendrier de temps attendu
# est : "standard" ; c’est le calendrier par défaut le calendrier
# grégorien. Dans ce calendrier, les dates/heures sont dans le
# calendrier grégorien, dans lequel une année est bissextile si (i)
# elle est divisible par 4 mais pas par 100 ou (ii) elle est
# divisible par 400.
#
#
# Paramètres :
#     standard_name : Un nom d’identification court de la dimension
#
#         long_name : Un nom d’identification long de la dimension
#
#             units : Spécifie l’unité de la dimension
#
#          calendar : Indique le type de calendrier utilisé
#
#              axis : Axe associé à la variable

from = as.Date("2201-01-01")
to = as.Date("2220-12-31")
origin = as.Date("1950-01-01")
units = paste0("days since ", origin)
time = seq.Date(from=from, to=to, by=timestep)
time = as.integer(time - origin)


NCf$time.name = "time"
NCf$time.value = time
NCf$time.01.standard_name = "time"
NCf$time.02.long_name = "time"
NCf$time.03.units = units
NCf$time.04.calendar = "standard"
NCf$time.05.axis = "T"


## 2. LES STATIONS ___________________________________________________
# Chaque élément ‘station’ est identifié à partir d’au moins 2
# variables qui sont des chaînes de caractères de longueur fixe et
# définie :
# station_code(station, code_strlen) : un code unique issu d’une
#                                      banque de données (ex : banque
#                                      HYDRO) qui le rattache à un
#                                      site hydrométrique bien
#                                      identifiés.
#
# station_name(station, name_strlen) : un libellé de la station
#                                      décrivant le cours d’eau, la
#                                      ville la plus proche et un
#                                      complément sur le site en
#                                      question (ex : La Vienne à
#                                      Limoges - Pont-Neuf ).
#
# Il peut être nécessaire d’ajouter des informations complémentaires,
# comme une classification des stations, on pourra alors s’appuyer sur
# la variable station_info(station).
#
#
# Paramètres :
#         long_name : Un nom d’identification long de la variable

match_code = match(Code, meta_ALL$code)

### 2.1. La dimension station ________________________________________
NCf$station.name = "station"
NCf$station.value = 1:length(Code)

### 2.2. Station name ________________________________________________
NCf$name.name = "name"
NCf$name.dimension = "name_strlen, station"
NCf$name.precision = "char"
NCf$name.value = meta_ALL$name[match_code]
NCf$name.01.long_name = "name of stations"
NCf$name_strlen.name = "name_strlen"
NCf$name_strlen.value = 1:max(nchar(NCf$name.value))
NCf$name_strlen.is_nchar_dimension = TRUE

### 2.3. Station code ________________________________________________
NCf$code.name = "code"
NCf$code.dimension = "code_strlen, station"
NCf$code.precision = "char"
NCf$code.value = Code
NCf$code.01.long_name = "code of stations"
NCf$code_strlen.name = "code_strlen"
NCf$code_strlen.value = 1:max(nchar(NCf$code.value))
NCf$code_strlen.is_nchar_dimension = TRUE

### 2.4. Station code type ___________________________________________
# "SANDRE" / "BSS" / "MESO"
NCf$code_type.name = "code_type"
NCf$code_type.dimension = "code_type_strlen, station"
NCf$code_type.precision = "char"
NCf$code_type.value = rep("SANDRE", length(Code))
NCf$code_type.01.long_name = "type of code for stations"
NCf$code_type_strlen.name = "code_type_strlen"
NCf$code_type_strlen.value = 1:max(nchar(NCf$code_type.value))
NCf$code_type_strlen.is_nchar_dimension = TRUE

### 2.5. Station network origin ________________________________________
# "ONDE" / "RCS" / "HYDRO" / "Explore2" / "Point Nodal" / "ADES"
NCf$network_origin.name = "network_origin"
NCf$network_origin.dimension = "network_origin_strlen, station"
NCf$network_origin.precision = "char"
NCf$network_origin.value = meta_ALL$source[match_code]
NCf$network_origin.01.long_name = "origin of network for stations"
NCf$network_origin_strlen.name = "network_origin_strlen"
NCf$network_origin_strlen.value = 1:max(nchar(NCf$network_origin.value))
NCf$network_origin_strlen.is_nchar_dimension = TRUE
