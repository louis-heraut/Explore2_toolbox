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
# | | ___    _ _   ___  _ __  
# | |/ -_)  | ' \ / _ \| '  \ 
# |_|\___|  |_||_|\___/|_|_|_|                      
#     _           __  _      _     _           
#  __| | _  _    / _|(_) __ | |_  (_) ___  _ _ 
# / _` || || |  |  _|| |/ _|| ' \ | |/ -_)| '_|
# \__,_| \_,_|  |_|  |_|\__||_||_||_|\___||_|   ______________________
# Les éléments composant le nom du fichier fournissent rapidement des
# informations sur la simulation et s’écrit comme ce qui suit :
#
#     Indicator_TimeFrequency_StartTime-EndTime_Domain_ModelXXX.nc

if (!is_SAFRAN) {
    projection_ok =
        grepl(dataEX$GCM[1], meta_projection$gcm) &
        grepl(dataEX$RCM[1], meta_projection$rcm)
    if (!any(projection_ok)) {
        stop(paste0(dataEX$GCM[1], " ", dataEX$RCM[1]))
    }
}

## 1. Indicateur _____________________________________________________
# Le nom de l’indicateur
NCf$title.01.Indicator = metaEX_var$variable_en

## 2. Pas de temps ___________________________________________________
# Le pas de temps du traitement
if (grepl(Month_pattern, var)) {
    NCf$title.02.TimeFrequency = 'mon'
    variable_standard_name = paste0("monthly ", metaEX_var$variable_en)
} else if (!is.null(season)) {
    NCf$title.02.TimeFrequency = paste0('seas-', season)
    variable_standard_name = paste0("seasonal ", metaEX_var$variable_en, " ", season)
# } else if (var %in% Variable_hyr) {
    # NCf$title.02.TimeFrequency = 'hyr'
    # variable_standard_name = metaEX_var$variable_en
} else {
    NCf$title.02.TimeFrequency = 'yr'
    variable_standard_name = metaEX_var$variable_en
}


## 3. Couverture temporelle __________________________________________
# Couverture temporelle des données sous forme YYYYMMDD-YYYYMMDD
NCf$title.03.StartTime_EndTime = GWL #"GWL-15, GWL-20, GWL-30"

## 4. Type ___________________________________________________________
NCf$title.04.type = "TIMEseries_GEOstation"

## 5. Domain _________________________________________________________
#  Couverture spatiale des données
if (dataEX$HM[1] == "EROS") {
    NCf$title.05.Domain = "FR-Bretagne-Loire"
} else if (dataEX$HM[1] == "J2000") {
    NCf$title.05.Domain = "FR-Rhone-Loire"
} else if (dataEX$HM[1] == "MORDOR-TS") {
    NCf$title.05.Domain = "FR-Loire"
} else {
    NCf$title.05.Domain = "FR-METRO"
}

## 6. Dataset ________________________________________________________
NCf$title.06.dataset = "TRACC-2023"

## 7. Bc-Inst-Method-Obs-Period ______________________________________
# Identifiant de la méthode de correction de biais statistique =
# Institut-Méthode-Réanalyse-Période
BC_short = c('ADAMONT', 'CDFt')
BC_name = c('MF-ADAMONT', 'LSCE-IPSL-CDFt')
if (is_SAFRAN) {
    NCf$title.07.Bc_Inst_Method = ""
} else {
    NCf$title.07.Bc_Inst_Method = BC_name[BC_short == dataEX$BC[1]]
}

## 8. Experiment _____________________________________________________
# Identifiant de l’expérience historique ou future via le scénario
NCf$title.08.Experiment = "rcp85" 

## 9. GCM-Model _________________________________________________
# Identifiant du GCM forçeur
if (is_SAFRAN) {
    NCf$title.09.GCM_Model = ""
} else {
    NCf$title.09.GCM_Model = meta_projection$gcm.short[projection_ok]
}

## 10. RCM-Model _________________________________________________
# Identifiant du RCM
if (is_SAFRAN) {
    NCf$title.10.RCM_Model = ""
} else {
    NCf$title.10.RCM_Model = meta_projection$rcm.short[projection_ok] 
}

## 11. HYDRO-Inst-Model _______________________________________________
# Identifiant du HYDRO = Institut-Modèle
NCf$title.11.HYDRO_Inst_Model = dataEX$HM[1]
