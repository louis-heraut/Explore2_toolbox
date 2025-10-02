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

if (!is_SAFRAN & !is_delta & !compute_mean) {
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
    variable_standard_name = paste0("Monthly ", metaEX_var$variable_en)
} else if (!is.null(season)) {
    NCf$title.02.TimeFrequency = paste0('seas-', season)
    variable_standard_name = paste0("Seasonal ", metaEX_var$variable_en, " ", season)
# } else if (var %in% Variable_hyr) {
    # NCf$title.02.TimeFrequency = 'hyr'
    # variable_standard_name = metaEX_var$variable_en
} else {
    NCf$title.02.TimeFrequency = 'yr'
    variable_standard_name = metaEX_var$variable_en
}

## 3. Couverture temporelle __________________________________________
# Couverture temporelle des données sous forme YYYYMMDD-YYYYMMDD
if (is_TRACC) {
    NCf$title.03.StartTime_EndTime = rwl
} else {
    if (is_delta | compute_mean) {
        NCf$title.03.StartTime_EndTime =
            paste0(lubridate::year(futur_period[1]),
                   "-",
                   lubridate::year(futur_period[2]))
    } else {
        NCf$title.03.StartTime_EndTime = paste0(format(min(Date), "%Y"),
                                                "-",
                                                format(max(Date), "%Y"))
    }
}

## 4. Opération temporelle ___________________________________________
if (is_delta | compute_mean) {
    NCf$title.04.TIMEoperation = "TIMEavg"
} else {
    NCf$title.04.TIMEoperation = "TIMEseries"
}

## 5. Type d’anomalie ________________________________________________
if (is_delta) {
    NCf$title.05.ANOMx = "ANOMrd-1976-2005"
}

## 6. Géométrie des données __________________________________________
NCf$title.06.GEOdata = "GEOstation"

## 7. Domain _________________________________________________________
#  Couverture spatiale des données
if (is_delta | compute_mean) {
    NCf$title.07.Domain = "FR-METRO"
} else {
    if (dataEX$HM[1] == "EROS") {
        NCf$title.07.Domain = "FR-Bretagne-Loire"
    } else if (dataEX$HM[1] == "J2000") {
        NCf$title.07.Domain = "FR-Rhone-Loire"
    } else if (dataEX$HM[1] == "MORDOR-TS") {
        NCf$title.07.Domain = "FR-Loire"
    } else {
        NCf$title.07.Domain = "FR-METRO"
    }
}

## 8. Dataset ________________________________________________________
if (is_TRACC) {
    NCf$title.08.dataset = "TRACC-2023-EXPLORE2-2024"
} else {
    NCf$title.08.dataset = "EXPLORE2-2024"
}

## 9. Bc-Inst-Method-Obs-Period ______________________________________
# Identifiant de la méthode de correction de biais statistique =
# Institut-Méthode-Réanalyse-Période
BC_short = c('ADAMONT', 'CDFt')
BC_name = c('MF-ADAMONT', 'LSCE-IPSL-CDFt')
if (is_SAFRAN) {
    NCf$title.09.Bc_Inst_Method = ""
} else {
    if (is_TRACC) {
        NCf$title.09.Bc_Inst_Method = "MF-ADAMONT"
    } else {
        if (only_ADAMONT) {
            NCf$title.09.Bc_Inst_Method = "MF-ADAMONT"
        } else if (is_delta | compute_mean) {
            NCf$title.09.Bc_Inst_Method = "ENSavg"
        } else {
            NCf$title.09.Bc_Inst_Method =
                BC_name[BC_short == dataEX$BC[1]]
        }
    }
}    

## 10. Experiment _____________________________________________________
# Identifiant de l’expérience historique ou future via le scénario
NCf$title.10.Experiment = dataEX$EXP[1]

## 11. GCM-Model _________________________________________________
# Identifiant du GCM forçeur
if (is_SAFRAN) {
    NCf$title.11.GCM_Model = ""
} else {
    if (is_delta | compute_mean) {
        NCf$title.11.GCM_Model = "ENSavg"
    } else {
        NCf$title.11.GCM_Model =
            meta_projection$gcm_short[projection_ok]
    }
}

## 12. RCM-Model _________________________________________________
# Identifiant du RCM
if (is_SAFRAN) {
    NCf$title.12.RCM_Model = ""
} else {
    if (is_delta | compute_mean) {
        NCf$title.12.RCM_Model = "ENSavg"
    } else {
        NCf$title.12.RCM_Model =
            meta_projection$rcm_short[projection_ok] 
    }
}

## 13. HYDRO-Inst-Model _______________________________________________
# Identifiant du HYDRO = Institut-Modèle
if (is_delta | compute_mean) {
    NCf$title.13.HYDRO_Inst_Model = "ENSavg"
} else {
    NCf$title.13.HYDRO_Inst_Model = dataEX$HM[1]
}
