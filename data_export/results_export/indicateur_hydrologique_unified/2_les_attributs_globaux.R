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


#  _                   _    _         _  _           _       
# | | ___  ___   __ _ | |_ | |_  _ _ (_)| |__  _  _ | |_  ___
# | |/ -_)(_-<  / _` ||  _||  _|| '_|| || '_ \| || ||  _|(_-<
# |_|\___|/__/  \__,_| \__| \__||_|  |_||_.__/ \_,_| \__|/__/
#        _       _                     
#  __ _ | | ___ | |__  __ _  _  _ __ __
# / _` || |/ _ \| '_ \/ _` || || |\ \ /
# \__, ||_|\___/|_.__/\__,_| \_,_|/_\_\ ______________________________
# |___/ Les attributs globaux sont souvent récupérés des fichiers
# sources. Ils renseignent sur la réalisation de la simulation, s’il
# s’agit d’une descente d’échelle dynamique ils contiennent les
# informations sur le forçage aux conditions aux limites (GCM) qui
# sont essentiels à la traçabilité. Les informations concernant la
# correction de biais (méthode, institut, date de mise en œuvre,
# référence) sont spécifiés dans les attributs avec le préfixe "bc_".


## 1. COUPLAGE GCM/RCM _______________________________________________
### 1.1. projet_id ___________________________________________________
# Identification du projet
NCf$global.01.project_id =
    ncdf4::ncatt_get(NC, 0, "project_id")$value

if (is_SAFRAN) {
    NCf$global.02.observation_id =
        ncdf4::ncatt_get(NC, 0, "observation_id")$value

    NCf$global.03.observation =
        ncdf4::ncatt_get(NC, 0, "observation")$value

    NCf$global.04.institution =
        ncdf4::ncatt_get(NC, 0, "institution")$value

    NCf$global.05.institution_id =
        ncdf4::ncatt_get(NC, 0, "institution_id")$value

    
} else {
    if (is_delta | compute_mean) {
        NCf$global.01.ensemble_stat = "ENSavg"
### 1.3. driving_* ___________________________________________________
        NCf$global.05.driving_experiment_name = dataEX$EXP[1]
        
### 1.6. experiment_id _______________________________________________
        # Un nom d’identification court de l’expérience (scénario ou historique)
        RCP = toupper(gsub(".*[-]", "", dataEX$EXP[1]))
        RCP = paste0(substr(RCP, 1, 4), ".", substr(RCP, 5, 5))
        NCf$global.11.experiment = paste0("historical+", RCP,
                                          " run with GCM forcing")
        NCf$global.12.experiment_id = dataEX$EXP[1]

        
    } else {
### 1.2. forcing _____________________________________________________
    # Une chaîne de caractères indiquant le modèle de forçage de cette
    # simulation
        NCf$global.02.forcing = 
            paste0(meta_projection$rcm_short[projection_ok],
                   " with ",
                   meta_projection$gcm_short[projection_ok],
                   " forcing data") 
### 1.3. driving_* ___________________________________________________
        # Des chaînes de caractères caractérisant le modèle forçeur dans la
        # descente d’échelle dynamique
        NCf$global.03.driving_model_id =
            ncdf4::ncatt_get(NC, 0, "driving_model_id")$value
        NCf$global.04.driving_model_ensemble_member =
            ncdf4::ncatt_get(NC, 0,
                             "driving_model_ensemble_member")$value
        NCf$global.05.driving_experiment_name = dataEX$EXP[1]

        NCf$global.06.driving_experiment =
            paste0(NCf$global.03.driving_model_id, ", ",
                   NCf$global.05.driving_experiment_name, ", ",
                   NCf$global.04.driving_model_ensemble_member)

### 1.4. institute_id ________________________________________________
        # Un nom court du centre de modélisation contribuant aux données avant
        # correction
        NCf$global.07.institution =
            ncdf4::ncatt_get(NC, 0, "institution")$value
        # "CNRM (Centre National de Recherches Meteorologiques, Toulouse 31057, France)"
        NCf$global.08.institute_id =
            ncdf4::ncatt_get(NC, 0, "institute_id")$value
        # "CNRM"

### 1.5. model_id ____________________________________________________
        # Un acronyme qui identifie le modèle utilisé pour générer les
        # données avant correction
        NCf$global.09.model_id =
            ncdf4::ncatt_get(NC, 0, "model_id")$value
        # "CNRM-ALADIN63"
        NCf$global.10.rcm_version_id =
            ncdf4::ncatt_get(NC, 0, "rcm_version_id")$value
        # "v2"

### 1.6. experiment_id _______________________________________________
        # Un nom d’identification court de l’expérience (scénario ou historique)
        RCP = toupper(gsub(".*[-]", "", dataEX$EXP[1]))
        RCP = paste0(substr(RCP, 1, 4), ".", substr(RCP, 5, 5))
        NCf$global.11.experiment = paste0("historical+", RCP,
                                          " run with GCM forcing")
        NCf$global.12.experiment_id = dataEX$EXP[1]
        
### 1.8. contact _____________________________________________________
        # Fournit l’adresse électronique de la personne responsable des
        # données
        NCf$global.14.contact =
            ncdf4::ncatt_get(NC, 0, "contact")$value
        
### 1.9. creation_date _______________________________________________
        # La date à laquelle la simulation a été réalisée
        NCf$global.15.creation_date =
            ncdf4::ncatt_get(NC, 0, "creation_date")$value

### 1.10. comment ____________________________________________________
        # Informations sur l’initialisation de la simulation ou fournit des
        # références littéraires
        NCf$global.16.comment = ncdf4::ncatt_get(NC, 0, "comment")$value
        # "CORDEX Europe EUR-11 CNRM-ALADIN 6.3 L91 CNRM-CERFACS-CNRM-CM5: EUC12v63-3.02. Reference : Daniel M., Lemonsu A., Déqué M., Somot S., Alias A., Masson V. (2018) Benefits of explicit urban parametrization in regional climate modelling to study climate and city interactions. Climate Dynamics, 1-20, doi:10.1007/s00382-018-4289-x"
        NCf$global.17.driving_experiment_comment =
            ncdf4::ncatt_get(NC, 0, "driving_experiment_comment")$value
        # "Known issue correction: this simulation (named v2) is not affected by the error previously identified in the lateral boundary conditions files of CNRM-CERFACS-CNRM-CM5"
    }


## 2. CORRECTION DE BIAIS ATMOSPHÉRIQUE ______________________________
### 2.1. product _____________________________________________________
    # Une chaîne de caractères indiquant la méthodologie pour créer cet
    # ensemble de données.
    NCf$global.18.product = "bias-correction"

    if ((is_delta | compute_mean) & !is_TRACC & !only_ADAMONT) {
        NCf$global.19.bc_ensemble_stat = "ENSavg"
        
    } else {
### 2.2. bc_institute_id _____________________________________________
         # Un nom d’identification court du centre qui a mis en œuvre la
         # correction de biais
         NCf$global.19.bc_institute_id =
             ncdf4::ncatt_get(NC, 0, "bc_institute_id")$value

### 2.3. bc_contact_id _______________________________________________
         # Fournit l’adresse électronique de la personne responsable des données
         NCf$global.20.bc_contact =
             ncdf4::ncatt_get(NC, 0, "bc_contact")$value

### 2.4. bc_creation_date ____________________________________________
         # La date à laquelle la correction de biais a été faite
         NCf$global.21.bc_creation_date =
             ncdf4::ncatt_get(NC, 0, "bc_creation_date")$value

### 2.5. bc_method_id ________________________________________________
         # Une chaîne de caractères indiquant la méthode de correction de biais
         NCf$global.22.bc_method =
             ncdf4::ncatt_get(NC, 0, "bc_method")$value
         NCf$global.23.bc_method_id =
             ncdf4::ncatt_get(NC, 0, "bc_method_id")$value

### 2.6. bc_observation_id ___________________________________________
         # Une chaîne de caractères de la base d’observation utilisée pour
         # corriger les données
         NCf$global.24.bc_observation =
             ncdf4::ncatt_get(NC, 0, "bc_observation")$value
         NCf$global.25.bc_observation_id =
             ncdf4::ncatt_get(NC, 0, "bc_observation_id")$value

### 2.7. bc_domain ___________________________________________________
         # Une chaîne de caractères indiquant le domaine d’application de la
         # méthode de correction
        NCf$global.26.bc_domain =
            ncdf4::ncatt_get(NC, 0, "bc_domain")$value

### 2.8. bc_period ___________________________________________________
         # Période sur laquelle a été appliqué la phase d’apprentissage de la
         # méthode de correction
         NCf$global.27.bc_period_ref =
             ncdf4::ncatt_get(NC, 0, "bc_period_ref")$value
         NCf$global.28.bc_period_rcm =
             ncdf4::ncatt_get(NC, 0, "bc_period_rcm")$value

### 2.9. bc_info _____________________________________________________
         # Une compilation des attributs : bc_institute_id "-" bc_method_id
         # "-" bc_observation_id en accord avec Bc-Inst-Method
        NCf$global.29.bc_info =
            ncdf4::ncatt_get(NC, 0, "bc_info")$value

### 2.10. bc_comment _________________________________________________
         # Complément d’information sur la méthode de correction ou fournit
         # des références littéraires
         NCf$global.30.bc_comment =
             ncdf4::ncatt_get(NC, 0, "bc_comment")$value
         NCf$global.31.Conventions =
             ncdf4::ncatt_get(NC, 0, "bc_Conventions")$value
    }
}


## 3. MODÉLISATION HYDROLOGIQUE ______________________________________
### 3.1. product _____________________________________________________
# Une chaîne de caractères indiquant la méthodologie pour créer cet
# ensemble de données
NCf$global.31.product = "hydro-climatique"

### 3.2. hy_projet_id ________________________________________________
# Identification du projet
NCf$global.32.hy_projet_id = "EXPLORE2"

if (is_delta | compute_mean) {
    NCf$global.33.hy_ensemble_stat = "ENSavg"
} else {
### 3.3. hy_institute_id _____________________________________________
    # Un nom d’identification court du centre de modélisation contribuant
    # aux données
    NCf$global.33.hy_institute_id =
        ncdf4::ncatt_get(NC, 0, "hy_institute_id")$value

### 3.4. hy_model_id _________________________________________________
    # Un acronyme qui identifie le modèle hydrologique
    NCf$global.34.hy_model_id =
        ncdf4::ncatt_get(NC, 0, "hy_model_id")$value

### 3.5. hy_version_id _______________________________________________
    NCf$global.35.hy_version_id =
        ncdf4::ncatt_get(NC, 0, "hy_version_id")$value

### 3.6. hy_creation_date ____________________________________________
    # La date à laquelle la simulation a été réalisée
    NCf$global.36.hy_creation_date =
        ncdf4::ncatt_get(NC, 0, "hy_creation_date")$value

### 3.7. hy_contact __________________________________________________
    # Fournit le nom ou l’adresse électronique de la personne responsable
    # des données
    NCf$global.37.hy_contact =
        ncdf4::ncatt_get(NC, 0, "hy_contact")$value
}


## 4. INDICATEUR HYDROLOGIQUE ________________________________________
### 4.1. indicator_institute _________________________________________
# Un nom d’identification court du centre de modélisation contribuant
# aux données
NCf$global.38.indicator_institute = "INRAE"

### 4.2. indicator_creation_date _____________________________________
# La date à laquelle on a calculé l’indicateur
NCf$global.39.indicator_creation_date = as.character(Sys.Date())

### 4.3. indicator_time _________________________________________
# L’intervalle de temps d’échantillonnage de la série de données
NCf$global.40.indicator_time_period = NCf$title.02.TimeFrequency

if (NCf$title.02.TimeFrequency == 'mon') {
    NCf$global.41.indicator_time_selection = "each month"
    
} else if (!is.null(season)) {
    NCf$global.41.indicator_time_selection = season
}

NCf$global.42.indicator_time_operation = "TIMEseries"
if (is_TRACC) {
    NCf$global.43.indicator_time_span = "20 years"
    if (!is_delta & !compute_mean) {
        NCf$global.44.indicator_time_central_year = central_year
        NCf$global.45.indicator_time_range = range_year
    }
} else {
    if (is_delta | compute_mean) {
        NCf$global.43.indicator_time_span = "30 years"
        NCf$global.45.indicator_time_range = range_year
    }
}

### 4.4. format_NetCDF _______________________________________________
# Une chaîne de caractères qui indique la référence normative appliquée
NCf$global.46.version_spec_format = "spécifications - version du 2025-04-08"
