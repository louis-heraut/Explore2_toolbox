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


#  ___   ___  ___    _    ___                               _   
# |   \ | _ \|_ _|  /_\  / __|    ___ __ __ _ __  ___  _ _ | |_ 
# | |) ||   / | |  / _ \ \__ \   / -_)\ \ /| '_ \/ _ \| '_||  _|
# |___/ |_|_\|___|/_/ \_\|___/   \___|/_\_\| .__/\___/|_|   \__| ______
# Export pour le portail DRIAS des données |_| hydro-climatiques 
library(NCf)
library(dplyr)
library(ncdf4)

computer = Sys.info()["nodename"]

if (grepl("LGA-LYP6123", computer)) {
    out_dir = "NetCDF"
    script_dirpath = "."
    data_dirpath = "/media/lheraut/Explore2/hydrological-projections/hydrological-projections_daily-time-series_by-chain_merged-netcdf"
    results_dirpath = "/media/lheraut/Explore2/hydrological-projections_indicateurs-TRACC/hydrological-projections_yearly-variables_by-chain_csv"

    metadata_dirpath = "/media/lheraut/Explore2/hydrological-metadata"
    Projection_file = "chaines_simulations_Explore2.csv"
    chain_to_remove_file = "code_Chain_outliers.csv"
    metaEX_ALL_file = "indicateurs_hydrologiques_agregees_Explore2.csv"
    meta_ALL_file = "stations_Explore2.csv"
}

verbose = TRUE
MPI = ""


## MPI _______________________________________________________________
post = function(x, ...) {
    if (verbose) {
        if (MPI != "") {
            print(paste0(formatC(as.character(rank),
                                 width=3, flag=" "),
                         "/", size-1, " > ", x), ...)
        } else {
            print(x, ...)
        }
    }
}

if (MPI != "") {
    library(Rmpi)
    rank = mpi.comm.rank(comm=0)
    size = mpi.comm.size(comm=0)

    if (size > 1) {
        if (rank == 0) {
            Rrank_sample = sample(0:(size-1))
            for (root in 1:(size-1)) {
                Rmpi::mpi.send(as.integer(Rrank_sample[root+1]),
                               type=1, dest=root,
                               tag=1, comm=0)
            }
            Rrank = Rrank_sample[1]
        } else {
            Rrank = Rmpi::mpi.recv(as.integer(0),
                                   type=1,
                                   source=0,
                                   tag=1, comm=0)
        }
    } else {
        Rrank = 0
    }
    post(paste0("Random rank attributed : ", Rrank))
    
} else {
    rank = 0
    size = 1
    Rrank = 0
}


## INTRO _____________________________________________________________
Variable = c(
    # "^Q05A$", "^Q10A$", "^QJXA$", "^tQJXA$", "^VCX3$", "^tVCX3$",
    # "^VCX10$", "^tVCX10$", "^dtFlood$",
    
    # "^Q50A$", "^QA$", "^QMA_", "^QSA_",
    
    # "^Q95A$", "^Q90A$", "^QMNA$",
    # "^VCN3$", "^VCN10$", "^VCN30$",
    # "^startLF$", "^centerLF$", "^dtLF$",
    # "^VCN3_summer$", "^VCN10_summer$", "^VCN30_summer$",
    # "^startLF_summer$", "^centerLF_summer$", "^dtLF_summer$"  # CHANGER LES NOMS DES VARIABLES 
    
    #"^endLF$", "^endLF_summer$",
    "^centerLF_summer$"
)
Variable_pattern = paste0("(", paste0(Variable, collapse=")|("), ")")
# Variable_hyr = c("QA")

Season_pattern = "(DJF)|(MAM)|(JJA)|(SON)|(MJJASON)|(NDJFMA)"
Month = c("jan", "feb", "mar", "apr", "may", "jun",
          "jul", "aug", "sep", "oct", "nov", "dec")
Month_pattern = paste0("(", paste0(Month, collapse=")|("), ")")

# meta_projection = ASHE::read_tibble(file.path(metadata_dirpath,
# meta_projection_file))
Projection = ASHE::read_tibble(file.path(metadata_dirpath,
                                         Projection_file))
chain_to_remove = ASHE::read_tibble(file.path(metadata_dirpath,
                                              chain_to_remove_file))
metaEX_ALL = ASHE::read_tibble(file.path(metadata_dirpath,
                                         metaEX_ALL_file))
meta_ALL = ASHE::read_tibble(file.path(metadata_dirpath,
                                       meta_ALL_file))
n_lim = 4 

data_Paths = list.files(data_dirpath,
                        pattern="[.]nc$",
                        full.names=TRUE,
                        recursive=TRUE)
data_Paths = data_Paths[!grepl("debit_narratifs_all", data_Paths)]

# Chain_dirpath = list.dirs(results_dirpath, recursive=FALSE)
Results_path = list.files(results_dirpath, recursive=TRUE,
                          full.names=TRUE)
Chain_dirpath = gsub(".csv", "", basename(Results_path))

clean_chain = function(chain) {
    paste(chain[-c(1:3)], collapse = "_")
}
Chain_dirpath = sapply(strsplit(Chain_dirpath, "_"), clean_chain)

get_var = function (var_path) {
    gsub("_yr", "", paste(var_path[c(1:2)], collapse = "_"))
}

GWL = c("GWL-15", "GWL-2.0", "GWL-30")
nGWL = length(GWL)


### NOT SAFRAN
# Chain_dirpath = Chain_dirpath[!grepl("^SAFRAN[_]",
# basename(Chain_dirpath))]
# Chain_dirpath = Chain_dirpath[grepl("^SAFRAN[_]",
# basename(Chain_dirpath))]
###

nChain_dirpath = length(Chain_dirpath)

if (MPI == "file") {            
    start = ceiling(seq(1, nChain_dirpath,
                        by=(nChain_dirpath/size)))
    if (any(diff(start) == 0)) {
        start = 1:nChain_dirpath
        end = start
    } else {
        end = c(start[-1]-1, nChain_dirpath)
    }
    if (rank == 0) {
        post(paste0(paste0("rank ", 0:(size-1), " get ",
                           end-start+1, " files"),
                    collapse="    "))
    }
    if (Rrank+1 > nChain_dirpath) {
        Chain_dirpath = NULL
    } else {
        Chain_dirpath = Chain_dirpath[start[Rrank+1]:end[Rrank+1]]
    }
}

# EC-EARTH_historical-rcp26_HadREM3-GA7_ADAMONT_EROS
# MORDOR-SD/SAFRAN_MORDOR-SD"
### /!\ ###
# OK =
# grepl("SAFRAN", Chain_dirpath) &
# grepl("rcp26", Chain_dirpath) &
# grepl("EARTH", Chain_dirpath) &
# grepl("HadREM3", Chain_dirpath) &
# grepl("MORDOR-SD", Chain_dirpath)
# Chain_dirpath = Chain_dirpath[OK] 
###########

nChain_dirpath = length(Chain_dirpath)


## Tool ______________________________________________________________
add_chain = function (dataEX, Projection_chain) {
    # if (!is_SAFRAN) {
        # dataEX = tidyr::unite(dataEX,
                              # "Chain",
                              # "GCM", "EXP",
                              # "RCM", "BC",
                              # "HM", sep="|",
                              # remove=FALSE)
    # } else {
        # dataEX = tidyr::unite(dataEX,
                              # "Chain",
                              # "EXP", "HM", sep="|",
                              # remove=FALSE)
    # }

    dataEX = dplyr::bind_cols(dataEX,
                              dplyr::select(Projection_chain,
                                            EXP, GCM, RCM,
                                            BC, HM,
                                            climateChain, Chain))
    return (dataEX) 
}



filter_code = function (dataEX) {
    exp = gsub(".*[-]", "", dataEX$EXP[1])
    if (is_SAFRAN) {
        Code_selection =
            dplyr::filter(meta_ALL,
                          get(paste0("n")) >= n_lim)$code
    } else {
        Code_selection =
            dplyr::filter(meta_ALL,
                          get(paste0("n_", exp)) >= n_lim)$code
    }

    dataEX = dplyr::filter(dataEX, code %in% Code_selection)

    dataEX$code_Chain = paste0(dataEX$code, "_",
                               dataEX$Chain)
    dataEX = dplyr::filter(dataEX,
                           !(code_Chain %in%
                             chain_to_remove$code_Chain))
    return (dataEX)
}




# filter_code = function (dataEX) {
#     if (is_SAFRAN) {
#         Code_selection =
#             dplyr::filter(meta_ALL,
#                           get(paste0("n")) >= n_lim)$code
#     } else {
#         Code_selection =
#             dplyr::filter(meta_ALL,
#                           get(paste0("n_rcp85")) >= n_lim)$code
#     }
    
#     dataEX = dplyr::filter(dataEX, Station %in% Code_selection) ###
    
#     #dataEX$code_Chain = paste0(dataEX$code, "_",
#     #dataEX$Chain)
#     dataEX = dplyr::filter(dataEX,
#                            !(code_Chain %in%
#                              chain_to_remove$code))
#     return (dataEX)
# }

transform_chain <- function(dataEX, var) { ################## pour avoir une mise en forme finale + proche de ce que tu avais en utilisant le code_Chain rajouté à add_Chain pour filtrer 
    dataEX <- dataEX %>%
        separate(code_Chain, into = c("Station_tmp", "GCM", "EXP", "RCM", "BC", "HM"), sep = "[_|]") %>%
        select(-Station_tmp) %>%
        rename(!!sym(var) := Value) %>%  # renommage dynamique ici de Value par l'objet var
        select(Annee, Temp, Station, !!sym(var), GCM, EXP, RCM, BC, HM)
    return(dataEX)
}


## PROCESS ___________________________________________________________
for (i in 1:nChain_dirpath) {
    if (nChain_dirpath == 0) {
        break
    }
    chain_dirpath =  Chain_dirpath[i]
    is_SAFRAN = grepl("SAFRAN", basename(chain_dirpath))

    post(paste0("* ", i, " -> ",
                round(i/nChain_dirpath*100, 1), "%"))
    post(chain_dirpath)

    Projection_chain = dplyr::filter(Projection,
                                     gsub("historical-", "",
                                          Projection$Chain) == chain_dirpath)
    
    regexp = gsub("historical[[][-][]]", "", Projection_chain$regexp)
    regexp_NC = Projection_chain$regexp_simulation_DRIAS
    
    data_paths = data_Paths[grepl(regexp,
                                  basename(data_Paths))]
    data_path = data_paths[1]
    NC = ncdf4::nc_open(data_path)


    for (j in 1:nGWL) {
        gwl = GWL[j]
        
        Var_path =
            Results_path[grepl(gwl, basename(Results_path)) &
                         grepl(regexp, basename(Results_path))]
        Var = sapply(strsplit(basename(Var_path), "_"), get_var)
        
        Ok = grepl(Variable_pattern, Var)
        Var_path = Var_path[Ok]
        Var = Var[Ok]
        
        
        ### /!\ ###
        # Var_path = Var_path[grepl("startLF_summer", Var_path)]
        ###########
        nVar_path = length(Var_path)
        
        is_month_done = FALSE
        for (k in 1:nVar_path) {

            var = Var[k]
            var_path = Var_path[k]

            post(paste0("** ", k, " -> ",
                        round(k/nVar_path*100, 1), "%"))
            
            if (is_month_done & grepl(Month_pattern, var)) {
                next
            }

            metaEX_var = metaEX_ALL[metaEX_ALL$variable_en == var,]

            if (!is_month_done & grepl(Month_pattern, var)) {
                var_no_pattern =
                    gsub("[_]", "",
                         gsub(Month_pattern, "",
                              metaEX_var$variable_en))
                var_no_pattern = gsub("QMA", "QA", var_no_pattern)
                metaEX_var$variable_en = var_no_pattern
                metaEX_var$name_en = gsub("each .*", "each month",
                                          metaEX_var$name_en)
                
                var_Month = paste0(gsub(Month_pattern, "", var),
                                   Month)
                dataEX = dplyr::tibble()
                for (var_month in var_Month) {
                    var_month_path = paste0(file.path(dirname(var_path),
                                                      var_month),
                                            ".fst")
                    dataEX_tmp = ASHE::read_tibble(var_month_path)
                    dataEX_tmp = add_chain(dataEX_tmp)
                    dataEX_tmp = filter_code(dataEX_tmp)
                    dataEX_tmp = debug_years(dataEX_tmp, var_month)
                    dataEX_tmp = dplyr::rename(dataEX_tmp,
                                               !!var_no_pattern:=
                                                   dplyr::all_of(var_month))
                    dataEX =
                        dplyr::bind_rows(dataEX, dataEX_tmp)
                }
                dataEX = dplyr::arrange(dataEX, code, date)
                is_month_done = TRUE
                timestep = "month"

            } else {
                dataEX = ASHE::read_tibble(var_path)
                dataEX = add_chain(dataEX, Projection_chain)
                dataEX = filter_code(dataEX)

                sampling_period = unlist(strsplit(metaEX_var$sampling_period_en, ", "))

                ####


                
                if (length(metaEX_var$sampling_period_en) != 2) {
                    SamplingPeriod =
                        dplyr::summarise(dplyr::group_by(dataEX, code),
                                         start=format(date[1], "%m-%d"),
                                         end=format(as.Date(paste0("1970-", start))-1,
                                                    "%m-%d"))
                } else {
                    SamplingPeriod =
                        dplyr::tibble(code=levels(factor(dataEX$code)),
                                      start=metaEX_var$sampling_period_en[1],
                                      end=metaEX_var$sampling_period_en[2])
                }
                dataEX$date = as.Date(paste0(lubridate::year(dataEX$date),
                                             "-01-01"))
                # dataEX = debug_years(dataEX, var)
                dataEX = dplyr::arrange(dataEX, code, date)
                
                timestep = "year"

                if (grepl("summer", var)) {
                    season = "MJJASON"
                    var_no_pattern = gsub("summer", "", var)
                } else if (grepl("winter", var)) {
                    season = "NDJFMA"
                    var_no_pattern = gsub("winter", "", var)
                } else if (grepl(Season_pattern, var)) {
                    season = stringr::str_extract(var, Season_pattern)
                    var_no_pattern = gsub(Season_pattern, "", var)
                    var_no_pattern = gsub("QSA", "QA", var_no_pattern)
                } else {
                    season = NULL
                    var_no_pattern = var
                }
                var_no_pattern = gsub("[_]$", "", var_no_pattern)
                metaEX_var$variable_en = var_no_pattern
                dataEX = dplyr::rename(dataEX, !!var_no_pattern:=var)
            }
            

            if (!("date" %in% names(dataEX))) {
                next
            }
            

            

            
            Date = dataEX_GWL$Annee ###
            Date = seq.Date(min(Date), max(Date), by=timestep) ###
            central_year <- format(Date[10], "%Y") ###
            # if (timestep == "year") {
            
            # Date = seq.Date(as.Date(paste0(lubridate::year(min(Date)),
            #                                "-01-01")),
            #                 as.Date(paste0(lubridate::year(max(Date)),
            #                                "-01-01")),
            #                 by=timestep)
            # Date_tmp = as.Date(levels(factor(dataEX$date)))
            # if (length(Date_tmp) != length(Date)) {
            #     dataEX$date = as.Date(paste0(lubridate::year(dataEX$date),
            #                                  "-01-01"))
            # }
            
            # tmp = dplyr::distinct(dplyr::select(dataEX, -date))
            # tmp = dplyr::reframe(dplyr::group_by(tmp, code, Chain),
            #                      date=Date)
            
            # if (nrow(tmp) != nrow(dataEX)) {
            #     dataEX = dplyr::select(dataEX, Chain, code, date,
            #                            dplyr::all_of(var_no_pattern))
            #     dataEX = dplyr::full_join(dataEX, tmp,
            #                               by=c("Chain", "code", "date"))
            #     dataEX = tidyr::separate(dataEX, "Chain",
            #                              c("GCM", "EXP",
            #                                "RCM", "BC",
            #                                "HM"), sep="[|]",
            #                              remove=FALSE)
            #     dataEX = dplyr::arrange(dataEX, Chain, code, date)
            # }
            
            # }
            # else if (timestep == "month") {
            # Date = seq.Date(min(Date), max(Date), by=timestep)
            # }
            
            Code = levels(factor(dataEX_GWL$Station)) ###
            
            dataEX_matrix = dplyr::select(dataEX_GWL, Station, Annee,
                                          dplyr::all_of(var_no_pattern)) ###
            dataEX_matrix =
                tidyr::pivot_wider(dataEX_matrix,
                                   names_from=Station,
                                   values_from=dplyr::all_of(var_no_pattern)) ###
            dataEX_matrix = dplyr::select(dataEX_matrix, -Annee) ###
            dataEX_matrix = t(as.matrix(dataEX_matrix))
            
            
            ###
            initialise_NCf()
            
            list_path = list.files(script_dirpath,
                                   pattern='*.R$',
                                   full.names=TRUE)
            
            list_path = list_path[!grepl("DRIAS_export", list_path)]
            for (path in list_path) {
                source(path, encoding='UTF-8')   
            }
            
            if (!(file.exists(out_dir))) {
                dir.create(out_dir)
            }
            
            NC_path = generate_NCf(out_dir=out_dir,
                                   return_path=TRUE,
                                   verbose=FALSE)
            
            ### verif ###
            NC_test = ncdf4::nc_open(NC_path)
            code_test = Code[runif(1, 1, length(Code))]
            Code_test = ncdf4::ncvar_get(NC_test, "code")
            Date_test = ncdf4::ncvar_get(NC_test, "time") +
                as.Date("1950-01-01")
            id_code = which(Code_test == code_test) 
            Value_test =
                ncdf4::ncvar_get(NC_test,
                                 metaEX_var$variable_en)[id_code,]
            Value_test[!is.finite(Value_test)] = NA
            if (grepl("QMA_apr", var_path)) {
                ok = lubridate::month(Date_test) == 4
                Date_test = Date_test[ok]
                Value_test = Value_test[ok]
            }
            Date_test = Date_test[!is.na(Value_test)]
            Value_test = Value_test[!is.na(Value_test)]
            
            date_min = "2031-01-01" # Ici j'ai pris cette date car le 1.5 de CNRM-CM5_ALADIN63_ADAMONT_CTRIP va de 2031 à 2050
            
            dataEX_test = dataEX_GWL # Je préfère reprendre ici le dataex_GWL et pas le fichier d'origine pour ne pas refaire le mise en forme 
            dataEX_test = dplyr::filter(dataEX_test, Annee >= date_min) ###
            dataEX_test = dplyr::filter(dataEX_test, ###
                                        Station==code_test)
            #dataEX_test$year = lubridate::year(dataEX_test$date)
            if (!grepl("QMA_apr", var_path)) {
                dataEX_test$date =
                    as.Date(paste0(dataEX_test$year, "-01-01"))
            }
            
            valEX = dataEX_test[[var]] ###
            dateEX = dataEX_test$Annee ###
            dateEX = dateEX[!is.na(valEX)] ###
            valEX = valEX[!is.na(valEX)] ###
            
            surface_test =
                ncdf4::ncvar_get(NC_test,
                                 "topologicalSurface_model")[id_code]
            L93_X_test = ncdf4::ncvar_get(NC_test, "L93_X")[id_code]
            
            meta_ALL_test = dplyr::filter(meta_ALL, code==code_test)
            hm_test = gsub("[-]", "_", dataEX_test$HM[1])
            surface_var = paste0("surface_", hm_test, "_km2")
            
            ok1 = all(dateEX == Date_test)
            ok2 = all.equal(valEX, Value_test, 0.001)
            if (!is_SAFRAN) {
                ok3 = all.equal(meta_ALL_test[[surface_var]],
                                surface_test,
                                0.1)
            } else {
                # there is some NA in surface in SAFRAN MORDOR-SD
                ok3 = TRUE
            }
            ok4 = all.equal(meta_ALL_test$XL93_m,
                            L93_X_test,
                            0.1)
            
            ncdf4::nc_close(NC_test)
            
            if (!is.logical(ok1)) {
                post(ok1)
                print(dateEX)
                print(Date_test)
                post(code_test)
                post(chain_dirpath)
                post(var_path)
                stop(paste0("1 ", NC_path))
            }
            if (!is.logical(ok2)) {
                post(ok2)
                print(valEX)
                print(Value_test)
                post(code_test)
                post(chain_dirpath)
                post(var_path)
                stop(paste0("2 ", NC_path))
            }
            if (!is.logical(ok3)) {
                post(ok3)
                post(code_test)
                post(chain_dirpath)
                post(var_path)
                stop(paste0("3 ", NC_path))
            }
            if (!is.logical(ok4)) {
                post(ok4)
                post(code_test)
                post(chain_dirpath)
                post(var_path)
                stop(paste0("4 ", NC_path))
            }
            
            is_ok = ok1 & ok2 & ok3 & ok4
            
            if (!is_ok) {
                post(ok1)
                post(ok2)
                post(ok3)
                post(ok4)
                post(code_test)
                post(chain_dirpath)
                post(var_path)
                stop(paste0("all ", NC_path))
            }
            ### end verif ###
        }
        ncdf4::nc_close(NC)
        *
        post("")
    }  
    
} 

if (MPI != "") {
    Sys.sleep(10)
    mpi.finalize()
}
