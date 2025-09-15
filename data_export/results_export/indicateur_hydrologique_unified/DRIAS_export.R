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

####
# VERIF CHAIN TO REMOVE SAFRAN
####
is_TRACC =
    TRUE
    # FALSE

is_delta =
    TRUE
    # FALSE

only_rcp85_and_ADAMONT =
    # TRUE
    FALSE

###
# VERIF CHAIN TO REMOVE SAFRAN
###
without_SAFRAN =
    TRUE
    # FALSE

Variable = c(
    # "Q05A_yr", "Q10A_yr", "Q50A_yr", "Q90A_yr", "Q95A_yr",
    # "QJXA_yr", "VCX10_yr", "VCX3_yr",
    # "tQJXA_yr", "tVCX10_yr", "tVCX3_yr",
    # "dtFlood_yr",
    "QA_yr"
    # "QS_",
    # "QM_",
    # "QMNA",
    # "startLF_summer", "centerLF_summer", "endLF_summer", 
    # "startLF", "centerLF", "endLF",
    # "dtLF_summer", "dtLF",
    # "VCN10", "VCN3", "VCN30",
    # "VCN10_summer", "VCN3_summer", "VCN30_summer"
)

historical = c("1976-01-01", "2005-08-31")
Futurs = list(
    H1=c("2021-01-01", "2050-12-31"),
    H2=c("2041-01-01", "2070-12-31"),
    H3=c("2070-01-01", "2099-12-31")
)


MPI = "file"
verbose = TRUE


computer = Sys.info()["nodename"]

if (grepl("LGA-LYP6123", computer)) {
    out_dir = "NetCDF"
    script_dirpath = "."
    data_dirpath = "/media/lheraut/Explore2/hydrological-projections/hydrological-projections_daily-time-series_by-chain_merged-netcdf"
    
    if (is_TRACC) {
        if (is_delta) {
            results_dirpath = "/media/lheraut/Explore2/hydrological-projections_indicators-TRACC/hydrological-projections_indicators-TRACC_changes-by-warming-level-ref-1976-2005_by-chain_parquet"

        } else {
            results_dirpath = "/media/lheraut/Explore2/hydrological-projections_indicators-TRACC//hydrological-projections_TRACC-indicators_yearly-variables_by-chain_fst"
        }
    } else {
        if (is_delta) {
            results_dirpath = "/media/lheraut/Explore2/hydrological-projections_indicators/hydrological-projections_indicators_changes-by-horizon-ref-1976-2005_by-chain_parquet"

        } else {
            results_dirpath = "/media/lheraut/Explore2/hydrological-projections_indicators/hydrological-projections_indicators_yearly-variables_by-chain_parquet"
        }
    }
    
    meta_projection_file = "tableau_metadata_EXPLORE2.csv"
    
    metadata_dirpath = "/media/lheraut/Explore2/metadata"
    Projection_file = "chaines_simulations_Explore2.csv"
    chain_to_remove_file = "code_Chain_outliers_Explore2.csv"
    metaEX_ALL_file = "indicateurs_hydrologiques_agregees_Explore2.csv"
    meta_ALL_file = "stations_Explore2.csv"
}

if (is_TRACC) {
    out_dir = paste0(out_dir, "_TRACC")
}
if (is_delta) {
    out_dir = paste0(out_dir, "_delta")
}


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
Variable_pattern = gsub("[_]", "[_]", gsub("[-]", "[-]", Variable))
if (is_delta) {
    Variable_pattern = paste0("(",
                              paste0(paste0("delta[-]",
                                            Variable_pattern),
                                     collapse=")|("), ")")
} else {
    Variable_pattern = paste0("(",
                              paste0(Variable_pattern,
                                     collapse=")|("), ")")
}

Season_pattern = "(DJF)|(MAM)|(JJA)|(SON)|(MJJASON)|(NDJFMA)"
Month = c("jan", "feb", "mar", "apr", "may", "jun",
          "jul", "aug", "sep", "oct", "nov", "dec")
Month_pattern = paste0("(", paste0(Month, collapse=")|("), ")")
date_min = "1975-01-01"


meta_projection = ASHE::read_tibble(file.path(script_dirpath,
                                              meta_projection_file))
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

Results_path = list.files(results_dirpath, recursive=TRUE,
                          full.names=TRUE)
Chain_dirpath = gsub("([.]fst)|([.]parquet)", "", basename(Results_path))

clean_chain = function(chain) {
    if (is_TRACC) {
        if (is_delta) {    
            paste(chain[-c(1:3, 9:10)], collapse = "_") #########
        } else {
            paste(chain[-c(1:3)], collapse = "_")
        }
    } else {
        if (is_delta) {    
            paste(chain[-c(1:2, 8:9)], collapse = "_")            
        } else {
            paste(chain[-c(1:2)], collapse = "_")
        }
    }
}
Chain_dirpath = sapply(strsplit(Chain_dirpath, "_"), clean_chain)
Chain_dirpath = unique(Chain_dirpath)

if (is_delta) {    
    # ClimateChain_dirpath_ALL = gsub("[_]$", "",
    # stringr::str_extract(Chain_dirpath, ".*[_]"))
    ClimateChain_dirpath_ALL = gsub("[_].*", "", Chain_dirpath)
    ClimateChain_dirpath = unique(ClimateChain_dirpath_ALL)
    Chain_dirpath = ClimateChain_dirpath
    
}

get_var = function (var_path) {
    gsub("", "", paste(var_path[c(1:2)], collapse = "_"))
}

if (is_TRACC) {
    GWL = c(
        "GWL-15",
        "GWL-20",
        "GWL-30"
    )
    RWL = c(
        "RWL-20",
        "RWL-27",
        "RWL-40"
    )
    nWL = length(RWL)
} else {
    nWL = length(Futurs)
}

###
# Chain_dirpath =
    # "historical-rcp85"
    # "rcp85_MPI-ESM-LR_REMO_ADAMONT"
    # "historical-rcp85_CNRM-CM5_ALADIN63_ADAMONT_CTRIP"
###


if (only_rcp85_and_ADAMONT) {
    Chain_dirpath = Chain_dirpath[grepl("rcp85", Chain_dirpath) &
                                  grepl("ADAMONT", Chain_dirpath)]
}
if (without_SAFRAN) {
    Chain_dirpath = Chain_dirpath[!grepl("^SAFRAN", Chain_dirpath)]
}    

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
###
# Chain_dirpath = Chain_dirpath[c(3:length(Chain_dirpath))]
###
nChain_dirpath = length(Chain_dirpath)


## Tool ______________________________________________________________
add_chain = function (dataEX) {
    if (is_SAFRAN) {
        dataEX = tidyr::unite(dataEX,
                              "Chain",
                              "EXP", "HM", sep="_",
                              remove=FALSE)
    } else {
        dataEX = tidyr::unite(dataEX,
                              "Chain",
                              "EXP", "GCM",
                              "RCM", "BC",
                              "HM", sep="_",
                              remove=FALSE)
    }
    return (dataEX) 
}

# debug_years_old = function (dataEX, var) {
#     Date = seq.Date(min(dataEX$date),
#                     max(dataEX$date),
#                     by="years")
#     tmp = dplyr::distinct(dplyr::select(dataEX, -date))
#     tmp = dplyr::reframe(dplyr::group_by(tmp, code, Chain),
#                          date=Date)

#     if (nrow(tmp) != nrow(dataEX)) {
#         dataEX = dplyr::select(dataEX, Chain, code, date,
#                                dplyr::all_of(var))
#         dataEX = dplyr::full_join(dataEX, tmp,
#                                   by=c("Chain", "code", "date"))
#         if (!is_SAFRAN) {
#             dataEX = tidyr::separate(dataEX, "Chain",
#                                      c("GCM", "EXP",
#                                        "RCM", "BC",
#                                        "HM"), sep="[_]",
#                                      remove=FALSE)
#         } else {
#             dataEX = tidyr::separate(dataEX, "Chain",
#                                      c("EXP", "HM"),
#                                      sep="[_]",
#                                      remove=FALSE)
#         }
#         dataEX = dplyr::arrange(dataEX, Chain, code, date)
#     }
#     dataEX = dplyr::filter(dataEX, date_min <= date)
#     return (dataEX)
# }

debug_years = function (dataEX) {
    Date = seq.Date(min(dataEX$date),
                    max(dataEX$date),
                    by="years")
    dataEX_tmp = tidyr::complete(dataEX, code, date=Date)
    dataEX_tmp = tidyr::fill(dplyr::group_by(dataEX_tmp,
                                             code),
                             dplyr::where(is.character),
                             .direction="downup")
    dataEX_tmp = dplyr::ungroup(dataEX_tmp)
    dataEX_tmp = dplyr::filter(dataEX_tmp, date_min <= date)
    return (dataEX_tmp)
}
# dataEX$date =
#     as.Date(paste0(lubridate::year(dataEX$date),
#                    "-01-01"))
# dataEX_save = dataEX
# dataEX_1 = debug_years(dataEX, var)
# dataEX_2 = debug_years_new(dataEX, var)


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
    if (!any(grepl("historical", dataEX$code_Chain))) {
        chain_to_remove_code_Chain = gsub("historical-", "",
                                          chain_to_remove$code_Chain)
    } else {
        chain_to_remove_code_Chain = chain_to_remove$code_Chain   
    }
    dataEX = dplyr::filter(dataEX,
                           !(code_Chain %in%
                             chain_to_remove_code_Chain))
    dataEX = dplyr::select(dataEX, -code_Chain)
    return (dataEX)
}

stop()


## PROCESS ___________________________________________________________
for (i in 1:nChain_dirpath) {
    if (nChain_dirpath == 0) {
        break
    }
    chain_dirpath = Chain_dirpath[i]    
    is_SAFRAN = grepl("SAFRAN", basename(chain_dirpath))
    
    post(paste0("* ", i, " -> ",
                round(i/nChain_dirpath*100, 1), "% : ",
                chain_dirpath))

    # if (is_delta) {
    #     Projection_chain =
    #         dplyr::filter(Projection,
    #                       grepl(chain_dirpath,
    #                             gsub("historical-", "",
    #                                  Projection$Chain)))
    # } else {
    #     Projection_chain =
    #         dplyr::filter(Projection,
    #                       gsub("historical-", "",
    #                            Projection$Chain) == chain_dirpath)
    # }

    Projection_chain =
        dplyr::filter(Projection,
                      grepl(chain_dirpath,
                            Projection$Chain))
    
    regexp = gsub("historical[[][-][]]", "", Projection_chain$regexp)
    regexp_NC = Projection_chain$regexp_simulation_DRIAS
    
    regexp = paste0("(", paste0(regexp, collapse=")|("), ")")
    regexp_NC = paste0("(", paste0(regexp_NC, collapse=")|("), ")")
    
    data_paths = data_Paths[grepl(regexp_NC,
                                  basename(data_Paths))]
    data_path = data_paths[1]
    NC = ncdf4::nc_open(data_path)


    for (j in 1:nWL) {
        if (is_TRACC) {
            rwl = RWL[j]
            rwl = RWL[j]
            post(paste0("** ", j, " -> ",
                        round(j/nWL*100, 1), "% : ", rwl))
        } else {
            if (is_delta) {
                futur = names(Futurs)[j]
                futur_period = Futurs[[j]]
                post(paste0("** ", j, " -> ",
                            round(j/nWL*100, 1), "% : ", futur))
            }
        }
        
        # Var_path =
        #     Results_path[grepl(rwl, basename(Results_path)) &
        #                  grepl(regexp, basename(Results_path))]
        # Var = sapply(strsplit(basename(Var_path), "_"), get_var)
        
        # Ok = grepl(Variable_pattern, Var)
        # Var_path = Var_path[Ok]
        # Var = Var[Ok]
        
        # nVar_path = length(Var_path)

        if (is_TRACC) {
            Ok_res = grepl(rwl, basename(Results_path)) &
                grepl(regexp, basename(Results_path))
        } else {
            if (is_delta) {
                Ok_res = grepl(futur, basename(Results_path)) &
                    grepl(regexp, basename(Results_path))
            } else {
                Ok_res = grepl(regexp, basename(Results_path))
            }
        }
        Var_path = Results_path[Ok_res]
        Var_ALL = sapply(strsplit(basename(Var_path), "_"), get_var)
        
        Ok_var = grepl(Variable_pattern, Var_ALL)
        Var_path = Var_path[Ok_var]
        Var_ALL = Var_ALL[Ok_var]

        Var = unique(Var_ALL)  
        
        Var_path_list =
            lapply(Var, function (x) Var_path[Var_ALL == x])
        names(Var_path_list) = Var

        nVar_path_list = length(Var_path_list)
        is_month_done = FALSE


        for (k in 1:nVar_path_list) {
            var = names(Var_path_list)[k]
            var_h = paste0(var, "_", futur)
            var_path = Var_path_list[[k]]

            post(paste0("*** ", k, " -> ",
                        round(k/nVar_path_list*100, 1), "% : ",
                        var))
            
            if (is_month_done & grepl(Month_pattern, var)) {
                next
            }

            if (is_TRACC) {
                Ok_meta = grepl(var, metaEX_ALL$variable_en)
            } else {
                if (is_delta) {
                    Ok_meta = grepl(futur, metaEX_ALL$variable_en) &
                        grepl(var, metaEX_ALL$variable_en)
                } else {
                    Ok_meta = grepl(var, metaEX_ALL$variable_en)
                }
            }
            metaEX_var = metaEX_ALL[Ok_meta,]
            metaEX_var$variable_en = var
            
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
                    # var_month_file = unlist(strsplit(basename(var_path), "_"))
                    # var_month_file =
                    #     paste0(var_month_file[3:length(var_month_file)],
                    #            collapse="_")
                    # var_month_file = paste0(var_month, "_", var_month_file)
                    
                    # var_month_path = file.path(dirname(var_path),
                    #                            var_month_file)
                    var_month_file = strsplit(basename(var_path), "_")
                    var_month_file = sapply(var_month_file,
                                            function (x) paste0(x[3:length(x)],
                                                                collapse="_"))
                    var_month_file = paste0(var_month, "_", var_month_file)
                    
                    var_month_path = file.path(dirname(var_path),
                                               var_month_file)
                    
                    if (is_delta) {
                        dataEX_tmp_list = lapply(var_month_path, ASHE::read_tibble)
                        dataEX_tmp = purrr::reduce(dataEX_tmp_list, dplyr::bind_rows)
                        dataEX_tmp = add_chain(dataEX_tmp)
                        dataEX_tmp = filter_code(dataEX_tmp)
                        if (is_TRACC) {
                            groups = c("GWL", "EXP", "GCM", "RCM",
                                       "BC", "date", "code")
                        } else {
                            groups = c("EXP", "GCM", "RCM",
                                       "BC", "date", "code")
                        }
                        dataEX_tmp =
                            dplyr::summarise(
                                       dplyr::group_by(dataEX_tmp,
                                                       dplyr::across(all_of(groups))),
                                       !!var_month:=mean(get(var_month),
                                                         na.rm=TRUE),
                                       .groups="drop")
                    } else {
                        dataEX_tmp = ASHE::read_tibble(var_month_path)
                        dataEX_tmp = add_chain(dataEX_tmp)
                        dataEX_tmp = filter_code(dataEX_tmp)
                    }
                        
                    if (!is_TRACC) { 
                        dataEX_tmp = debug_years(dataEX_tmp)
                    }
                    dataEX_tmp =
                        dplyr::rename(dataEX_tmp,
                                      !!var_no_pattern:=
                                          dplyr::all_of(var_month))
                    dataEX =
                        dplyr::bind_rows(dataEX, dataEX_tmp)
                }
                dataEX = dplyr::arrange(dataEX, code, date)
                dataEX_save = dataEX
                is_month_done = TRUE
                timestep = "month"

            } else {
                if (is_delta) {
                    dataEX_list = lapply(var_path, ASHE::read_tibble)
                    dataEX = purrr::reduce(dataEX_list, dplyr::bind_rows)
                    dataEX = add_chain(dataEX)
                    dataEX = filter_code(dataEX)
                    dataEX = dplyr::rename(dataEX, !!var:=var_h)
                    dataEX_save = dataEX
                    if (is_TRACC) {
                        dataEX = dataEX ################
                    } else {
                        dataEX = 
                            dplyr::summarise(
                                       dplyr::group_by(dataEX,
                                                       code, EXP, GCM,
                                                       RCM, BC),
                                       !!var:=mean(get(var),
                                                   na.rm=TRUE),
                                       .groups="drop")
                        dataEX = 
                            dplyr::summarise(dplyr::group_by(dataEX,
                                                             code, EXP,
                                                             GCM, RCM),
                                             !!var:=mean(get(var),
                                                         na.rm=TRUE),
                                             .groups="drop")
                        dataEX = 
                            dplyr::summarise(dplyr::group_by(dataEX,
                                                             code, EXP),
                                             !!var:=mean(get(var),
                                                         na.rm=TRUE),
                                             .groups="drop")
                    }
                } else {
                    dataEX = ASHE::read_tibble(var_path)
                    dataEX = add_chain(dataEX)
                    dataEX = filter_code(dataEX)
                    dataEX_save = dataEX
                }

                sampling_period =
                    unlist(strsplit(metaEX_var$sampling_period_en,
                                    ", "))

                if (length(sampling_period) == 2) {
                    SamplingPeriod =
                        dplyr::tibble(code=levels(factor(dataEX$code)),
                                      start=sampling_period[1],
                                      end=sampling_period[2])
                                        
                } else if (is_delta) {
                    SamplingPeriod = sampling_period
                } else {
                    SamplingPeriod =
                        dplyr::summarise(
                                   dplyr::group_by(dataEX, code),
                                   start=format(date[1], "%m-%d"),
                                   end=format(as.Date(paste0("1970-",
                                                             start))-1,
                                              "%m-%d"))
                }
                
                if (is_delta) { 
                    timestep = "year" #############
                } else {
                    dataEX$date =
                        as.Date(paste0(lubridate::year(dataEX$date),
                                       "-01-01"))
                    if (!is_TRACC) {
                        dataEX = debug_years(dataEX)
                    }
                    dataEX = dplyr::arrange(dataEX, code, date)
                    timestep = "year"
                }
                
                # dataEX$date =
                #     as.Date(paste0(lubridate::year(dataEX$date),
                #                    "-01-01"))
                # if (!is_TRACC) {
                #     dataEX = debug_years(dataEX)
                # }
                # dataEX = dplyr::arrange(dataEX, code, date)
                # timestep = "year"

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
                    # var_no_pattern = var
                    var_no_pattern = gsub("yr", "", var)
                }
                var_no_pattern = gsub("[_]$", "", var_no_pattern)
                metaEX_var$variable_en = var_no_pattern
                dataEX = dplyr::rename(dataEX, !!var_no_pattern:=var)
            }
            

            if (is_TRACC) {
                if (!is_delta) {
                    Date = dataEX_save$date
                    Date = seq.Date(min(Date), max(Date), by=timestep)
                    
                    Year_range = range(lubridate::year(Date))
                    duration = Year_range[2] - Year_range[1] + 1
                    if (duration < 20 & !is_delta) {
                        Date = seq.Date(min(Date),
                                        min(Date)+lubridate::years(19),
                                        by=timestep)
                        
                        dataEX = tidyr::complete(dataEX, code, date=Date)
                        dataEX = tidyr::fill(dplyr::group_by(dataEX,
                                                             code),
                                             dplyr::where(is.character),
                                             .direction="downup")
                        dataEX = dplyr::ungroup(dataEX)
                    }
                    central_year = format(Date[10], "%Y")
                    range_year = paste0(lubridate::year(min(Date)), "-",
                                        lubridate::year(max(Date)))
                }
            } else {
                if (is_delta) {
                    range_year =
                        paste0(lubridate::year(futur_period[1]),
                               "-",
                               lubridate::year(futur_period[2]))
                }
            }

            Code = levels(factor(dataEX$code))

            if (is_delta) {
                # dataEX_matrix =
                #     dplyr::select(dataEX, code,
                #                   dplyr::all_of(var_no_pattern))
                # dataEX_matrix = dplyr::select(dataEX_matrix, -code)
                # dataEX_matrix = as.matrix(dataEX_matrix)
                dataEX_matrix = dataEX[[var_no_pattern]]
            } else {
                dataEX_matrix =
                    dplyr::select(dataEX, code, date,
                                  dplyr::all_of(var_no_pattern))
                dataEX_matrix =
                    tidyr::pivot_wider(dataEX_matrix,
                                       names_from=code,
                                       values_from=
                                           dplyr::all_of(var_no_pattern))
                dataEX_matrix = dplyr::select(dataEX_matrix, -date)
                dataEX_matrix = t(as.matrix(dataEX_matrix))
            }


            ###
            initialise_NCf()
            
            list_path = list.files(script_dirpath,
                                   pattern='*.R$',
                                   full.names=TRUE)
            list_path = list_path[!grepl("DRIAS_export", list_path)]
            list_path = list_path[!grepl("script", list_path)]
            for (path in list_path) {
                source(path, encoding='UTF-8')
            }
            
            if (!(file.exists(out_dir))) {
                dir.create(out_dir)
            }

            NC_path = generate_NCf(out_dir=out_dir,
                                   chunksizes_list=NULL,
                                   return_path=TRUE,
                                   verbose=FALSE)

            # stop()

            
            ### verif ###
            NC_test = ncdf4::nc_open(NC_path)
            code_test = Code[runif(1, 1, length(Code))]
            Code_test = ncdf4::ncvar_get(NC_test, "code")

            if (!is_delta) {
                if (is_TRACC) {
                    Date_test = ncdf4::ncatt_get(NC_test, 0,
                                                 "indicator_time_range")$value
                    Date_test = unlist(strsplit(Date_test, "-"))
                    if (timestep == "year") {
                        Date_test = seq.Date(as.Date(paste0(Date_test[1],
                                                            "-01-01")),
                                             as.Date(paste0(Date_test[2],
                                                            "-01-01")),
                                             by=timestep)
                    } else if (timestep == "month") {
                        Date_test = seq.Date(as.Date(paste0(Date_test[1],
                                                            "-01-01")),
                                             as.Date(paste0(Date_test[2],
                                                            "-12-31")),
                                             by=timestep)       
                    } else {
                        stop("timestep not define")
                    }
                } else {
                    Date_test = ncdf4::ncvar_get(NC_test, "time") +
                        as.Date("1950-01-01")
                }
            }
            
            id_code = which(Code_test == code_test)
            if (is_delta) {
                Value_test =
                    ncdf4::ncvar_get(NC_test,
                                     metaEX_var$variable_en)[id_code]
            } else {
                Value_test =
                    ncdf4::ncvar_get(NC_test,
                                     metaEX_var$variable_en)[id_code,]
            }
            Value_test[!is.finite(Value_test)] = NA

            if (is_delta) {
                dataEX_test_list = lapply(var_path, ASHE::read_tibble)
                dataEX_test = purrr::reduce(dataEX_test_list,
                                            dplyr::bind_rows)
                dataEX_test = add_chain(dataEX_test)
                dataEX_test = filter_code(dataEX_test)
                dataEX_test = dplyr::rename(dataEX_test, !!var:=var_h)
                # if (!grepl("QMA_apr", var)) {
                    # dataEX_test$date =
                        # as.Date(paste0(lubridate::year(dataEX_test$date),
                                       # "-01-01"))
                # }
                if (is_TRACC) {
                    dataEX = dataEX ################
                } else {
                    dataEX_test = 
                        dplyr::summarise(
                                   dplyr::group_by(dataEX_test,
                                                   code, EXP, GCM,
                                                   RCM, BC),
                                   !!var:=mean(get(var),
                                               na.rm=TRUE),
                                   .groups="drop")
                    dataEX_test = 
                        dplyr::summarise(dplyr::group_by(dataEX_test,
                                                         code, EXP,
                                                         GCM, RCM),
                                         !!var:=mean(get(var),
                                                     na.rm=TRUE),
                                         .groups="drop")
                    dataEX_test = 
                        dplyr::summarise(dplyr::group_by(dataEX_test,
                                                         code, EXP),
                                         !!var:=mean(get(var),
                                                     na.rm=TRUE),
                                         .groups="drop")
                }
            } else {
                dataEX_test = ASHE::read_tibble(var_path)    
            }
            if (!is_TRACC & !is_delta) {
                dataEX_test = dplyr::filter(dataEX_test,
                                            date_min <= date)
            }
            dataEX_test = dplyr::filter(dataEX_test,
                                        code==code_test)

            if (!is_delta) {
                if (grepl("QMA_apr", var)) {
                    ok = lubridate::month(Date_test) == 4
                    Date_test = Date_test[ok]
                    Value_test = Value_test[ok]
                }
                Date_test = Date_test[!is.na(Value_test)]
                dataEX_test$year = lubridate::year(dataEX_test$date)
                if (!grepl("QMA_apr", var)) {
                    dataEX_test$date =
                        as.Date(paste0(dataEX_test$year, "-01-01"))
                }
            }
            Value_test = Value_test[!is.na(Value_test)]
            
            valEX = dataEX_test[[var]]
            if (!is_delta) {
                DateEX = dataEX_test$date
                DateEX = DateEX[!is.na(valEX)]
            }
            valEX = valEX[!is.na(valEX)]

            if (!is_delta) {
                ok1 = all(DateEX == Date_test)
            } else {
                ok1 = TRUE
            }
            ok2 = all.equal(valEX, Value_test, 0.001)


            meta_ALL_test = dplyr::filter(meta_ALL, code==code_test)

            if (!is_delta) {
                surface_test =
                    ncdf4::ncvar_get(NC_test,
                                     "topologicalSurface_model")[id_code]
                hm_test = gsub("[-]", "_", dataEX_test$HM[1])
                surface_var = paste0("surface_", hm_test, "_km2")
            }
            
            # there is some NA in surface in SAFRAN MORDOR-SD
            if (!is_SAFRAN & !is_delta) {
                ok3 = all.equal(meta_ALL_test[[surface_var]],
                                surface_test,
                                0.1)
            } else {
                ok3 = TRUE
            }

            L93_X_test = ncdf4::ncvar_get(NC_test, "L93_X")[id_code]
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

            # stop()
        }
    }  
    ncdf4::nc_close(NC)
} 

if (MPI != "") {
    Sys.sleep(10)
    mpi.finalize()
}
