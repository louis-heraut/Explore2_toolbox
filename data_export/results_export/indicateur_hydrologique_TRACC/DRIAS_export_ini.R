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
    data_dirpath = "/media/lheraut/lhrt_ext/archive/Documents/INRAE/data/Explore2/hydrologie/projection_merge"
    results_dirpath = "/media/lheraut/lhrt_ext/archive/Documents/INRAE/projects/Explore2_project/Explore2_toolbox/results/projection/hydrologie"
    MPI = "file"
}


if (grepl("botan", computer)) {
    out_dir = "NetCDF"
    script_dirpath = "."
    data_dirpath = "/home/louis/Documents/bouleau/INRAE/data/Explore2/hydrologie/projection_merge"
    results_dirpath = "/home/louis/Documents/bouleau/INRAE/project/Explore2_project/Explore2_toolbox/results/projection/hydrologie"
    MPI = ""
}

if (grepl("spiritx", computer)) {
    out_dir = "/scratchx/lheraut/NetCDF"
    script_dirpath = "/home/lheraut/library/Explore2_toolbox/data_export/results_export/DRIAS_1D"
    data_dirpath = "/scratchx/lheraut/data/Explore2/hydrologie/projection_merge"
    results_dirpath = "/scratchx/lheraut/projection"
    MPI = "file"
}

meta_projection_file = "tableau_metadata_EXPLORE2.csv"
Projection_file = "projections_selection.csv"
chain_to_remove_file = "chain_to_remove_adjust.csv"
meta_ALL_file = "stations_selection.csv"

verbose = TRUE


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
    # "^startLF_summer$", "^centerLF_summer$", "^dtLF_summer$"
    
    "^endLF$", "^endLF_summer$"
)
Variable_pattern = paste0("(", paste0(Variable, collapse=")|("), ")")
Variable_hyr = c("QJXA", "tQJXA", "tVCX3", "VCN10", "VCN3", "VCX3")

Season_pattern = "(DJF)|(MAM)|(JJA)|(SON)|(MJJASON)|(NDJFMA)"
Month = c("jan", "feb", "mar", "apr", "may", "jun",
          "jul", "aug", "sep", "oct", "nov", "dec")
Month_pattern = paste0("(", paste0(Month, collapse=")|("), ")")

date_min = "1975-01-01"


meta_projection = ASHE::read_tibble(file.path(script_dirpath,
                                              meta_projection_file))
Projection = ASHE::read_tibble(file.path(results_dirpath,
                                         Projection_file))
chain_to_remove = ASHE::read_tibble(file.path(results_dirpath,
                                              chain_to_remove_file))
meta_ALL = ASHE::read_tibble(file.path(results_dirpath,
                                       meta_ALL_file))
n_lim = 4 

data_Paths = list.files(data_dirpath,
                        pattern="[.]nc$",
                        full.names=TRUE,
                        recursive=TRUE)

Chain_dirpath = list.dirs(results_dirpath, recursive=FALSE)
Chain_dirpath = list.dirs(Chain_dirpath, recursive=FALSE)

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
add_chain = function (dataEX) {
    if (!is_SAFRAN) {
        dataEX = tidyr::unite(dataEX,
                              "Chain",
                              "GCM", "EXP",
                              "RCM", "BC",
                              "HM", sep="|",
                              remove=FALSE)
    } else {
        dataEX = tidyr::unite(dataEX,
                              "Chain",
                              "EXP", "HM", sep="|",
                              remove=FALSE)
    }
    return (dataEX) 
}

debug_years = function (dataEX, var) {
    Date = seq.Date(min(dataEX$date),
                    max(dataEX$date),
                    by="years")
    tmp = dplyr::distinct(dplyr::select(dataEX, -date))
    tmp = dplyr::reframe(dplyr::group_by(tmp, code, Chain),
                         date=Date)

    if (nrow(tmp) != nrow(dataEX)) {
        dataEX = dplyr::select(dataEX, Chain, code, date,
                               dplyr::all_of(var))
        dataEX = dplyr::full_join(dataEX, tmp,
                                  by=c("Chain", "code", "date"))
        if (!is_SAFRAN) {
            dataEX = tidyr::separate(dataEX, "Chain",
                                     c("GCM", "EXP",
                                       "RCM", "BC",
                                       "HM"), sep="[|]",
                                     remove=FALSE)
        } else {
            dataEX = tidyr::separate(dataEX, "Chain",
                                     c("EXP", "HM"),
                                     sep="[|]",
                                     remove=FALSE)
        }
        dataEX = dplyr::arrange(dataEX, Chain, code, date)
    }
    dataEX = dplyr::filter(dataEX, date_min <= date)
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
    
    regexp = gsub("historical[[][-][]]", "",
                  Projection$regexp[Projection$dir ==
                                    basename(chain_dirpath)])
    data_paths = data_Paths[grepl(regexp,
                                  basename(data_Paths))]
    data_path = data_paths[1]
    NC = ncdf4::nc_open(data_path)

    Var_path = list.files(chain_dirpath,
                          pattern="[.]fst",
                          full.names=TRUE,
                          recursive=TRUE)
    Var_path = Var_path[grepl(Variable_pattern,
                              basename(gsub("[.]fst", "", Var_path)))]
    Var_path = Var_path[!grepl("meta", basename(Var_path))]

    ### /!\ ###
    # Var_path = Var_path[grepl("startLF_summer", Var_path)]
    ###########
    nVar_path = length(Var_path)
    
    is_month_done = FALSE
    for (j in 1:nVar_path) {
        var_path = Var_path[j]
        var = gsub("[.]fst", "", basename(var_path))

        post(paste0("** ", j, " -> ",
                          round(j/nVar_path*100, 1), "%"))
        
        if (is_month_done & grepl(Month_pattern, var)) {
            next
        }

        metaEX_path =
            file.path(dirname(dirname(var_path)),
                      paste0(gsub("data", "meta",
                                  basename(dirname(var_path))),
                             ".fst"))
        metaEX = ASHE::read_tibble(metaEX_path)
        metaEX_var = metaEX[metaEX$variable_en == var,]

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
            dataEX = add_chain(dataEX)
            dataEX = filter_code(dataEX)

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
            dataEX = debug_years(dataEX, var)
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
        
        meta_path = file.path(dirname(dirname(var_path)), "meta.fst")
        meta = ASHE::read_tibble(meta_path)
        meta = dplyr::arrange(meta, code)

        if (!("date" %in% names(dataEX))) {
            next
        }

        Date = dataEX$date
        Date = seq.Date(min(Date), max(Date), by=timestep)
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

        Code = levels(factor(dataEX$code))

        dataEX_matrix = dplyr::select(dataEX, code, date,
                                      dplyr::all_of(var_no_pattern))
        dataEX_matrix =
            tidyr::pivot_wider(dataEX_matrix,
                               names_from=code,
                               values_from=dplyr::all_of(var_no_pattern))
        dataEX_matrix = dplyr::select(dataEX_matrix, -date)
        dataEX_matrix = t(as.matrix(dataEX_matrix))


        ###
        initialise_NCf()

        list_path = list.files(script_dirpath,
                               pattern='*.R$',
                               full.names=TRUE)
        list_path = list_path[!grepl("DRIAS_export.R", list_path)]
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
        
        dataEX_test = ASHE::read_tibble(var_path)
        dataEX_test = dplyr::filter(dataEX_test, date_min <= date)
        dataEX_test = dplyr::filter(dataEX_test,
                                    code==code_test)
        dataEX_test$year = lubridate::year(dataEX_test$date)
        if (!grepl("QMA_apr", var_path)) {
            dataEX_test$date =
                as.Date(paste0(dataEX_test$year, "-01-01"))
        }
        
        valEX = dataEX_test[[var]]
        dateEX = dataEX_test$date
        dateEX = dateEX[!is.na(valEX)]
        valEX = valEX[!is.na(valEX)]
        
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
    post("")
}


if (MPI != "") {
    Sys.sleep(10)
    mpi.finalize()
}
