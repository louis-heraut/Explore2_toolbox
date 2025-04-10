# Copyright 2024 Louis Héraut (louis.heraut@inrae.fr)*1,
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
# Explore2 R toolbox is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Explore2 R toolbox.
# If not, see <https://www.gnu.org/licenses/>.







## for by code
# by_dir = "by_code"

## create dirs
# Dirs = list.dirs(by_dir, full.names=TRUE, recursive=FALSE)
# Letters = gsub(".*[_]", "", Dirs)
# Letters = gsub("[[:digit:]]+", "", Letters)
# Letters = unique(Letters)
# Letters_dirpaths = file.path(by_dir, Letters)
# sapply(Letters_dirpaths, dir.create)

## move dirs
# Dirs = list.dirs(by_dir, full.names=TRUE, recursive=FALSE)
# Dirs = Dirs[grepl("data", Dirs)]
# Letters = gsub(".*[_]", "", Dirs)
# Letters = gsub("[[:digit:]]+", "", Letters)
# from = Dirs
# to = file.path(by_dir, Letters, basename(Dirs))
# file.copy(from, to)
# for (f in from) {
#     if (length(list.files(f)) == 0) {
#         unlink(f, recursive = TRUE)
#     }
# }

## move files
# Files = list.files(by_dir, ".fst", full.names=TRUE, recursive=FALSE)
# Files = Files[grepl("(data)|(meta)", Files)]
# Letters = gsub(".*[_]", "", Files)
# Letters = gsub("([[:digit:]]+)|([.]fst)", "", Letters)
# from = Files
# to = file.path(by_dir, Letters, basename(Files))
# file.copy(from, to)






# Paths = list.files(file.path("by_chain", "NetCDF"),
#                    ".nc", full.names=TRUE, recursive=TRUE)

# Paths = Paths[!grepl("_SAFRAN_", Paths)]
# Files = basename(Paths)

# Infos = strsplit(Files, "_")
# EXP_merged = c("historical-rcp26", "historical-rcp45",
#                "historical-rcp85", "SAFRAN")
# EXP = c("historical", "rcp26", "rcp45", "rcp85", "SAFRAN")
# GCM = unique(sapply(Infos, '[[', 10))
# RCM = unique(sapply(Infos, '[[', 11))
# BC = unique(sapply(Infos, '[[', 8))
# HM = gsub(".nc", "", unique(sapply(Infos, '[[', 12)))

# Letters = list.dirs("by_code", recursive=FALSE, full.names=FALSE)

# for (type in Types) {
#     if (grepl("projection", type)) {
#         if (grepl("by-chain", type)) {

#             if ((grepl("daily-time-series", type) &
#                  !grepl("merged", type)) |
#                 grepl("hourly-time-series", type)) {
#                 EXP_tmp = EXP
#             } else {
#                 EXP_tmp = EXP_merged
#             }

#             for (exp in EXP_tmp) {
#                 if (exp == "SAFRAN") {
#                     for (hm in HM) {
#                         dir.create(file.path(type, exp, hm),
#                                    recursive=TRUE)
#                     }
#                 } else {
#                     for (gcm in GCM) {
#                         for (rcm in RCM) {
#                             for (bc in BC) {
#                                 for (hm in HM) {
#                                     dir.create(file.path(type, exp, gcm,
#                                                          rcm, bc, hm),
#                                                recursive=TRUE)
#                                 }
#                             }
#                         }
#                     }
#                 }
#             }

#         } else {
#             sapply(file.path(type, Letters), dir.create, recursive=TRUE)
#         }

#     } else {
#         dir.create(type)
#     }
# }



download_DRIAS =
    # TRUE
    FALSE


to_archive = c(
    ## diag
    #### daily-time-series
    # "hydrological-diagnostic_daily-time-series_netcdf",
    # "hydrological-diagnostic_daily-time-series_fst",
    #### variables
    # "hydrological-diagnostic_yearly-variables_fst",
    # "hydrological-diagnostic_criteria_fst",

    ## proj
    ### climatological-projection
    # "climatological-projection_daily-time-series_by-chain_netcdf" #B
    # "climatological-projection_hourly-time-series_by-chain_netcdf",

    ### hydrological-projection
    #### daily-time-series
    # "hydrological-projection_daily-time-series_by-chain_raw-netcdf" #B
    # "hydrological-projection_daily-time-series_by-chain_cleaned-netcdf" #B
    # "hydrological-projection_daily-time-series_by-chain_merged-netcdf" #B
    #### variables
    # "hydrological-projection_series-by-horizon_by-chain_fst", #B
    # "hydrological-projection_series-by-horizon_by-code_fst" #B
    # "hydrological-projection_changes-by-horizon_by-chain_fst", #B
    # "hydrological-projection_changes-by-horizon_by-code_fst" #B
    "hydrological-projection_yearly-variables_by-chain_netcdf" #B
    # "hydrological-projection_yearly-variables_by-chain_fst" #B
    # "hydrological-projection_yearly-variables_by-code_fst" #B
    # "hydrological-projection_daily-variables_by-chain_fst" #A
)


## Work Paths ________________________________________________________
archive_base_dir = "/media/lheraut/Explore2"
local_resdir =
    # "results"
    "/media/lheraut/lhrt_ext/archive/Documents/INRAE/projects/Explore2_project/Explore2_toolbox/results"
local_resources_dir = "resources"
external_resdir =
    # "/media/lheraut/SUPER_LOU/archive/project/Explore2_project/Explore2_toolbox/results"
    "/media/lheraut/Explore2/"
external_datadir = "/media/lheraut/SUPER_LOU/archive/data/Explore2/hydrologie"

example_projection_dir = "CTRIP/CNRM-CM5_historical-rcp26_ALADIN63_ADAMONT_CTRIP"

URL_DRIAS_file = "URL_DRIAS.txt"
output_DRIAS_dir = "output_DRIAS"

## Meta variables ____________________________________________________
meta_variables_paths = list.files(file.path(local_resdir,
                                            "projection", "hydrologie",
                                            example_projection_dir),
                                  pattern="metaEX.*[.]fst",
                                  full.names=TRUE, recursive=TRUE) 

### meta variables serie _____________________________________________
meta_variables_serie_paths = meta_variables_paths[grepl("serie", meta_variables_paths)]
meta_variables_serie = dplyr::tibble()
for (meta_variables_path in meta_variables_serie_paths) {
    meta_variables_serie =
        dplyr::bind_rows(meta_variables_serie,
                         ASHE::read_tibble(meta_variables_path))
}
meta_variables_serie = dplyr::arrange(meta_variables_serie, variable_en)

### meta variables criteria __________________________________________
meta_variables_criteria_paths = meta_variables_paths[grepl("criteria", meta_variables_paths)]
meta_variables_criteria = dplyr::tibble()
for (meta_variables_path in meta_variables_criteria_paths) {
    meta_variables_criteria =
        dplyr::bind_rows(meta_variables_criteria,
                         ASHE::read_tibble(meta_variables_path))
}
meta_variables_criteria = dplyr::arrange(meta_variables_criteria, variable_en)


## Meta codes ________________________________________________________
codes_selection_file = "stations_selection.csv"
meta_codes = ASHE::read_tibble(file.path(local_resdir, "projection",
                                         "hydrologie",
                                         codes_selection_file))

## Meta chains  ______________________________________________________
chains_selection_file = "projections_selection.csv"
meta_chains_merged = ASHE::read_tibble(file.path(local_resdir,
                                                 "projection",
                                                 "hydrologie",
                                                 chains_selection_file))

meta_chains_merged = dplyr::select(meta_chains_merged,
                                   -c(file, dir, regexp, Chain, climateChain))
meta_chains_merged = tidyr::unite(meta_chains_merged,
                                  col=climateChain,
                                  EXP, GCM, RCM, BC,
                                  sep="|", na.rm=TRUE, remove=FALSE)
meta_chains_merged = tidyr::unite(meta_chains_merged,
                                  col=Chain,
                                  EXP, GCM, RCM, BC, HM,
                                  sep="|", na.rm=TRUE, remove=FALSE)
meta_chains_merged = tidyr::unite(meta_chains_merged,
                                  col=path,
                                  EXP, GCM, RCM, BC, HM,
                                  sep="/", na.rm=TRUE, remove=FALSE)
meta_chains_merged = dplyr::relocate(meta_chains_merged, climateChain,
                                     .after=HM)
meta_chains_merged = dplyr::relocate(meta_chains_merged, Chain,
                                     .after=climateChain)
meta_chains_merged = dplyr::relocate(meta_chains_merged, path,
                                     .after=Chain)
meta_chains_merged = dplyr::relocate(meta_chains_merged, EXP,
                                     .before=GCM)

Dirpaths = meta_chains_merged$path
Dirpaths = gsub("historical[-]", "", Dirpaths)
Dirpaths_SAFRAN = Dirpaths[grepl("SAFRAN", Dirpaths)]
Dirpaths_rcp = Dirpaths[grepl("rcp", Dirpaths)]
Dirpaths_historical = unique(gsub("rcp[[:digit:]]+",
                                  "historical", Dirpaths_rcp))
Dirpaths = c(Dirpaths_historical, Dirpaths_rcp, Dirpaths_SAFRAN)
Chain = gsub("/", "|", Dirpaths)
Chain2separate = gsub("SAFRAN[|]", "SAFRAN|NA|NA|NA|", Chain)
climateChain = stringr::str_extract(Chain, ".*[|]")
climateChain = gsub("[|]$", "", climateChain)

meta_chains = dplyr::tibble(Chain2separate=Chain2separate,
                            climateChain=climateChain,
                            Chain=Chain,
                            path=Dirpaths)
meta_chains = tidyr::separate(meta_chains,
                              Chain2separate,
                              c("EXP", "GCM", "RCM", "BC", "HM"),
                              sep="[|]")

meta_chains_tmp = dplyr::filter(meta_chains,
                                EXP != "SAFRAN")
Dirpaths = file.path(meta_chains_tmp$EXP,
                     meta_chains_tmp$GCM,
                     meta_chains_tmp$RCM,
                     meta_chains_tmp$BC)
Dirpaths = unique(Dirpaths)
climateChain = gsub("/", "|", Dirpaths)

meta_chains_climate = dplyr::tibble(climateChain=climateChain,
                                    path=Dirpaths)
meta_chains_climate = tidyr::separate(meta_chains_climate,
                                      climateChain,
                                      c("EXP", "GCM", "RCM", "BC"),
                                      remove=FALSE,
                                      sep="[|]")
meta_chains_climate = dplyr::relocate(meta_chains_climate,
                                      climateChain, .before=path)


## Tools _____________________________________________________________
remove_empty_dirs = function(root_dir) {
    dirs = list.dirs(root_dir, recursive=TRUE, full.names=TRUE)
    dirs = dirs[order(nchar(dirs), decreasing=TRUE)]
    for (dir in dirs) {
        if (length(list.files(dir, all.files=TRUE,
                              recursive=TRUE)) == 0) {
            unlink(dir, recursive=TRUE)
        }
    }
}


get_structure = function (by="chain",
                          is_merged=TRUE,
                          is_climate=FALSE) {
    if (by == "chain") {
        if (is_climate) {
            meta_chains_tmp = meta_chains_climate
        } else {
            if (is_merged) {
                meta_chains_tmp = meta_chains_merged
            } else {
                meta_chains_tmp = meta_chains
            }
        }
        Dirpaths = meta_chains_tmp$path
        
    } else if (by == "code") {
        Dirpaths = list.dirs(file.path(external_resdir,
                                       "projection_for_figure",
                                       "hydrologie"),
                             recursive=FALSE,
                             full.names=FALSE)
        Dirpaths = stringr::str_extract(Dirpaths,
                                        "[[:alpha:]][[:digit:]]{2}")
        Dirpaths = file.path(substr(Dirpaths, 1, 1), Dirpaths)
        Dirpaths = unique(Dirpaths)
    }
    return (list(Dirpaths=Dirpaths, meta_chains=meta_chains_tmp))
}

get_data_archive = function (archive_dir, From,
                             by="chain",
                             uncompress=FALSE,
                             is_merged=TRUE,
                             is_shitty=FALSE,
                             is_climate=FALSE,
                             mode="copy",
                             test=FALSE) {

    if (by == "chain") {
        Dirpaths = get_structure(by=by,
                                 is_merged=is_merged,
                                 is_climate=is_climate)$Dirpaths
        Dirpaths_list = strsplit(Dirpaths, split="/")
        nDirpaths = length(Dirpaths)

        print("archive data")
        for (i in 1:nDirpaths) {
            if (i %% 2 == 0) {
                print(paste0(round(i/nDirpaths*100, 1), "%"))
            }
            dirpath_list = Dirpaths_list[[i]]
            
            exp = dirpath_list[1]
            if (exp == "SAFRAN") {
                if (is_shitty) {
                    exp_test = "SAFRAN-France-20"
                } else {
                    exp_test = "SAFRAN"
                }
                if (is_climate) {
                    Ok = grepl(exp_test, From)
                } else {
                    hm = dirpath_list[2]
                    Ok = grepl(exp_test, From) &
                        grepl(hm, From) 
                }
                
                if (any(Ok)) {
                    from = From[Ok]
                    if (is_climate) {
                        dir = file.path(archive_base_dir, archive_dir,
                                        exp)
                    } else {
                        dir = file.path(archive_base_dir, archive_dir,
                                        exp, hm)
                    }
                    to = file.path(dir, basename(from))
                    if (!dir.exists(dir)) {
                        dir.create(unique(dirname(to)), recursive=TRUE)
                    }
                    if (!test) {
                        if (mode == "copy") {
                            file.copy(from, to)
                        } else if (mode == "move") {
                            file.rename(from, to)
                        }
                    }
                } else {
                    if (is_climate) {
                        message(paste(exp))
                    } else {
                        message(paste(exp, hm))
                    }
                }
                
            } else {
                gcm = dirpath_list[2]
                rcm = dirpath_list[3]
                bc = dirpath_list[4]
                if (is_climate) {
                    Ok = grepl(exp, From) &
                        grepl(gcm, From) &
                        grepl(rcm, From) &
                        grepl(bc, From)
                } else {
                    hm = dirpath_list[5]
                    Ok = grepl(exp, From) &
                        grepl(gcm, From) &
                        grepl(rcm, From) &
                        grepl(bc, From) &
                        grepl(hm, From)
                }

                if (any(Ok)) {
                    from = From[Ok]
                    if (is_climate) {
                        dir = file.path(archive_base_dir, archive_dir,
                                        exp, gcm, rcm, bc)
                    } else {
                        dir = file.path(archive_base_dir, archive_dir,
                                        exp, gcm, rcm, bc, hm)
                    }
                    to = file.path(dir, basename(from))
                    if (!dir.exists(dir)) {
                        dir.create(unique(dirname(to)), recursive=TRUE)
                    }
                    if (!test) {
                        if (mode == "copy") {
                            file.copy(from, to)
                        } else if (mode == "move") {
                            file.rename(from, to)
                        }
                    }
                } else {
                    if (is_climate) {
                        message(paste(exp, gcm, rcm, bc))
                    } else {
                        message(paste(exp, gcm, rcm, bc, hm))
                    }
                }
            }
        }

    } else if (by == "code") {
        Dirpaths = get_structure(by=by,
                                 is_merged=is_merged,
                                 is_climate=is_climate)$Dirpaths
        nDirpaths = length(Dirpaths)
        
        print("archive data")
        for (i in 1:nDirpaths) {
            if (i %% 2 == 0) {
                print(paste0(round(i/nDirpaths*100, 1), "%"))
            }
            dirpath = Dirpaths[i]
            Ok = grepl(gsub(".*[/]", "", dirpath), From)
            
            if (any(Ok)) {
                from = From[Ok]
                dir = file.path(archive_base_dir, archive_dir,
                                dirpath)
                to = file.path(dir, basename(from))  
                if (!dir.exists(dir)) {
                    dir.create(unique(dirname(to)), recursive=TRUE)
                }
                if (!test) {
                    file.copy(from, to)
                }
            } else {
                message(dirpath)
            }
        }
    }

    if (uncompress) {
        print("uncompress archive data")
        for (i in 1:nDirpaths) {
            if (i %% 2 == 0) {
                print(paste0(round(i/nDirpaths*100, 1), "%"))
            }
            dirpath = Dirpaths[i]
            dataEX_paths = list.files(file.path(archive_base_dir,
                                                archive_dir,
                                                dirpath),
                                      pattern="dataEX",
                                      full.names=TRUE)
            for (dataEX_path in dataEX_paths) {
                dataEX = ASHE::read_tibble(dataEX_path)
                ID_names = names(dataEX)
                ID_names = ID_names[sapply(dataEX, is.character)]
                Variables_H = names(dataEX)
                Variables_H = Variables_H[sapply(dataEX, is.numeric)]
                Variables = gsub("[_]H[[:digit:]]", "", Variables_H)
                
                for (variable in Variables) {
                    variables_h = Variables_H[grepl(variable,
                                                    Variables_H,
                                                    fixed=TRUE)]
                    dataEX_variable =
                        dplyr::select(dataEX,
                                      dplyr::all_of(c(ID_names,
                                                      variables_h)))
                    ASHE::write_tibble(dataEX_variable,
                                       path=file.path(archive_base_dir,
                                                         archive_dir,
                                                      dirpath,
                                                      paste0(variable, ".fst")))
                    
                }
            }
            unlink(dataEX_paths)
        }
    }
}

get_meta_archive = function (archive_dir,
                             meta_variables=NULL,
                             by="chain",
                             is_merged=TRUE,
                             is_climate=FALSE) {

    res = get_structure(by=by,
                        is_merged=is_merged,
                        is_climate=is_climate)
    Dirpaths = res$Dirpaths
    nDirpaths = length(Dirpaths)
    meta_chains_tmp = res$meta_chains
    
    if (!is.null(meta_variables)) {
        print("archive meta variables")
        for (i in 1:nDirpaths) {
            if (i %% 2 == 0) {
                print(paste0(round(i/nDirpaths*100, 1), "%"))
            }
            dirpath = Dirpaths[i]
            ASHE::write_tibble(meta_variables,
                               path=file.path(archive_base_dir,
                                              archive_dir,
                                              dirpath,
                                              "meta_variables.csv"))
        }
    }

    print("archive meta chains")
    for (i in 1:nDirpaths) {
        if (i %% 2 == 0) {
            print(paste0(round(i/nDirpaths*100, 1), "%"))
        }
        dirpath = Dirpaths[i]
        if (by == "chain") {
            meta_chains_tmp2 = dplyr::filter(meta_chains_tmp,
                                             path == dirpath)
        } else {
            meta_chains_tmp2 = meta_chains_tmp
        }
        ASHE::write_tibble(meta_chains_tmp2,
                           path=file.path(archive_base_dir,
                                          archive_dir,
                                          dirpath,
                                          "meta_chains.csv"))
    }

    print("archive meta codes")
    if (!is_climate) {
        for (i in 1:nDirpaths) {
            if (i %% 2 == 0) {
                print(paste0(round(i/nDirpaths*100, 1), "%"))
            }
            dirpath = Dirpaths[i]
            path = list.files(file.path(archive_base_dir,
                                        archive_dir,
                                        dirpath),
                              pattern="([.]nc)|([.]fst)", 
                              full.names=TRUE)[1]
            format = gsub(".*[.]", "", path)
            if (format == "fst") {
                Code_tmp = unique(ASHE::read_tibble(path)$code)
            } else if (format == "nc") {
                Code_tmp =
                    ncdf4::ncvar_get(ncdf4::nc_open(path), "code")
            }
            meta_codes_tmp = dplyr::filter(meta_codes, code
                                           %in% Code_tmp)
            ASHE::write_tibble(meta_codes_tmp,
                               path=file.path(archive_base_dir,
                                              archive_dir,
                                              dirpath,
                                              "meta_codes.csv"))
        }
    }
}


## Process ___________________________________________________________
if (download_DRIAS) {
    options(timeout=300)
    URLs = readLines(file.path(local_resources_dir, URL_DRIAS_file))
    output_dir = file.path(archive_base_dir, output_DRIAS_dir)
    if (!dir.exists(output_dir)) {
        dir.create(output_dir, showWarnings=FALSE)
    }
    nURL = length(URLs)
    start = 1
    for (i in start:nURL) {
        url = URLs[i]
        print(paste0(i, "/", nURL, " -> ",
                     round(i/nURL*100, 2), "%"))
        file = basename(url)
        path = file.path(output_dir, file)
        download.file(url, destfile=path, mode="wb")
    }
}


### 0 ________________________________________________________________
archive_dir = "climatological-projection_daily-time-series_by-chain_netcdf"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(archive_base_dir, output_DRIAS_dir),
                      pattern=".nc",
                      full.names=TRUE, recursive=TRUE)

    get_data_archive(archive_dir, From,
                     by="chain",
                     is_merged=FALSE,
                     is_climate=TRUE,
                     mode="move")
    
    get_meta_archive(archive_dir, meta_variables=NULL,
                     by="chain",
                     is_merged=FALSE,
                     is_climate=TRUE)
}


### 1 ________________________________________________________________
archive_dir = "hydrological-projection_daily-time-series_by-chain_raw-netcdf"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(external_datadir, "projection"),
                      pattern=".nc",
                      full.names=TRUE, recursive=TRUE)

    get_data_archive(archive_dir, From,
                     by="chain",
                     is_merged=FALSE,
                     is_shitty=TRUE)
    
    get_meta_archive(archive_dir, meta_variables=NULL,
                     by="chain",
                     is_merged=FALSE)
}


### 2 ________________________________________________________________
archive_dir = "hydrological-projection_daily-time-series_by-chain_cleaned-netcdf"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(external_datadir, "projection_clean"),
                      pattern=".nc",
                      full.names=TRUE, recursive=TRUE)

    get_data_archive(archive_dir, From,
                     by="chain",
                     is_merged=FALSE,
                     is_shitty=TRUE)
    
    get_meta_archive(archive_dir, meta_variables=NULL,
                     by="chain",
                     is_merged=FALSE)
}


### 3 ________________________________________________________________
archive_dir = "hydrological-projection_daily-time-series_by-chain_merged-netcdf"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(external_datadir, "projection_merge"),
                      pattern=".nc",
                      full.names=TRUE, recursive=TRUE)

    get_data_archive(archive_dir, From,
                     by="chain",
                     is_shitty=TRUE)
    
    get_meta_archive(archive_dir, meta_variables=NULL,
                     by="chain")
}


### 4 ________________________________________________________________
#### by_chain ________________________________________________________
archive_dir = "hydrological-projection_series-by-horizon_by-chain_fst"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(local_resdir, "projection",
                                "hydrologie"),
                      pattern=".fst",
                      full.names=TRUE, recursive=TRUE)

    pattern = "(medQJ_H)|(QM_H)|(FDC_H)"
    From = From[grepl(pattern, From)]
    get_data_archive(archive_dir, From,
                     by="chain")

    meta_variables = dplyr::filter(meta_variables_serie,
                                   grepl(pattern,
                                         variable_en))
    get_meta_archive(archive_dir, meta_variables,
                     by="chain")
}

#### by_code _________________________________________________________
archive_dir = "hydrological-projection_series-by-horizon_by-code_fst"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(external_resdir,
                                "projection_for_figure",
                                "hydrologie"),
                      pattern=".fst",
                      full.names=TRUE, recursive=TRUE)

    pattern = "medQJ_H"
    From = From[grepl(pattern, From)]
    get_data_archive(archive_dir, From,
                     by="code")

    meta_variables = dplyr::filter(meta_variables_serie,
                                   grepl(pattern,
                                         variable_en))
    get_meta_archive(archive_dir, meta_variables,
                     by="code")
}


### 5 ________________________________________________________________
#### by_chain ________________________________________________________
archive_dir = "hydrological-projection_changes-by-horizon_by-chain_fst"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(local_resdir, "projection",
                                "hydrologie"),
                      pattern=".fst",
                      full.names=TRUE, recursive=TRUE)

    pattern = "dataEX.*criteria"
    From = From[grepl(pattern, From)]
    get_data_archive(archive_dir, From,
                     by="chain",
                     uncompress=TRUE)
    
    meta_variables = meta_variables_criteria
    get_meta_archive(archive_dir, meta_variables,
                     by="chain")
}

#### by_code _________________________________________________________
archive_dir = "hydrological-projection_changes-by-horizon_by-code_fst"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(external_resdir,
                                "projection_for_figure",
                                "hydrologie"),
                      pattern=".fst",
                      full.names=TRUE, recursive=TRUE)

    pattern = "dataEX.*criteria"
    From = From[grepl(pattern, From)]
    get_data_archive(archive_dir, From,
                     by="code",
                     uncompress=TRUE)

    To = list.files(file.path(archive_base_dir, archive_dir),
                    pattern=".fst",
                    full.names=TRUE, recursive=TRUE)
    Variables = gsub("[.]fst", "", unique(basename(To)))
    Variables_H = paste0(rep(Variables, each=3),
                         rep(c("_H1", "_H2", "_H3"), length(Variables)))
    meta_variables = dplyr::filter(meta_variables_criteria,
                                   variable_en %in% Variables_H)
    get_meta_archive(archive_dir, meta_variables,
                     by="code")
}


### 6 ________________________________________________________________
archive_dir = "hydrological-projection_yearly-variables_by-chain_netcdf"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(external_resdir, "NetCDF"),
                      pattern=".nc",
                      full.names=TRUE, recursive=TRUE)
    get_data_archive(archive_dir, From,
                     by="chain")

    meta_variables = meta_variables_serie
    Variables_table = 
        c("centerLF_summer"="centerLF_seas-MJJASON",
          "centerLF$"="centerLF_yr",
          "dtFlood"="dtFlood_yr",
          "dtLF_summer"="dtLF_seas-MJJASON",
          "dtLF$"="dtLF_yr",
          "Q05A"="Q05A_yr",
          "Q10A"="Q10A_yr",
          "Q50A"="Q50A_yr",
          "Q90A"="Q90A_yr",
          "Q95A"="Q95A_yr",
          "QMA[_].*"="QA_mon",
          "QSA_DJF"="QA_seas-DJF",
          "QSA_JJA"="QA_seas-JJA",
          "QSA_MAM"="QA_seas-MAM",
          "QSA_SON"="QA_seas-SON",
          "QA"="QA_yr",
          "QJXA"="QJXA_hyr",
          "QMNA$"="QMNA_yr",
          "startLF_summer"="startLF_seas-MJJASON",
          "startLF$"="startLF_yr",
          "endLF_summer"="endLF_seas-MJJASON",
          "endLF$"="endLF_yr",
          "tQJXA"="tQJXA_hyr",
          "tVCX10"="tVCX10_yr",
          "tVCX3"="tVCX3_hyr",
          "^VCN10$"="VCN10_hyr",
          "VCN10_summer"="VCN10_seas-MJJASON",
          "VCN30_summer"="VCN30_seas-MJJASON",
          "VCN30$"="VCN30_yr",
          "VCN3$"="VCN3_hyr",
          "VCN3_summer"="VCN3_seas-MJJASON",
          "VCX10"="VCX10_yr",
          "VCX3"="VCX3_hyr")

    meta_variables$variable_in_nc = NA
    for (i in 1:length(Variables_table)) {
        variable_nc = Variables_table[i]
        variable = names(Variables_table)[i]
        ok = grepl(variable, meta_variables$variable_en)
        meta_variables$variable_in_nc[ok] = variable_nc
    }
    meta_variables = dplyr::relocate(meta_variables,
                                     variable_in_nc,
                                     .before=variable_en)
    meta_variables = dplyr::filter(meta_variables, !is.na(variable_in_nc))
    
    get_meta_archive(archive_dir, meta_variables,
                     by="chain")
}


### 7 ________________________________________________________________
#### by_chain ________________________________________________________
archive_dir = "hydrological-projection_yearly-variables_by-chain_fst"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(local_resdir, "projection",
                                "hydrologie"),
                      pattern=".fst",
                      full.names=TRUE, recursive=TRUE)

    From = From[grepl("serie", From) &
                !grepl("meta", From) &
                !grepl("(medQJ_H)|(QM_H)|(FDC_H)", From)]
    get_data_archive(archive_dir, From,
                     by="chain")

    Variable = gsub("[.]fst", "", unique(basename(From)))
    meta_variables = dplyr::filter(meta_variables_serie,
                                   variable_en %in% Variable)
    get_meta_archive(archive_dir, meta_variables,
                     by="chain")
}

#### by_code _________________________________________________________
archive_dir = "hydrological-projection_yearly-variables_by-code_fst"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(external_resdir,
                                "projection_for_figure",
                                "hydrologie"),
                      pattern=".fst",
                      full.names=TRUE, recursive=TRUE)

    From = From[grepl("serie", From) &
                !grepl("meta", From) &
                !grepl("medQJ_H", From)]
    get_data_archive(archive_dir, From,
                     by="code")

    Variable = gsub("[.]fst", "", unique(basename(From)))
    meta_variables = dplyr::filter(meta_variables_serie,
                                   variable_en %in% Variable)
    get_meta_archive(archive_dir, meta_variables,
                     by="code")
}


### 8 ________________________________________________________________
archive_dir = "hydrological-projection_daily-variables_by-chain_fst"
if (archive_dir %in% to_archive) {
    From = list.files(file.path(external_resdir, "BF"),
                      pattern=".fst",
                      full.names=TRUE, recursive=TRUE)

    meta_variables_path = From[grepl("metaEX", From)][1]
    meta_variables = ASHE::read_tibble(meta_variables_path)
    
    pattern = "BF_LH"
    From = From[grepl(pattern, From)]
    get_data_archive(archive_dir, From,
                     by="chain")

    get_meta_archive(archive_dir, meta_variables,
                     by="chain")
}













### tmp path _________________________________________________________ 
# Paths_tmp = list.files(file.path(archive_base_dir, archive_dir), pattern="meta_chain", full.names=TRUE, recursive=TRUE)

# Paths_tmp = list.dirs(base_dir, full.names=TRUE, recursive=TRUE)

### rename ___________________________________________________________
# file.copy(Paths_tmp, gsub("LSCE-IPSL-CDFt", "CDFt", Paths_tmp))
### rewrite __________________________________________________________
# rewrite = function(path) {
#     ASHE::write_tibble(ASHE::read_tibble(path),
#                        filedir=dirname(path),
#                        filename="meta_variables.csv")
# }
# sapply(Paths_tmp, rewrite)
### delete ___________________________________________________________
# unlink(Paths_tmp)
# remove_empty_dirs(file.path(archive_base_dir, archive_dir))

# for (dir in Types) {
# Paths_tmp = list.files(dir, pattern="meta[_]chains",
#                        full.names=TRUE, recursive=TRUE)
# for (path in Paths_tmp) {
#     ASHE::write_tibble(meta_projection,
#                        filedir=dirname(path),
#                        filename="meta_chains.csv")
# }

# Paths_tmp = list.dirs(dir, full.names=TRUE, recursive=TRUE)
# file.copy(Paths_tmp, gsub("LSCE-IPSL-CDFt", "CDFt", Paths_tmp))

# Paths_tmp = list.files(dir, pattern="meta_projections", full.names=TRUE, recursive=TRUE)
# file.copy(Paths_tmp, gsub("meta_projections", "meta_chains", Paths_tmp))
# unlink(Paths_tmp)
# }





## initialisation ____________________________________________________
### by_code __________________________________________________________
# Paths = list.files("by_code", pattern=".fst",
#                    full.names=TRUE, recursive=TRUE)
# metaEX_serie_path = Paths[grepl("metaEX_serie", Paths)][1]
# metaEX_serie = ASHE::read_tibble(metaEX_serie_path)
# metaEX_criteria_path = Paths[grepl("metaEX_criteria", Paths)][1]
# metaEX_criteria = ASHE::read_tibble(metaEX_criteria_path)


## hydrological-projection_series-by-horizon_by-code_fst _____________
# base_dir = "hydrological-projection_series-by-horizon_by-code_fst"

# From = Paths[grepl("medQJ", Paths)]
# Letters_Digits = gsub(".*[_]", "", dirname(From))
# Digits = substr(Letters_Digits, 2, 3)
# Letters = substr(Letters_Digits, 1, 1)

# To_dirpath = file.path(base_dir, Letters, Digits)
# for (dirpath in To_dirpath) {
#     if (!dir.exists(dirpath)) {
#         dir.create(dirpath, recursive=TRUE)
#     }
# }
# To = file.path(To_dirpath, basename(From))
# file.copy(From, To)

# metaEX = dplyr::filter(metaEX_serie, grepl("medQJ", variable_en))
# sapply(To_dirpath, ASHE::write_tibble, tbl=metaEX, filename="meta_variables.csv")

# sapply(To_dirpath, ASHE::write_tibble, tbl=meta_projection, filename="meta_chains.csv")

# for (dirpath in To_dirpath) {
#     Code_tmp =
#         unique(ASHE::read_tibble(file.path(dirpath,
#                                            "medQJ_H0.fst"))$code)
#     meta_station_tmp = dplyr::filter(meta_station, code %in% Code_tmp)
#     ASHE::write_tibble(meta_station_tmp,
#                        filedir=dirpath,
#                        filename="meta_codes.csv")
# }


## hydrological-projection_changes-by-horizon_by-code_fst ____________
# base_dir = "hydrological-projection_changes-by-horizon_by-code_fst"

# From = Paths[grepl("dataEX_criteria", Paths)]
# Letters_Digits = gsub(".fst", "", gsub(".*[_]", "", basename(From)))
# Digits = substr(Letters_Digits, 2, 3)
# Letters = substr(Letters_Digits, 1, 1)

# To_dirpath = file.path(base_dir, Letters, Digits)
# for (dirpath in To_dirpath) {
#     if (!dir.exists(dirpath)) {
#         dir.create(dirpath, recursive=TRUE)
#     }
# }
# To = file.path(To_dirpath, "changes.fst")
# file.copy(From, To)

# sapply(To_dirpath, ASHE::write_tibble, tbl=metaEX_criteria, filename="meta_variables.csv")

# sapply(To_dirpath, ASHE::write_tibble, tbl=meta_projection, filename="meta_chains.csv")

# for (dirpath in To_dirpath) {
#     Code_tmp =
#         unique(ASHE::read_tibble(file.path(dirpath,
#                                            "changes.fst"))$code)
#     meta_station_tmp = dplyr::filter(meta_station, code %in% Code_tmp)
#     ASHE::write_tibble(meta_station_tmp,
#                        filedir=dirpath,
#                        filename="meta_codes.csv")
# }


## hydrological-projection_yearly-variables_by-code_fst ______________
# base_dir = "hydrological-projection_yearly-variables_by-code_fst"

# From = Paths[grepl("dataEX_serie", Paths)]
# Letters_Digits = gsub(".*[_]", "", dirname(From))
# Digits = substr(Letters_Digits, 2, 3)
# Letters = substr(Letters_Digits, 1, 1)

# To_dirpath = file.path(base_dir, Letters, Digits)
# for (dirpath in To_dirpath) {
#     if (!dir.exists(dirpath)) {
#         dir.create(dirpath, recursive=TRUE)
#     }
# }
# To = file.path(To_dirpath, basename(From))
# file.copy(From, To)

# metaEX = dplyr::filter(metaEX_serie, !grepl("medQJ", variable_en))
# sapply(To_dirpath, ASHE::write_tibble, tbl=metaEX, filename="meta_variables.csv")

# sapply(To_dirpath, ASHE::write_tibble, tbl=meta_projection, filename="meta_chains.csv")

# for (dirpath in To_dirpath) {
#     Code_tmp =
#         unique(ASHE::read_tibble(file.path(dirpath,
#                                            "QA.fst"))$code)
#     meta_station_tmp = dplyr::filter(meta_station, code %in% Code_tmp)
#     ASHE::write_tibble(meta_station_tmp,
#                        filedir=dirpath,
#                        filename="meta_codes.csv")
# }




