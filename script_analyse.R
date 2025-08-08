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
# along with Explore2 R toolbox.
# If not, see <https://www.gnu.org/licenses/>.


## 1. ANALYSING OF DATA ______________________________________________
if ('analyse_data' %in% to_do) {

    # if (mode == "diagnostic") {
    #     ASHE::post("### Analaysing data")
    
    #     print("number of simulated station")
    #     print(summarise(group_by(dataEX_criteria, HM),
    #                     n=length(unique(code))))
    #     print("")
    
    #     print("Choice of best modele for complete")
    #     # which(is.na(meta$Surface_km2))
    #     # which(is.na(meta[["Surface_MORDOR-SD_km2"]]))
    #     # which(is.na(meta$Surface_SMASH_km2))
    #     SMASH_surface =
    #         sum(abs(meta$surface_km2 - meta[["surface_SMASH_km2"]]) /
    #             meta$surface_km2,
    #             na.rm=TRUE)
    #     MORDOR_SD_surface =
    #         sum(abs(meta$surface_km2 - meta[["surface_MORDOR-SD_km2"]]) /
    #             meta$surface_km2,
    #             na.rm=TRUE)
    #     print("SMASH")
    #     print(SMASH_surface)
    #     print("MORDOR-SD")
    #     print(MORDOR_SD_surface)
    #     print("")
    
    #     print("Surface relative supérieur à 50 %")
    #     for (hm in HM_to_use) {
    #         dS_rel = abs(meta[[paste0("surface_", hm, "_km2")]] - meta$surface_km2)/meta$surface_km2
    #         dS_rel[is.na(dS_rel)] = 0
    #         print(hm)
    #         print(meta$code[dS_rel>0.2])
    #     }
    # }






    # dataEX =
    #     full_join(
    #         summarise(group_by(
    #             filter(dataEX,
    #                    historical[1] <= date &
    #                    date <= historical[2]),
    #             code, Chain, GCM, RCM, BC, HM),
    #             historical=mean(get(var)),
    #             .groups="drop"),
    
    #         summarise(group_by(
    #             filter(dataEX,
    #                    futur[1] <= date &
    #                    date <= futur[2]),
    #             code, Chain),
    #             futur=mean(get(var)),
    #             .groups="drop"),
    
    #         by=c("code", "Chain"))


    # dataEX$delta =
    #     (dataEX$futur - dataEX$historical) /
    #     dataEX$historical * 100


    # dataEX = summarize(group_by(dataEX,
    #                             code, GCM, RCM, BC),
    #                    meanDelta=mean(delta),
    #                    .groups="drop")

    # dataEX = summarize(group_by(dataEX,
    #                             code, GCM, RCM),
    #                    meanDelta=mean(meanDelta),
    #                    .groups="drop")

    # dataEX = summarize(group_by(dataEX,
    #                             code),
    #                    meanDelta=mean(meanDelta),
    #                    .groups="drop")




    if ("compute_delta" %in% analyse_data) {

        # stop()

        
        Projections = Projections[Projections$EXP != "SAFRAN",]
        DirPaths = Projections$path
        DirPaths = list.dirs(DirPaths,
                             recursive=FALSE)
        
        Paths = list.files(DirPaths,
                           pattern=gsub("[$]", "[.]fst$",
                                        variables_regexp),
                           full.names=TRUE,
                           recursive=FALSE)

        nPaths = length(Paths)
        
        for (i in 1:nPaths) {

            print(paste0(i, "/", nPaths, " -> ",
                         round(i/nPaths*100, 1), " %"))
            
            path = Paths[i]
            dataEX = ASHE::read_tibble(path)
            var = gsub("[.]fst", "", basename(path))
            
            
            for (j in 1:nFuturs) {
                futur = Futurs[[j]]
                name_futur = names(Futurs)[j]
                print(name_futur)
                
                deltaEX =
                    full_join(
                        summarise(group_by(
                            filter(dataEX,
                                   historical[1] <= date &
                                   date <= historical[2]),
                            code, GCM, EXP, RCM, BC, HM),
                            historical=mean(get(var), na.rm=TRUE),
                            .groups="drop"),
                        
                        summarise(group_by(
                            filter(dataEX,
                                   futur[1] <= date &
                                   date <= futur[2]),
                            code), ###### ?????????????????????
                            futur=mean(get(var), na.rm=TRUE),
                            .groups="drop"),
                        by=c("code"))

                deltaEX$delta =
                    (deltaEX$futur - deltaEX$historical) /
                    deltaEX$historical * 100

                output_dir = gsub("projection",
                                  paste0("projection_delta_",
                                         name_futur),
                                  dirname(path))
                if (!dir.exists(output_dir)) {
                    dir.create(output_dir, recursive=TRUE)
                }
                
                ASHE::write_tibble(deltaEX, output_dir,
                                   paste0(var, ".fst"))
            }
        }
    }
}






