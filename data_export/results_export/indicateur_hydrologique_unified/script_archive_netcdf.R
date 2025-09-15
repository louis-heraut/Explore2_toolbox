
archive_diskdir = "/media/lheraut/Explore2"

archive_dir =
    file.path("hydrological-projections_indicateurs-TRACC",
              "hydrological-projections_yearly-variables_by-chain_filtered-netcdf")

archive_dirpath = file.path(archive_diskdir,
                            archive_dir)

From = list.files("NetCDF", pattern=".nc", full.names=TRUE,
                   recursive=TRUE)

To = file.path(archive_dirpath,
               gsub("NetCDF/", "", From))

Dirs = unique(dirname(To))
for (dir in Dirs) {
    if (!dir.exists(dir)) {
        dir.create(dir)
    }
}
    
file.copy(From, To)
