

dir =
    # "NetCDF_TRACC_OK"
    "NetCDF_TRACC_multimodel_OK"

Paths = list.files(dir, pattern=".nc", full.names=TRUE, recursive=TRUE)


HM_ALL = gsub(".nc", "", sapply(strsplit(basename(Paths), "_"), "[", 12))
HM = unique(HM_ALL)

RWL_ALL = gsub(".nc", "", sapply(strsplit(basename(Paths), "_"), "[", 3))
RWL = unique(RWL_ALL)


for (rwl in RWL) {
    for (hm in HM) {
        if (hm == "ENSavg") {
            outdir = file.path(dir, paste0(rwl, "_multimodel"))
        } else {
            outdir = file.path(dir, paste0(rwl, "_", hm))
        }
        if (!dir.exists(outdir)) {
            dir.create(outdir)
        }
        From = Paths[grepl(rwl, basename(Paths)) &
                     grepl(hm, basename(Paths))]
        To = file.path(outdir, basename(From))
        file.rename(From, To)
    }
}


### return
# From = Paths
# To = file.path(dir, basename(Paths))
# file.rename(From, To)
