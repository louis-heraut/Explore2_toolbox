

Paths = list.files("NetCDF", pattern=".nc", full.names=TRUE)


HM_ALL = gsub(".nc", "", sapply(strsplit(Paths, "_"), "[", 12))
HM = unique(HM_ALL)

for (hm in HM) {
    outdir = file.path("NetCDF", hm)
    if (!dir.exists(outdir)) {
        dir.create(outdir)
    }

    From = Paths[grepl(hm, Paths)]
    To = file.path(outdir, basename(From))
    file.rename(From, To)
}


