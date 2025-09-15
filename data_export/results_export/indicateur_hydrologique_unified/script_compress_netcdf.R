

dir =
    "NetCDF_TRACC_OK"

Dirs = list.dirs(dir, full.names=TRUE, recursive=FALSE)
outDirs = file.path(paste0(Dirs, ".tar.gz"))
nDirs = length(Dirs)
    
for (i in 1:nDirs) {
    tar(tarfile=outDirs[i],
        files=Dirs[i],
        compression="gzip")
}
