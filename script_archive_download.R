

URL_DRIAS_file = "URL_DRIAS.txt"
output_DRIAS_dir = "output_DRIAS"

options(timeout=300)
URLs = readLines(file.path(URL_DRIAS_file))
output_dir = file.path(output_DRIAS_dir)
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
