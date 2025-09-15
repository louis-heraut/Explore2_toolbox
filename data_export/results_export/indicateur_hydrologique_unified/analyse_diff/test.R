path_nc =
# "delta-QA_yr_2021-2050_TIMEavg_ANOMrd-1976-2005_GEOstation_FR-METRO_EXPLORE2-2024_ENSavg_historical-rcp26_ENSavg_ENSavg_ENSavg.nc"
"delta-QA_yr_2070-2099_TIMEavg_ANOMrd-1976-2005_GEOstation_FR-METRO_EXPLORE2-2024_ENSavg_historical-rcp85_ENSavg_ENSavg_ENSavg.nc"

NC = ncdf4::nc_open(path_nc)

Code = ncdf4::ncvar_get(NC, "code")
QA = ncdf4::ncvar_get(NC, "delta-QA")
L93_X = ncdf4::ncvar_get(NC, "L93_X")
L93_Y = ncdf4::ncvar_get(NC, "L93_Y")

data_nc = dplyr::tibble(code=Code,
                        L93_X=L93_X,
                        L93_Y=L93_Y,
                        QA=QA)

# data = ASHE::read_tibble("data_rcp26_H1.csv")
data = ASHE::read_tibble("data_rcp85_H3.csv")
data_nc = dplyr::filter(data_nc, code %in% data$code)

library(dataSHEEP)
prob = 0.01
Palette = get_IPCC_Palette("hydro_10")
res = compute_colorBin(min=quantile(data_nc$QA, prob),
                       max=quantile(data_nc$QA, 1-prob),
                       center=0,
                       colorStep=10)
data_nc$color = get_colors(data_nc$QA,
                           upBin=res$upBin,
                           lowBin=res$lowBin,
                           Palette=Palette)

library(ggplot2)
plot = ggplot() + theme_minimal() + coord_fixed() + 
    # geom_point(data=data,
               # aes(x=L93_X,
                   # y=L93_Y,
                   # color=value))
    annotate("point",
             x=data_nc$L93_X, y=data_nc$L93_Y,
             size=3,
             color=data_nc$color)
ggsave(plot=plot, filename="map.pdf", units="cm", width=20, height=20)

ncdf4::nc_close(NC)


plot = ggplot() +
    theme_minimal() +
    geom_point(data=NULL,
               aes(x=1:nrow(data_nc), y=abs(data_nc$QA - data$QA))) +
    xlab("index des codes") +
    ylab("différence absolue entre MEANDRE et les NetCDF (deltaQA en %)")
plot
ggsave(plot=plot, filename="diff.pdf", units="cm", width=15, height=15)


sum(round(data$QA, 0) == round(data_nc$QA, 0))
