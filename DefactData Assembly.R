library(tidyverse)

#
# 2024 Data
#
source("../CommonFunctions/getSQL.r")

#
# Pulling data from the 2015-2019 DBase
#

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

DefectAssessment2019 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2019/DefectAssessment2019.sql"))

GBD2019 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2019/GBD2019.sql"))

DBI::dbDisconnect(con)

#
# Pulling from the 2020-2023 database
#

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPacker2023Repl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

DefectAssessment2023 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2023/DefectAssessment2023.sql"))

GBD2023 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2023/GBD2023.sql"))

DBI::dbDisconnect(con)

#
# pulling from the 2024-2025 database
#

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

DefectAssessment <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/DefectAssessment.sql"))

GBD2024 <- DBI::dbGetQuery(con, getSQL("../SQLQueryRepo/SQLfiles2024/GraderBatchTeIpu.sql"))

DBI::dbDisconnect(con)

#
# Aggregate the Seasons together
#

DAAgg <- DefectAssessment |>
  bind_rows(DefectAssessment2023) |>
  bind_rows(DefectAssessment2019) |>
  mutate(Defect = str_squish(Defect),
         Defect = str_to_lower(Defect),
         Defect = if_else(Defect %in% c("russet","russet cheek","russet stem"),
                          "russet",
                          Defect),
         Defect = if_else(Defect %in% c("grey mould (botrytis)","rot (botrytis)"),
                          "botrytis",
                          Defect),
         Defect = if_else(Defect %in% c("puncture","puncture (stem fresh)"),
                          "puncture",
                          Defect)) 
  
DefectClassification <- DAAgg |> 
  distinct(Defect) |>
  mutate(Category = case_when(Defect %in% c("alternaria","black rot",'blemish cosmetic',
                                            "blue mould (penicilin)","botrytis","bronze beetle damage",
                                            "bruise (fresh)","bruise (old)","calyx split","cracking",
                                            "cuts","dry botrytis","eye rot/canker rot","frost damage",
                                            "fumigation burn","glomerella (bitter rot)","lenticle breakdown",
                                            "lenticle burn","lenticle spot","major bruise (spoilt)","mouldy core",
                                            "n.alba","over maturity","phomopsis","phytophthora","pit","puncture",
                                            "rose weevil damage","stem end rot","stem split","white rot") ~ "indirect",
                              Defect %in% c("bird damage","black spot","blotch","elongated","export fruit","hail",
                                            "insect damage","leaves","low colour","misshapen","noctuid damage",
                                            "over size","parrot beak","pronounced russet","proximity burn",
                                            "proximity shrivel","russet","scarf skin","spray residue",
                                            "stem tear","sting","sunburn","surfaces deposits","tree damage",
                                            "under size","variety mark","wrong variety") ~ "independent",
                              Defect %in% c("scald","shrivel","stem end browning") ~ "direct",
                              TRUE ~ "unclassified")) 

  

GBD2019Mod <- GBD2019 |>
  mutate(StorageDays = as.numeric(PackDate - HarvestDate)) |>
  select(c(Season, GraderBatchID, StorageDays, HarvestDate, `Storage type`, MaturityCode))

GBDAgg <- GBD2024 |>
  bind_rows(GBD2023) |>
  filter(!PresizeInputFlag,
         BatchStatus == "Closed",
         `Packing site` == "Te Ipu Packhouse (RO)") |>
  mutate(StorageDays = as.numeric(PackDate - HarvestDate)) 
  
GBDAgg2 <- GBDAgg |>  
  select(c(Season, GraderBatchID, StorageDays, HarvestDate, `Storage type`, MaturityCode)) |>
  bind_rows(GBD2019Mod |> 
              filter(Season == 2019))

DefectsWithGB <- GBDAgg2 |>
  inner_join(DAAgg, by = c("Season", "GraderBatchID")) |>
  mutate(Storage = if_else(`Storage type` == "CA Smartfresh", "CA", "RA"),
         DefectProp = DefectQty/SampleQty) |>
  select(-c(`Storage type`))

da_summary <- DefectsWithGB |>
  filter(Season %in% c(2019:2024)) |>
  select(c(Season, GraderBatchID, StorageDays, HarvestDate, 
           Defect, DefectQty, SampleQty, Storage, MaturityCode)) |>
  group_by(Season, Defect, StorageDays, Storage) |>
  summarise(DefectQty = sum(DefectQty, na.rm=T),
            SampleQty = sum(SampleQty, na.rm=T),
            HarvestDate = min(HarvestDate, na.rm=T),
            MaturityCode = max(MaturityCode, na.rm=T),
            .groups = "drop") |>
  mutate(Proportion = DefectQty/(SampleQty+1),
         HarvestDay = yday(HarvestDate)) 

################################################################################
# Generate the loss curve table                                                #
################################################################################

packOut <- GBDAgg |>
  filter(Season %in% c(2019:2024)) |>
  summarise(InputKgs = sum(InputKgs, na.rm=T),
            RejectKgs = sum(RejectKgs, na.rm=T)) |>
  mutate(PackOut = 1-RejectKgs/InputKgs)

top_23 <- DefectsWithGB |>
  group_by(Defect) |>
  summarise(DefectQty = sum(DefectQty, na.rm=T),
            SampleQty = sum(SampleQty, na.rm=T),
            .groups = "drop") |>
  mutate(DefectProportion = (1-packOut[[1,3]])*DefectQty/SampleQty) |>
  dplyr::select(-c(DefectQty, SampleQty)) |>
  slice_max(DefectProportion, n=23) |>
  left_join(DefectClassification, by = "Defect") |>
  filter(!Category == "independent",
         !Defect %in% c("white rot", "botrytis", "stem end rot")) |>
  pull(Defect) |>
  fct_inorder()

glmf <- function(da_master, def) {
  seasons <- distinct(da_master, Season)
  da_master |>
    filter(Defect == {{def}}) %>%
    split(.$Season) |>
    map(~glm(DefectQty ~ StorageDays + HarvestDay + MaturityCode + StorageDays:MaturityCode, 
             offset=log(SampleQty+1), 
             family="quasipoisson", data=.)) |>
    map_dfr(~.$coefficients) |>
    bind_cols(seasons) |>
    mutate(Defect = {{def}})
}

glmf2 <- function(da_master, def) {
  seasons <- distinct(da_master, Season)
  da_master |>
    filter(Defect == {{def}}) %>%
    split(.$Season) |>
    map(~glm(DefectQty ~ StorageDays, offset=log(SampleQty+1), 
             family="quasipoisson", data=.)) |>
    map_dfr(~.$coefficients) |>
    bind_cols(seasons) |>
    mutate(Defect = {{def}})
}

SD <- function(Defect, corrMeanIntercept, corrMeanEstimate, StorageDays, ...) {
  tibble(StorageDays = seq(0,300,1)) |>
    mutate(fittedValue = exp(corrMeanIntercept + corrMeanEstimate*StorageDays),
           DefectName = !!enquo(Defect))
}

da_summaryRA <- da_summary |>
  filter(Storage == "RA")

meanIntRA <- top_23 |>
  map(~glmf2(da_summaryRA, .)) %>%
  map_dbl(~mean(.$`(Intercept)`)) |>
  as_tibble_col(column_name = "meanIntercept") 

meanEstRA <- top_23 |>
  map(~glmf(da_summaryRA, .)) %>%
  map_dbl(~mean(.$StorageDays)) |>
  as_tibble_col(column_name = "meanEstimate") |>
  bind_cols(meanIntRA) |>
  bind_cols(as_tibble_col(top_23, column_name="Defect")) |>
  mutate(corrMeanEstimate = if_else(meanEstimate < 0.0, 0.0, meanEstimate),
         corrMeanIntercept = case_when(meanEstimate < 0.0 ~ -100,
                                       TRUE ~ meanIntercept), 
         corrMeanIntercept = if_else(corrMeanIntercept > -3, -3, corrMeanIntercept)
  ) |>
  relocate(Defect, .before = meanEstimate) |>
  ungroup()

meanEstRA |>
  select(c(Defect, meanEstimate, meanIntercept, corrMeanEstimate)) |>
  mutate(meanEstimate = exp(meanEstimate), 
         meanIntercept = exp(meanIntercept),
         corrMeanEstimate = exp(corrMeanEstimate)) |>
  mutate(across(.cols = meanEstimate:corrMeanEstimate, ~ round(., 3))) 

firstDefect <- meanEstRA |>
  slice_head(n=1) |>
  dplyr::select(Defect) |>
  pull() |>
  as.character()

lastDefect <- meanEstRA |>
  slice_tail(n=1) |>
  dplyr::select(Defect) |>
  pull() |>
  as.character()

AggLossCurve <- meanEstRA |>
  pmap(SD) |>
  bind_rows() |>
  mutate(fittedValue = fittedValue*(1-packOut[[1,3]])) |>
  pivot_wider(names_from = DefectName, values_from = fittedValue) |>
  rowwise() |>
  mutate(aggLoss = sum(c_across({{firstDefect}}:{{lastDefect}})))  

corr <- AggLossCurve$aggLoss[[1]] # Aggregate loss at storage day 0

corrAggLossCurveRA <- AggLossCurve %>%
  mutate(corrAggLoss = aggLoss - corr)

################################################################################
# Consider the CA now                                                          #
################################################################################

glmfCA <- function(da_master, def) {
  seasons <- distinct(da_master, Season)
  da_master |>
    filter(Defect == {{def}}) %>%
    split(.$Season) |>
    map(~glm(DefectQty ~ StorageDays + HarvestDay, offset=log(SampleQty+1), 
             family="quasipoisson", data=.)) |>
    map_dfr(~.$coefficients) |>
    bind_cols(seasons) |>
    mutate(Defect = {{def}})
}

SDCA <- function(Defect, corrMeanIntercept, corrMeanEstimate, StorageDays, ...) {
  tibble(StorageDays = seq(0,300,1)) |>
    mutate(fittedValue = exp(corrMeanIntercept + corrMeanEstimate*StorageDays),
           DefectName = !!enquo(Defect))
}

da_summaryCA <- da_summary |>
  filter(Storage == "CA")

meanIntCA <- top_23 |>
  map(~glmf2(da_summaryCA, .)) %>%
  map_dbl(~mean(.$`(Intercept)`)) |>
  as_tibble_col(column_name = "meanIntercept") 

meanEstCA <- top_23 |>
  map(~glmfCA(da_summaryCA, .)) %>%
  map_dbl(~mean(.$StorageDays)) |>
  as_tibble_col(column_name = "meanEstimate") |>
  bind_cols(meanIntCA) |>
  bind_cols(as_tibble_col(top_23, column_name="Defect")) |>
  mutate(corrMeanEstimate = if_else(meanEstimate < 0.0, 0.0, meanEstimate),
         corrMeanIntercept = case_when(meanEstimate < 0.0 ~ -100,
                                       TRUE ~ meanIntercept), 
         corrMeanIntercept = if_else(corrMeanIntercept > -4, -4, corrMeanIntercept)
  ) |>
  relocate(Defect, .before = meanEstimate) |>
  ungroup()

meanEstCA |>
  select(c(Defect, meanEstimate, meanIntercept, corrMeanEstimate)) |>
  mutate(meanEstimate = exp(meanEstimate), 
         meanIntercept = exp(meanIntercept),
         corrMeanEstimate = exp(corrMeanEstimate)) |>
  mutate(across(.cols = meanEstimate:corrMeanEstimate, ~ round(., 5))) 

firstDefectCA <- meanEstCA |>
  slice_head(n=1) |>
  dplyr::select(Defect) |>
  pull() |>
  as.character()

lastDefectCA <- meanEstCA |>
  slice_tail(n=1) |>
  dplyr::select(Defect) |>
  pull() |>
  as.character()

AggLossCurveCA <- meanEstCA |>
  pmap(SDCA) |>
  bind_rows() |>
  mutate(fittedValue = fittedValue*(1-packOut[[1,3]])) |>
  pivot_wider(names_from = DefectName, values_from = fittedValue) |>
  rowwise() |>
  mutate(aggLoss = sum(c_across({{firstDefectCA}}:{{lastDefectCA}})))

corrCA <- AggLossCurveCA$aggLoss[[1]] # Aggregate loss at storage day 0

corrAggLossCurveCA <- AggLossCurveCA |>
  mutate(corrAggLoss = aggLoss - corrCA)

corrAggLossCurveCA |>
  mutate(StorageType = "CA") |>
  bind_rows(corrAggLossCurveRA |> mutate(StorageType = "RA")) |>
  ggplot(aes(x=StorageDays, y=corrAggLoss, colour=StorageType)) +
  geom_line(linewidth = 1) +
  scale_y_continuous("Percentage loss due to storage", 
                     breaks = scales::breaks_extended(n=9),
                     labels = scales::label_percent(accuracy = 0.1)) +
  labs(x="storage days") +
  scale_colour_manual(values=c("#a9342c", "#48762e", "#526280", "#aec9e3", "#edb7a7")) +
  ggthemes::theme_economist() + 
  theme(axis.title.x = element_text(margin = margin(t = 10), size = 9),
        axis.title.y = element_text(margin = margin(r = 10), size = 9),
        axis.text.y = element_text(size = 7, hjust=1),
        axis.text.x = element_text(size = 7, angle = 0, hjust = 0.5, vjust = 1),
        plot.background = element_rect(fill = "#F7F1DF", colour = "#F7F1DF"),
        legend.text = element_text(size = 8),
        legend.title = element_text(size = 8),
        strip.text = element_text(margin = margin(b=2, t=2), size = 8),
        strip.background = element_rect(fill="#fbf4af"),
        panel.grid.major.y = element_line(linewidth = 0.5),
        legend.position = "top")

ggsave("LossCurev2024.png", width = 10, height = 7)

################################################################################
# Generating the loss curve data                                               #
################################################################################

FinalLossCures2024 <- corrAggLossCurveRA |>
  select(c(StorageDays,corrAggLoss)) |>
  rename(PercentLossRA = corrAggLoss) |>
  bind_cols(corrAggLossCurveCA |> select(corrAggLoss)) |>
  rename(PercentLossCA = corrAggLoss) 

write_csv(FinalLossCures2024, "FinalLossCurve2024.csv")
         