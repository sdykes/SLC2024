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

regRA <- da_summary |>
  filter(Storage == "RA",
         Season %in% c(2019:2024)) |>
  group_by(Season) |>
  nest() |>
  mutate(model = map(data, ~glm(DefectQty ~ StorageDays, 
                                offset=log(SampleQty+1), 
                                family="quasipoisson", 
                                data=.) |> fitted.values())) |>
  unnest(c(data, model)) |>
  mutate(FittedValues = model/(SampleQty + 1)) 

regCA <- da_summary |>
  filter(Storage == "CA") |>
  group_by(Season) |>
  nest() |>
  mutate(model = map(data, ~glm(DefectQty ~ StorageDays, 
                                offset=log(SampleQty+1),
                                family="quasipoisson", 
                                data=.) |> fitted.values())) |>
  unnest(c(data, model)) |>
  mutate(FittedValues = model/(SampleQty + 1)) 

regRA |>
  bind_rows(regCA) |>
  ggplot(aes(StorageDays, Proportion, colour = Storage)) +
  geom_jitter(size = 1, alpha = 0.3) +
  geom_line(aes(y = FittedValues), size=0.5) +
  facet_wrap(vars(Season), ncol = 3) +
  ggthemes::theme_economist() +
  theme(axis.title.x = element_text(margin = margin(t = 10)),
        axis.title.y = element_text(margin = margin(r = 10)),
        plot.title = element_text(vjust = 2)) +
  scale_y_continuous("percentage of defects / %", labels = scales::label_percent(accuracy = 1.0)) +
  labs(x = "storage days") +
  scale_colour_manual(values=c("#a9342c", "#48762e", "#526280", "#aec9e3", "#edb7a7")) +
  ggthemes::theme_economist() + 
  theme(axis.title.x = element_text(margin = margin(t = 10), size = 9),
        axis.title.y = element_text(margin = margin(r = 10), size = 9),
        axis.text.y = element_text(size = 7, hjust=1),
        axis.text.x = element_text(size = 7, angle = 0, hjust = 0.5, vjust = 1),
        plot.background = element_rect(fill = "#F7F1DF", colour = "#F7F1DF"),
        legend.text = element_text(size = 7),
        legend.title = element_text(size = 7),
        strip.text = element_text(margin = margin(b=2, t=2), size = 7),
        strip.background = element_rect(fill="#fbf4af"),
        panel.grid.major.y = element_line(linewidth = 0.5),
        legend.position = "top")