# Packages
library(dplyr)
library(tidyr)
library(readr)
library(arrow)
library(lubridate)
library(ggplot2)

# Datasets
pm25_mean <- open_dataset(sources = "pm25_mean_mean.parquet") |>
  filter(year(date) >= 2018) |>
  mutate(name = "mean") |>
  collect()

pm25_max <- open_dataset(sources = "pm25_max_mean.parquet") |>
  filter(year(date) >= 2018) |>
  mutate(name = "max") |>
  collect()

pm25_min <- open_dataset(sources = "pm25_min_mean.parquet") |>
  filter(year(date) >= 2018) |>
  mutate(name = "min") |>
  collect()

pm25 <- bind_rows(pm25_mean, pm25_max, pm25_min)
rm(pm25_mean, pm25_max, pm25_min)

# Monthly data
pm25_monthly <- pm25 |>
  mutate(
    date = floor_date(date, "month")
  ) |>
  summarise(
    value = mean(value, na.rm = TRUE),
    .by = c(code_muni, date, name)
  ) |>
  pivot_wider() |>
  select(-min, -max) |>
  mutate(code_muni = as.numeric(substr(code_muni, 0, 6))) |>
  rename(cod6 = code_muni, ano_mes = date, pm25 = mean)

write_csv2(x = pm25_monthly, file = "dados_pm25.csv")

# Daily data
pm25_daily <- pivot_wider(pm25) |>
  mutate(code_muni = as.numeric(substr(code_muni, 0, 6))) |>
  rename(cod6 = code_muni, ano_mes = date, med = mean) |>
  relocate(min, .before = med)

write_csv2(x = pm25_daily, file = "dados_dia.csv")
