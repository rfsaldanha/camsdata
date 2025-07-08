# Packages
library(dplyr)
library(arrow)
library(lubridate)
library(ggplot2)

res <- open_dataset(sources = "pm25_mean_mean.parquet")

res |>
  filter(code_muni == 5103403) |>
  arrange(date) |>
  collect() |>
  ggplot(aes(x = date, y = value)) +
  geom_line() +
  labs(title = "PM2.5 - Médias diárias", subtitle = "Cuiabá, MT")

res |>
  filter(code_muni == 3304557) |>
  filter(year(date) == 2024) |>
  mutate(date = floor_date(date, "month")) |>
  arrange(date) |>
  collect() |>
  summarise(value = mean(value, na.rm = TRUE), .by = date) |>
  ggplot(aes(x = date, y = value)) +
  geom_line()
