# camsdata

Rotina de atualização dos dados de previsão consumidos pelo AlertAr Saúde.

## Execução

Por padrão, `cams_forecast.R` grava em `forecast_data/` dentro deste repositório.
No servidor histórico, o caminho `/dados/home/rfsaldanha/camsdata/forecast_data`
continua sendo reconhecido automaticamente. Para definir outro destino:

```sh
CAMS_FORECAST_DATA_DIR=/caminho/forecast_data Rscript cams_forecast.R
```

O diretório precisa conter `mun_epsg4326.rds`. Os nomes dos rasters, arquivos de
vento, banco DuckDB e `bdq_focos.rds` permanecem compatíveis com as versões
`main` e `dev` do app.

`bdq_focos.rds` contém os focos AQUA dos três dias processados, com as colunas
`id`, `lat`, `lon` e `data_hora_gmt`. Os eventos são deduplicados por `id` antes
da publicação.
