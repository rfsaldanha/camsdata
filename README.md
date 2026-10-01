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


## Previsões para a saúde indígena

Depois da publicação municipal, a rotina produz também
`cams_forecast_sesai.duckdb` no mesmo diretório. Esse banco é independente de
`cams_forecast.duckdb`: as 12 tabelas municipais, seus esquemas e os demais
arquivos consumidos pelo `polbr` permanecem inalterados. A integração dos novos
dados na interface do `polbr_sesai` é uma etapa posterior.

As fontes territoriais estão versionadas em [`data/sesai/`](data/sesai/README.md),
com procedência e hashes em `SHA256SUMS`. Não é necessário instalar o
`polbr_sesai` no servidor. O processamento usa 36 feições de DSEIs, incluindo as
duas áreas “em estudo”, e 397 feições de pólos-base da fonte atual. Exclui 55
“TERRITÓRIOS DE CONEXÃO” e um registro de geometria vazia, com auditoria no banco.
Não dissolve feições com códigos repetidos, nem corrige códigos zero. Portanto,
as contagens são de **feições territoriais**, não de códigos administrativos
distintos. As geometrias são reparadas em EPSG:5880, transformadas para EPSG:4326
e usadas sem simplificação. Cada DSEI é calculado diretamente em seu polígono.

Para processar **somente SESAI**, a partir dos rasters já publicados:

```sh
CAMS_FORECAST_DATA_DIR=/caminho/forecast_data Rscript cams_forecast_sesai.R
```

Esse comando não baixa dados, não envia notificações e não altera produtos
municipais. Requer os 12 rasters processados e `.cams_generation`; as datas vêm
do ciclo desse marcador, e não do relógio da execução. Aceita os mesmos padrões
de diretório da rotina principal. `CAMS_FORCE_UPDATE=true` força a reconstrução
do banco SESAI; sem essa opção, uma saída completa e atual é reutilizada.

Dependências adicionais: `digest` e `blob`. A etapa isolada usa também `sf`,
`terra`, `exactextractr`, `DBI`, `duckdb`, `lubridate` e `tibble`. Não exige os
pacotes de download/notificação da rotina completa. Para instalar o necessário:

```r
install.packages(c("sf", "terra", "exactextractr", "DBI", "duckdb",
                  "lubridate", "tibble", "digest", "blob"))
```

### Contrato do banco SESAI

Há 24 tabelas de séries: `<variável>_dsei_forecast` e
`<variável>_polo_forecast`. Cada tabela contém `territory_id VARCHAR`,
`date TIMESTAMP` e `value DOUBLE`, com índice único por território/data.

| Variável | Unidade | Intervalo |
|---|---|---|
| `iqar` | Indicador instantâneo de qualidade do ar, como no produto municipal | 3 h |
| `pm25`, `pm10` | µg/m³ | 1 h |
| `o3`, `no2`, `so2` | µg/m³ | 3 h |
| `co` | ppm | 3 h |
| `temp` | °C | 1 h |
| `uv` | Índice UV | 1 h |
| `wind_speed` | km/h | 1 h |
| `aerosol` | Unidade original do raster CAMS, sem conversão | 1 h |
| `prec` | mm, mantendo a acumulação do raster | 1 h |

São 121 instantes nas séries horárias e 41 nas séries de três horas, incluindo
o início e o horizonte de 120 horas. `value` usa a mesma média por fração de
cobertura de célula de `exact_extract(..., "mean")`, conversões e arredondamento
municipais: duas casas decimais. Valores sem cobertura/dados válidos são `NULL`.
O IQAr é a média do raster do indicador, como na série municipal.

Assim como no banco municipal, `date` guarda os instantes UTC em `TIMESTAMP`
sem informação de fuso no tipo SQL. Para apresentação no Brasil, converter
explicitamente para `America/Sao_Paulo` no consumidor, sem deslocar os instantes.

Os cadastros `dsei_territories` e `polo_territories` contêm todos os atributos
originais e as colunas adicionais:

- `territory_id`: `<tipo>:<sha256_da_fonte>:<linha_original>`, identificador
  técnico **não oficial**, atribuído antes das exclusões;
- `source_file`, `source_row` (base 1) e `source_sha256`: rastreabilidade;
- `geometry_wkb`: geometria processada como WKB em coluna BLOB;
- `geometry_epsg`: `4326`.

Os identificadores são estáveis para a mesma versão da fonte. A alteração de
qualquer componente do shapefile muda o hash combinado e os identificadores
daquela camada. Não usar `cod_dsei` ou `cod_polo` como chave única de séries.

`sesai_metadata` registra ciclo, versão do processamento, assinatura das
entradas e criação em UTC. `sesai_inputs` registra arquivos, SHA-256, tamanho e
mtime; `sesai_sources` contém versões e contagens por camada;
`sesai_exclusions` registra cada feição excluída e seu motivo.

Exemplo de consulta, usando o identificador obtido do cadastro:

```r
con <- DBI::dbConnect(duckdb::duckdb(),
                     "/caminho/forecast_data/cams_forecast_sesai.duckdb",
                     read_only = TRUE)
polos <- DBI::dbReadTable(con, "polo_territories")
serie <- DBI::dbGetQuery(con,
  "SELECT date, value FROM pm25_polo_forecast WHERE territory_id = ? ORDER BY date",
  params = list(polos$territory_id[[1]]))
serie$date <- lubridate::with_tz(serie$date, "America/Sao_Paulo")
geometrias <- sf::st_as_sfc(
  structure(as.list(polos$geometry_wkb), class = "WKB"), crs = 4326)
DBI::dbDisconnect(con, shutdown = TRUE)
```

### Publicação e recuperação

A etapa SESAI escreve em um arquivo temporário próprio no diretório de destino,
valida as tabelas e reabre o banco fechado antes da substituição por renomeação
no mesmo sistema de arquivos. Mudanças nas entradas durante o processamento
cancelam a publicação. O banco anterior é mantido se ocorrer uma falha; a
rotina termina com erro, informando que a publicação municipal foi preservada.
O ciclo em `sesai_metadata` permite identificar uma saída SESAI mais antiga.

Quando o ciclo municipal já está publicado, a rotina verifica a completude e a
versão SESAI e reconstrói o banco se necessário, sem novo download CAMS. A etapa
SESAI não cria `territories.rds` ou `places.rds` e não modifica `.cams_generation`.

O diretório `.cams_forecast_sesai.lock` impede dois escritores SESAI simultâneos.
Após interrupção abrupta do processo, removê-lo apenas depois de confirmar que
não há outro escritor em execução. Os leitores devem abrir o banco como
somente leitura e reabrir a conexão para consumir uma nova publicação.

### Testes

```sh
Rscript --vanilla tests/run.R
```

Os testes requerem `testthat` e `fs`, além das dependências de processamento.
Usam cópias temporárias dos dados reais de `forecast_data/`; outro conjunto
completo pode ser indicado por `CAMS_TEST_DATA_DIR`. Quando os rasters locais
não estão disponíveis, os testes de integração são marcados como ignorados.
Não há downloads, notificações ou substituição dos dados publicados.

A regressão municipal usa uma referência congelada anterior à extensão e
compara integralmente esquemas e valores das 12 tabelas, além das consultas
usadas por `polbr/main` e `polbr/dev`. Os testes SESAI cobrem as 24 séries,
geometrias, exclusões, ausência de dados, reconstrução e preservação da última
saída válida em falhas.
