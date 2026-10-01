# Fontes territoriais SESAI

Cópia integral, sem alterações, dos componentes dos shapefiles utilizados pelo
projeto vizinho `../polbr_sesai`, incorporada em 30/09/2026:

- `../polbr_sesai/shapefiles/36_DSEI/36DSEI.*` → `36_DSEI/36DSEI.*`;
- `../polbr_sesai/shapefiles/POLOS_2026/POLOS_BASE_AGOSTO_2025.*` →
  `POLOS_2026/POLOS_BASE_AGOSTO_2025.*`.

Esses caminhos documentam a procedência local; não há dependência do aplicativo
vizinho durante a execução. As datas nos nomes dos arquivos são mantidas como
recebidas e não constituem uma certificação da vigência administrativa da base.

`SHA256SUMS` registra o SHA-256 de cada componente original. Para conferir:

```sh
cd data/sesai
sha256sum --check SHA256SUMS
```

As fontes têm CRS EPSG:4674. Há 36 feições de DSEIs e 453 feições de pólos-base.
O processamento mantém as versões de DSEIs identificadas como “em estudo”,
exclui 55 feições chamadas “TERRITÓRIO DE CONEXÃO” e um registro vazio de pólo,
resultando em 36 e 397 feições, respectivamente. Códigos repetidos, códigos zero,
nomes e atributos originais são preservados; cada feição recebe sua própria série.

Para atualizar a fonte, substituir todos os componentes correspondentes e
atualizar `SHA256SUMS` e esta documentação. A rotina calcula o hash combinado de
cada fonte a partir dos nomes e hashes dos cinco componentes, na ordem `.shp`,
`.dbf`, `.shx`, `.prj`, `.cpg`. Uma mudança em qualquer componente cria uma nova
versão da fonte e novos identificadores técnicos para suas feições.
