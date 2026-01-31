## Dicionário de Dados — Camada Gold (Tabela Fato)

### Tabela: `fato_performance_diaria`

**Descrição:** Consolida indicadores diários de desempenho por linha, combinando dados operacionais (MCO) e métricas de velocidade/distância do GPS.

**Grão (granularidade):** 1 linha por dia por linha de ônibus.

**Origem:**
- Silver: `mco`, `linhas`, `gps`

**Regras de negócio principais:**
- GPS filtrado para velocidade entre 0 e 120 km/h.
- Datas do GPS ajustadas em +5 anos e +47 dias para alinhamento de distribuição semanal.
- Valores nulos de `vel_media_gps` e `distancia_total_gps_metros` preenchidos com 0.

#### Campos

| Campo | Tipo | Descrição | Origem/Regra |
|---|---|---|---|
| `data_referencia` | date | Data de referência da viagem (dia). | `mco.data_viagem` |
| `numero_linha` | string/int | Identificador da linha no MCO. | `mco.numero_linha` |
| `cod_linha_publico` | int | Código público da linha. | `linhas.cod_linha_publico` |
| `nome_linha` | string | Nome da linha. | `linhas.nome_bruto` |
| `nome_consorcio` | string | Consórcio da linha. | `mco.nome_consorcio` |
| `qtd_viagens_realizadas` | long | Quantidade de viagens realizadas no dia. | `count(*)` do MCO |
| `total_passageiros` | long | Total de passageiros no dia. | `sum(total_usuarios)` do MCO |
| `qtd_falhas_mecanicas` | long | Total de falhas mecânicas no dia. | `sum(teve_falha_mecanica)` |
| `duracao_media_viagem_min` | double | Duração média das viagens (min). | `avg(duracao_viagem_minutos)` |
| `vel_media_gps` | double | Velocidade média do GPS no dia (km/h). | `avg(velocidade_kmh)` (GPS filtrado) |
| `vel_maxima_gps` | double | Velocidade máxima do GPS no dia (km/h). | `max(velocidade_kmh)` |
| `distancia_total_gps_metros` | double | Distância total percorrida (m). | `sum(distancia_percorrida)` |
| `bairro_origem` | string | Bairro de origem da linha. | `linhas.bairro_origem` |
| `bairro_destino` | string | Bairro de destino da linha. | `linhas.bairro_destino` |

#### Chaves e relacionamentos

- **Chave natural:** (`data_referencia`, `numero_linha`)
- **Join GPS x MCO:**
	- `mco.data_viagem` = `gps.data_particao` (após ajuste de data)
	- `mco.join_cod_publico` = `gps.join_cod_publico`

