# Camada Gold — Construção dos Data Products

> Conteúdo de apoio para a Secção "Transformação de Dados: Da Camada Silver à Gold" do relatório (`main.tex`).
>
> Cobre o que é construído na camada *Gold*, como é construído (fontes *Silver*, *joins*, regras de agregação) e o esquema completo das duas tabelas finais (`gold.network_quality_daily` e `gold.churn_risk_daily`).


- **Colunas gold em `snake_case`** (contratos v1.2.0 em `docs/Data_Products - Data_Contracts/v2/`), alinhadas com particionamento Iceberg no Trino.
- **Grão fixo** — `(id_antena_conectada, data_hora)` no Produto A; `(telefone, data_referencia)` no Produto B.
- **Substituição atómica** — `CREATE OR REPLACE TABLE` via `replace_iceberg_table`.
- **Partitioning Iceberg** — `day(data_hora)` (Produto A); `data_referencia` (Produto B).
- **SLOs e regras de qualidade** nos contratos (`quality.constraints`).

Cada produto está catalogado em `docs/Data_Products - Data_Contracts/` e tem um *task* Flyte dedicado (`build_gold_network_quality`, `build_gold_churn_risk`).

---

## Produto A — `iceberg.gold.network_quality_daily`

**Domínio:** *network_operations*
**Destinatários:** Engenharia de Rede e *Network Operations Center* (NOC).
**Grão:** 1 linha por `(id_antena_conectada, data_hora)` — torre × dia civil.
**Fontes Silver:** `network_logs`, `towers`, `call_tests`.

### Como é construído

1. **Deduplicação das torres por célula física.** CTE `towers_for_geo`.
2. **Atribuição log → torre mais próxima** (Haversine).
3. **Enriquecimento por dia** (`CAST(timestamp_log AS DATE)`).
4. **Matriz mestra torres × dia** com `LEFT JOIN` — antenas sem actividade com zeros.
5. **Cruzamento com testes de voz** (`tests_by_day`).
6. **Agregações com filtros 3GPP** (`potencia_rsrp`, `qualidade_rsrq`, `ruido_sinr`, `velocidade_downlink`).
7. **Zoneamento** → `zona_leiria` (quadrantes de Leiria).
8. **Tecnologia** → `tecnologia_rede`.
9. **Validação** — `_assert_antenna_columns_sane`.

### Esquema completo

| Coluna | Tipo | O que significa |
|---|---|---|
| `gold_row_id` | BIGINT | Chave surrogate técnica. |
| `data_hora` | TIMESTAMP(3) | Início do dia; partição `day(data_hora)`. |
| `zona_leiria` | VARCHAR | Quadrante geográfico. |
| `latitude_ocorrencia` | DOUBLE | Latitude da torre. |
| `longitude_ocorrencia` | DOUBLE | Longitude da torre. |
| `torre_latitude` | DOUBLE | Latitude (mapas). |
| `torre_longitude` | DOUBLE | Longitude (mapas). |
| `id_antena_conectada` | BIGINT | `cell` OpenCelliD. |
| `estado_antena` | BOOLEAN | Torre activa/down no snapshot. |
| `distancia_antena_m` | DOUBLE | Distância média log→torre (m). |
| `tecnologia_rede` | VARCHAR | LTE / GSM / UMTS / NR / CDMA / OTHER. |
| `potencia_rsrp` | DOUBLE | Média RSRP (dBm), filtro 3GPP. |
| `qualidade_rsrq` | DOUBLE | Média RSRQ (dB). |
| `ruido_sinr` | DOUBLE | Média SINR (dB). |
| `velocidade_downlink` | DOUBLE | Throughput médio (Mbps). |
| `telefones_sucesso` | BIGINT | Telefones com último teste OK no dia/torre. |
| `telefones_falha` | BIGINT | Telefones com último teste falhado. |
| `telefones_sem_teste` | BIGINT | Sem resultado de teste conhecido. |

### KPIs derivados

- **Drop rate proxy:** `telefones_falha / (telefones_sucesso + telefones_falha) × 100`.
- **Cobertura por quadrante:** torres com `estado_antena = FALSE` por `zona_leiria`.
- **Degradação:** `potencia_rsrp < −110` ou `ruido_sinr < 0`.

---

## Produto B — `iceberg.gold.churn_risk_daily`

**Domínio:** *customer_retention*
**Grão:** 1 linha por `(telefone, data_referencia)`.
**Fontes Silver:** `cdr_customers`, `call_tests`, `network_logs`.

### Como é construído

1. **Datas de referência** — união de dias com actividade em `call_tests` e `network_logs`.
2. **Carga** — `UNION ALL` por dia, um `CREATE OR REPLACE`.
3. **Base CDR** × dia.
4. **Joins** — drops/MOS e `afetado_tempestade`.
5. **`gold_row_id`** — `ROW_NUMBER() OVER (ORDER BY data_referencia, telefone)`.
6. **Validação** — `_assert_churn_columns_sane`.

### Esquema completo

| Coluna | Tipo | O que significa |
|---|---|---|
| `gold_row_id` | BIGINT | Chave surrogate. |
| `data_referencia` | DATE | Dia; partição Iceberg. |
| `telefone` | VARCHAR | Cliente (CDR). |
| `afetado_tempestade` | BOOLEAN | Logs na zona Este no dia. |
| `receita_em_risco` | DOUBLE | Facturação diária (CDR). |
| `tempo_subscrito` | INTEGER | `account_length`. |
| `total_chamadas_suporte` | INTEGER | `custserv_calls`. |
| `total_drops` | BIGINT | Testes falhados no dia. |
| `qualidade_audio_mos` | DOUBLE | MOS médio. |
| `desistencia` | BOOLEAN | Variável alvo churn. |

### KPIs derivados

- **Receita em risco:** soma `receita_em_risco` com `afetado_tempestade = TRUE`.
- **Frustração:** `total_drops / total_chamadas_suporte`.
- **Churn ↔ MOS:** segmentos de `qualidade_audio_mos`.

---

## Estratégia de carga e governança

- **Full-replace atómico:** `replace_iceberg_table` → `CREATE OR REPLACE TABLE … AS …`.
- **Contratos v1.2.0** (`v2/`): schema `snake_case`, `update_mode: create_or_replace`.
- **Vista executiva:** `iceberg.gold.vw_storm_impact_executive` (sem vista intermédia por produto).
