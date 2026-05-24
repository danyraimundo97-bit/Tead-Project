# Camada Gold — Construção dos Data Products

> Conteúdo de apoio para a Secção "Transformação de Dados: Da Camada Silver à Gold" do relatório (`main.tex`).
>
> Cobre o que é construído na camada *Gold*, como é construído (fontes *Silver*, *joins*, regras de agregação) e o esquema completo das duas tabelas finais (`gold.network_quality_daily` e `gold.churn_risk_daily`).


- **Nomes de colunas em "linguagem de negócio"** (`Telefone`, `Receita_Em_Risco`, `Potencia_RSRP`, `Estado_Antena`), em vez de identificadores técnicos da *Silver*.
- **Grão fixo** — cada tabela tem uma chave de negócio explícita (`(ID_Antena_Conectada, Data_Hora)` no Produto A; `(Telefone, Data_Referencia)` no Produto B).
- **Carga ACID em substituição total** — cada execução `replace_table_transaction` substitui o conteúdo anterior; falhas disparam *rollback* e o *snapshot* anterior permanece.
- **Partitioning Iceberg** por dia (`Data_Hora` ou `Data_Referencia`), para acelerar filtros temporais.
- **SLOs e regras de qualidade** definidos em contrato — v1.0.0: frescura P95 ≤ 4 h (design batch noturno); v1.1.0 (`v2/`): snapshot após run manual; intervalos plausíveis para métricas como RSRP entre −140 e −44 dBm.

Cada produto está catalogado em `docs/Data_Products - Data_Contracts/` e tem um *task* Flyte dedicado (`build_gold_network_quality`, `build_gold_churn_risk`).

---

## Produto A — `iceberg.gold.network_quality_daily`

**Domínio:** *network_operations*
**Destinatários:** Engenharia de Rede e *Network Operations Center* (NOC).
**Grão:** 1 linha por `(ID_Antena_Conectada, Data_Hora)` — torre × dia civil.
**Fontes Silver:** `network_logs`, `towers`, `call_tests`.

### Como é construído

1. **Deduplicação das torres por célula física.** Como a *Silver* guarda vários *snapshots* da mesma torre, mantém-se apenas o *snapshot* mais recente por `(mcc, net, area, cell, unit)` (CTE `towers_for_geo`).
2. **Atribuição log → torre mais próxima.** Para cada registo de `network_logs` com coordenadas válidas, calcula-se a distância Haversine a todas as torres deduplicadas e fica-se com a torre de menor distância (CTE `dist_ranked` + `closest_tower`).
3. **Enriquecimento por dia.** Cada log fica anotado com a torre física, a distância à torre e o dia (`CAST(timestamp_log AS DATE)`).
4. **Matriz mestra: torres × dia.** A base de agregação é `daily_towers` (a *Silver* `towers` já vem com um *snapshot* por dia), garantida com `LEFT JOIN` ao log enriquecido — torres sem actividade ficam na tabela com contagens zero ("antenas vazias").
5. **Cruzamento com testes de voz.** `tests_by_day` colapsa `call_tests` por (telefone, dia) usando `max_by(result, date_of_test)` (último resultado conhecido); `LEFT JOIN` por telefone e dia.
6. **Agregações com filtros 3GPP.** As médias usam apenas leituras dentro de intervalos plausíveis para evitar contaminação por *outliers*:
   - `Potencia_RSRP`: `BETWEEN -140 AND -44`
   - `Qualidade_RSRQ`: `BETWEEN -50 AND 30`
   - `Ruido_SINR`: `BETWEEN -30 AND 80`
   - `Velocidade_Downlink`: `BETWEEN 0 AND 5000`
7. **Zoneamento de Leiria.** `Zona_Leiria` é derivado das coordenadas da torre em quatro quadrantes (`Norte-Leste`, `Norte-Oeste`, `Sul-Leste`, `Sul-Oeste`), usando os cortes `lat > 39.75` e `lon > −8.80`.
8. **Tecnologia da rede.** `Tecnologia_Rede` é derivado das colunas `radio_ohe_*` da torre, com prioridade LTE → GSM → UMTS → NR → CDMA → OTHER.
9. **Validação pós-carga.** `_assert_antenna_columns_sane` falha o pipeline se `ID_Antena_Conectada`, `Estado_Antena` ou `Distancia_Antena_m` ficarem `NULL` em **todas** as linhas.

### Esquema completo

| Coluna | Tipo | O que significa |
|---|---|---|
| `gold_row_id` | BIGINT | Chave surrogate técnica (`ROW_NUMBER` na carga). |
| `Data_Hora` | TIMESTAMP(3) | Início do dia de referência. |
| `Zona_Leiria` | VARCHAR | Quadrante geográfico (`Norte-Leste`, `Norte-Oeste`, `Sul-Leste`, `Sul-Oeste`). |
| `Latitude_Ocorrencia` | DOUBLE | Latitude da torre (referência geográfica do registo). |
| `Longitude_Ocorrencia` | DOUBLE | Longitude da torre. |
| `Torre_Latitude` | DOUBLE | Latitude da torre (duplicado por contrato para uso em mapas). |
| `Torre_Longitude` | DOUBLE | Longitude da torre. |
| `ID_Antena_Conectada` | BIGINT | `cell` da torre (OpenCelliD); chave de negócio. |
| `Estado_Antena` | BOOLEAN | Estado da torre no *snapshot*: `TRUE` = activa; `FALSE` = avariada (simulação tempestade Leste). |
| `Distancia_Antena_m` | DOUBLE | Distância Haversine média (log → torre) no dia. |
| `Tecnologia_Rede` | VARCHAR | LTE / GSM / UMTS / NR / CDMA / OTHER. |
| `Potencia_RSRP` | DOUBLE | Média RSRP (dBm) com filtro 3GPP. |
| `Qualidade_RSRQ` | DOUBLE | Média RSRQ (dB) com filtro de plausibilidade. |
| `Ruido_SINR` | DOUBLE | Média SINR (dB) com filtro de plausibilidade. |
| `Velocidade_Downlink` | DOUBLE | Throughput médio de descarga (Mbps). |
| `Telefones_Sucesso` | BIGINT | Telefones distintos com último teste bem-sucedido no dia/torre. |
| `Telefones_Falha` | BIGINT | Telefones distintos com último teste falhado/`DROP`. |
| `Telefones_Sem_Teste` | BIGINT | Telefones distintos sem resultado conhecido (caso `tb.last_result IS NULL`). |

### KPIs derivados

- **Drop rate proxy:** `Telefones_Falha / (Telefones_Sucesso + Telefones_Falha) × 100` (quando há testes).
- **Estado de cobertura por quadrante:** contagem de torres com `Estado_Antena = FALSE` por `Zona_Leiria`.
- **Degradação de sinal:** torres com `Potencia_RSRP < −110` ou `Ruido_SINR < 0`.

---

## Produto B — `iceberg.gold.churn_risk_daily`

**Domínio:** *customer_retention*
**Destinatários:** Direção de Marketing, Retenção de Clientes e Faturação.
**Grão:** 1 linha por `(Telefone, Data_Referencia)`.
**Fontes Silver:** `cdr_customers`, `call_tests`, `network_logs`.

### Como é construído

1. **Determinação das datas de referência.** Recolhe-se a união dos dias com actividade em `call_tests` (`date_of_test`) e em `network_logs` (`timestamp_log`) — só estes dias entram na tabela.
2. **Carga *chunked* dia a dia.** Por cada `Data_Referencia` faz-se um `INSERT` separado dentro da mesma transacção (`replace_table_transaction_multi_insert`), para limitar memória no Trino.
3. **Base: `silver.cdr_customers` × dia.** Para cada cliente do CDR é gerada uma linha por dia (`CROSS JOIN` implícito).
4. ***Joins* dos sinais técnicos:**
   - `LEFT JOIN` a um agregado diário de `call_tests`: contagem de `result = FALSE` (`Total_Drops`) e média MOS (`Qualidade_Audio_MOS`).
   - `LEFT JOIN` a um agregado diário de `network_logs`: se o cliente registou logs com `longitude > −8.80` nesse dia, é marcado `Afetado_Tempestade = TRUE`.
5. ***Surrogate key* monotónica.** `gold_row_id` parte de `MAX(gold_row_id)` da tabela e soma `ROW_NUMBER()` — mantém-se monotónico ao longo de toda a carga, mesmo entre *chunks* de dias diferentes.
6. **Validação pós-carga.** `_assert_churn_columns_sane` falha se `Telefone`, `Data_Referencia` ou `Desistencia` ficarem `NULL` em todas as linhas.

### Esquema completo

| Coluna | Tipo | O que significa |
|---|---|---|
| `gold_row_id` | BIGINT | Chave surrogate técnica (monotónica). |
| `Data_Referencia` | DATE | Dia de calendário; coluna de partição Iceberg. |
| `Telefone` | VARCHAR | Identificador do cliente (FK lógica para `silver.cdr_customers`). |
| `Afetado_Tempestade` | BOOLEAN | `TRUE` se o cliente registou logs na zona Este de Leiria nesse dia. |
| `Receita_Em_Risco` | DOUBLE | `day_charge + eve_charge + night_charge + intl_charge` — facturação total do dia. |
| `Tempo_Subscrito` | INTEGER | `account_length` — antiguidade do cliente (dias). |
| `Total_Chamadas_Suporte` | INTEGER | `custserv_calls` — contactos ao Apoio ao Cliente. |
| `Total_Drops` | BIGINT | Número de testes de chamada com `result = FALSE` no dia. |
| `Qualidade_Audio_MOS` | DOUBLE | MOS médio do cliente no dia (1 a 5). |
| `Desistencia` | BOOLEAN | Variável alvo de *churn* (`cdr.churn`). |

### KPIs derivados

- **Receita em risco por quadrante:** soma de `Receita_Em_Risco` filtrada por `Afetado_Tempestade = TRUE`.
- **Frustração:** rácio `Total_Drops / Total_Chamadas_Suporte` por cliente/dia.
- **Correlação Churn ↔ MOS:** taxa de `Desistencia = TRUE` em segmentos de `Qualidade_Audio_MOS` (ex.: < 2, 2–3, ≥ 3).
- **Segmentação por antiguidade:** distribuição de `Desistencia` por *buckets* de `Tempo_Subscrito`.

---

## Estratégia de carga e governança

- **ACID *full-replace*:** ambas as tabelas usam `replace_table_transaction` (Produto A) ou `replace_table_transaction_multi_insert` (Produto B) — uma única transacção Trino com `DELETE` + `INSERT` + `COMMIT`; qualquer erro força *rollback* e o *snapshot* anterior permanece intacto.
- **Idempotência:** o pipeline é desenhado para ser corrido várias vezes sobre o mesmo input sem efeitos colaterais (mesmas linhas, mesma ordem de `gold_row_id`).
- **Execução batch estática (v1.1.0):** o workflow `jdpt_lakehouse_pipeline` não está agendado; é disparado manualmente via Flyte para (re)construir o snapshot analítico do cenário pós-tempestade (jan.–fev. 2026). Contrato de design v1.0.0 mantém referência a batch noturno.
- **Partitioning Iceberg:** `partitioning = ARRAY['day("Data_Hora")']` no Produto A e `ARRAY['"Data_Referencia"']` no Produto B; reduz drasticamente o custo de queries com filtros temporais.
- **Contratos versionados (v1.0.0 schema; v1.1.0 operacional em `v2/`):** garantem grão, intervalos válidos para métricas (RSRP, MOS), chaves de negócio obrigatórias; SLO de frescura diária em v1.0.0, snapshot pós-run manual em v1.1.0 (ver `COMPARACAO_v1_v2.md`).
- **Compatibilidade evolutiva:** `add_nullable_column` é *minor* e backward-compatible; `rename_column` é *major* e *breaking*.
