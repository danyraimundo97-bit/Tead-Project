# Comparação Data Contracts v0 → v1

Documento para o relatório: evolução dos contratos de **especificação conceptual** (v0) para **implementação Trino/Iceberg** (v1).

| Ficheiro | v0 (antigo) | v1 (actual) |
|----------|-------------|-------------|
| Contrato A | [`v0/Contrato A - gold.network_quality_daily.txt`](v0/Contrato%20A%20-%20gold.network_quality_daily.txt) | [`v1/Contrato A - gold.network_quality_daily.txt`](v1/Contrato%20A%20-%20gold.network_quality_daily.txt) |
| Contrato B | [`v0/Contrato B - gold.churn_risk_daily.txt`](v0/Contrato%20B%20-%20gold.churn_risk_daily.txt) | [`v1/Contrato B - gold.churn_risk_daily.txt`](v1/Contrato%20B%20-%20gold.churn_risk_daily.txt) |
| Produto A | [`v0/Produto A - gold.network_quality_daily.txt`](v0/Produto%20A%20-%20gold.network_quality_daily.txt) | [`v1/Produto A - gold.network_quality_daily.txt`](v1/Produto%20A%20-%20gold.network_quality_daily.txt) |
| Produto B | [`v0/Produto B - gold.churn_risk_daily.txt`](v0/Produto%20B%20-%20gold.churn_risk_daily.txt) | [`v1/Produto B - gold.churn_risk_daily.txt`](v1/Produto%20B%20-%20gold.churn_risk_daily.txt) |

Versão **activa** em produção: **v1** (cópia espelhada em `Data_Contracts/` e `Data_Products/` na raiz das pastas).

---

## Contrato A — `gold.network_quality_daily`

| Aspeto | v0.1.0 (draft) | v1.0.0 (active) |
|--------|----------------|-----------------|
| **Estado** | `draft` | `active` |
| **Grão** | `(tower_id, date)` | `(ID_Antena_Conectada, Data_Hora)` |
| **PK** | `tower_id`, `date` | `ID_Antena_Conectada`, `Data_Hora` |
| **Idioma colunas** | Inglês (`avg_rsrp_dbm`, `tower_status`) | PT + schema Trino (`Potencia_RSRP`, `Estado_Antena`) |
| **Colunas** | 6 campos agregados | 18 campos (coords, tecnologia, contagens testes) |
| **Métrica DROP** | `drop_rate_pct` (decimal %) | `Telefones_Falha` / `Telefones_Sucesso` (contagens) |
| **Estado torre** | `tower_status` string ACTIVE/DOWN | `Estado_Antena` boolean |
| **Tabela catálogo** | (não especificado) | `iceberg.gold.network_quality_daily` |
| **Motivo mudança** | Template UC / negócio | Alinhamento com `build_gold_network_quality.py` e DDL Iceberg |

---

## Contrato B — `gold.churn_risk_daily`

| Aspeto | v0.1.0 (draft) | v1.0.0 (active) |
|--------|----------------|-----------------|
| **Grão** | `(phone_number, date)` | `(Telefone, Data_Referencia)` |
| **MOS** | `avg_mos_7d` (média móvel 7 dias) | `Qualidade_Audio_MOS` (AVG do dia) |
| **Receita** | `revenue_at_risk_eur` = projeção 30× se MOS<2 | `Receita_Em_Risco` = soma cargas CDR do dia |
| **Churn** | `is_churned` | `Desistencia` |
| **Suporte** | `custserv_calls_7d` (janela 7d) | `Total_Chamadas_Suporte` (CDR snapshot) |
| **Tempestade** | (não existia) | `Afetado_Tempestade` (lon > -8.80) |
| **Drops** | (não existia) | `Total_Drops` (call tests do dia) |
| **Late data** | MERGE em partições | Rebuild ACID (`acid_replace`) |
| **Motivo mudança** | Modelo preditivo hipotético | Implementação real em `build_gold_churn_risk.py` |

---

## Data Products — diferenças transversais

| Aspeto | v0 | v1 |
|--------|----|----|
| **Lineage silver** | `*_clean`, `towers_status` (nomes fictícios) | `iceberg.silver.network_logs`, `cdr_customers`, etc. |
| **Particionamento** | `date` genérico | `day("Data_Hora")` / `Data_Referencia` (Iceberg) |
| **Update mode** | `merge` | `acid_replace` (DELETE+INSERT transaccional) |
| **Workflow Flyte** | `workflows.gold_*_wf` (placeholder) | `jdpt_lakehouse_pipeline` + task real |
| **Views** | `warehouse.semantic.vw_*` (não implementada) | `iceberg.gold.vw_storm_impact_executive` |
| **Workflow real** | Não ligado ao repo | Ligado a `flyte-workflows/` |

---

## Narrativa para o relatório (schema evolution)

1. **v0.1.0** — contratos redigidos na fase de *Business Understanding* (CRISP-DM), com nomes anglófonos e métricas de negócio simplificadas.
2. **Implementação** — pipelines Flyte revelaram schema gold real (colunas PT, tipos Iceberg, fórmulas concretas).
3. **v1.0.0** — *major alignment*: renomeação de colunas, grão confirmado, semântica `Receita_Em_Risco` corrigida, particionamento e ACID documentados.
4. **Política futura** — `add_nullable_column` → minor; `rename_column` → major breaking (secção `change_management` em ambos os contratos v1).
5. **v1.1.0 (v2/)** — *minor operacional*: batch estático manual; ver [`COMPARACAO_v1_v2.md`](COMPARACAO_v1_v2.md).
