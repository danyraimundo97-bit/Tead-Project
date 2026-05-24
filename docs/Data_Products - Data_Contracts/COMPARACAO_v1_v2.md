# Comparação Data Contracts v1 → v2

Documento para o relatório: evolução **operacional** dos contratos v1.0.0 (design com batch noturno) para v1.1.0 (batch estático manual — cenário académico TEAD).

| Ficheiro | v1.0.0 (schema + design) | v1.1.0 (operacional — repo) |
|----------|--------------------------|-----------------------------|
| Contrato A | [`v1/Contrato A - gold.network_quality_daily.txt`](v1/Contrato%20A%20-%20gold.network_quality_daily.txt) | [`v2/Contrato A - gold.network_quality_daily.txt`](v2/Contrato%20A%20-%20gold.network_quality_daily.txt) |
| Contrato B | [`v1/Contrato B - gold.churn_risk_daily.txt`](v1/Contrato%20B%20-%20gold.churn_risk_daily.txt) | [`v2/Contrato B - gold.churn_risk_daily.txt`](v2/Contrato%20B%20-%20gold.churn_risk_daily.txt) |
| Produto A | [`v1/Produto A - gold.network_quality_daily.txt`](v1/Produto%20A%20-%20gold.network_quality_daily.txt) | [`v2/Produto A - gold.network_quality_daily.txt`](v2/Produto%20A%20-%20gold.network_quality_daily.txt) |
| Produto B | [`v1/Produto B - gold.churn_risk_daily.txt`](v1/Produto%20B%20-%20gold.churn_risk_daily.txt) | [`v2/Produto B - gold.churn_risk_daily.txt`](v2/Produto%20B%20-%20gold.churn_risk_daily.txt) |

**Schema e interface:** inalterados (mesmo grão, colunas, tipos Iceberg/Trino).

**Versão activa para implementação no repositório:** v1.1.0 (`v2/`).  
**Versão de referência de design (batch noturno):** v1.0.0 (`v1/` + espelho em `Data_Contracts/` e `Data_Products/`).

---

## Diferenças v1.0.0 → v1.1.0

| Aspeto | v1.0.0 | v1.1.0 |
|--------|--------|--------|
| **Schedule** | `"0 3 * * *"` / `"0 4 * * *"` | `null` |
| **Trigger** | (implícito cron) | `manual` |
| **Execution model** | batch diário agendado | `static_rebuild` |
| **Backfill unit** | `day` | `full_pipeline_rerun` |
| **SLO frescura** | P95 ≤ 4 h após fim do dia civil | Snapshot após conclusão do último run batch |
| **Alertas** | `freshness_breach` | `pipeline_failure` (Produto A) |
| **Motivo** | Modelo operacional de referência | Alinhamento com cenário académico e `jdpt_lakehouse_pipeline` manual |

---

## Narrativa para o relatório

1. **v1.0.0** — contrato de implementação com schema Trino/Iceberg e SLO de frescura diária (batch noturno Flyte).
2. **Decisão de projeto** — no contexto TEAD, o batch corre sob pedido para reconstruir um snapshot estático do período pós-tempestade, em vez de agendamento recorrente.
3. **v1.1.0** — *minor bump* operacional: mesma interface de dados, alteração documentada em `production` e `quality.slo` (sem breaking change para consumidores BI).

Ver também: [`COMPARACAO_v0_v1.md`](COMPARACAO_v0_v1.md) (evolução conceptual → implementação).
