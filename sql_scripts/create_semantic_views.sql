-- Vista executiva: cruza Produto A (qualidade rede) + Produto B (churn/receita) por dia e zona.
-- Query-on-read sobre marts materializados; executar após batch gold.
CREATE OR REPLACE VIEW iceberg.gold.vw_storm_impact_executive AS
SELECT
    CAST(nq.data_hora AS DATE) AS dia,
    nq.zona_leiria AS zona,
    COUNT(DISTINCT nq.id_antena_conectada)
        FILTER (WHERE nq.estado_antena = FALSE) AS antenas_down,
    AVG(nq.potencia_rsrp) AS rsrp_medio_dbm,
    AVG(nq.ruido_sinr) AS sinr_medio_db,
    SUM(nq.telefones_falha) AS total_telefones_falha,
    AVG(cr.qualidade_audio_mos) AS mos_medio,
    SUM(cr.receita_em_risco) AS receita_em_risco_eur,
    SUM(CASE WHEN cr.desistencia THEN 1 ELSE 0 END) AS churns_dia,
    SUM(CASE WHEN cr.afetado_tempestade THEN 1 ELSE 0 END) AS clientes_zona_afetada
FROM iceberg.gold.network_quality_daily nq
LEFT JOIN iceberg.gold.churn_risk_daily cr
    ON CAST(nq.data_hora AS DATE) = cr.data_referencia
GROUP BY CAST(nq.data_hora AS DATE), nq.zona_leiria;
