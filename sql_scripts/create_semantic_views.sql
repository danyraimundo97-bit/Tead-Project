-- Vista executiva: cruza Produto A (qualidade rede) + Produto B (churn/receita) por dia e zona.
-- Query-on-read sobre marts materializados; executar após batch gold.
CREATE OR REPLACE VIEW iceberg.gold.vw_storm_impact_executive AS
SELECT
    CAST(nq."Data_Hora" AS DATE) AS dia,
    nq."Zona_Leiria" AS zona,
    COUNT(DISTINCT nq."ID_Antena_Conectada")
        FILTER (WHERE nq."Estado_Antena" = FALSE) AS antenas_down,
    AVG(nq."Potencia_RSRP") AS rsrp_medio_dbm,
    AVG(nq."Ruido_SINR") AS sinr_medio_db,
    SUM(nq."Telefones_Falha") AS total_telefones_falha,
    AVG(cr."Qualidade_Audio_MOS") AS mos_medio,
    SUM(cr."Receita_Em_Risco") AS receita_em_risco_eur,
    SUM(CASE WHEN cr."Desistencia" THEN 1 ELSE 0 END) AS churns_dia,
    SUM(CASE WHEN cr."Afetado_Tempestade" THEN 1 ELSE 0 END) AS clientes_zona_afetada
FROM iceberg.gold.network_quality_daily nq
LEFT JOIN iceberg.gold.churn_risk_daily cr
    ON CAST(nq."Data_Hora" AS DATE) = cr."Data_Referencia"
GROUP BY CAST(nq."Data_Hora" AS DATE), nq."Zona_Leiria";
