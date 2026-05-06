@task
def build_gold_churn_risk() -> str:
    conn = trino.dbapi.connect(host='host.docker.internal', port=8080, user='flyte', catalog='iceberg')
    cur = conn.cursor()
    
    sql = """
    CREATE OR REPLACE TABLE iceberg.gold.churn_risk_daily AS 
    WITH customer_network_impact AS (
        SELECT 
            CAST(date_of_test AS DATE) AS data_evento,
            phone_number,
            AVG(mos) AS avg_daily_mos,
            COUNT(CASE WHEN call_test_result = 'DROP' THEN 1 END) AS total_drops
        FROM iceberg.silver.call_tests
        GROUP BY CAST(date_of_test AS DATE), phone_number
    )
    SELECT 
        i.data_evento, c.phone_number, i.avg_daily_mos, i.total_drops,
        (c.day_charge + c.eve_charge + c.night_charge) AS daily_charge,
        c.custserv_calls, c.churn
    FROM customer_network_impact i
    JOIN iceberg.silver.cdr_customers c ON i.phone_number = c.phone_number
    """
    
    cur.execute(sql)
    cur.fetchall()
    return "Gold Table: Churn Risk Daily created successfully!"