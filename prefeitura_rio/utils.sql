-- COMPARE STAGING AND PROD TABLES

WITH ergon_tables AS (
    SELECT table_id
    FROM `rj-smfp.recursos_humanos_ergon.__TABLES_SUMMARY__`
),

staging_tables AS (
    SELECT table_id
    FROM `rj-smfp.recursos_humanos_ergon_staging.__TABLES_SUMMARY__`
),

tb AS (
SELECT e.table_id as ergon_table, s.table_id as staging_table
FROM ergon_tables e
RIGHT JOIN staging_tables s
ON e.table_id = s.table_id
)

SELECT * FROM tb
WHERE ergon_table IS NULL


-- GET TABLES SIZE AND CREATION TIMESTAMP

SELECT
    table_id,
    TIMESTAMP_MILLIS(creation_time) AS creation_timestamp,
    TIMESTAMP_MILLIS(last_modified_time) AS last_modified_timestamp,
    ROUND(size_bytes / POW(2, 30), 2) AS table_size_gb,
    row_count AS table_rows
FROM `rj-smas.protecao_social_cadunico.__TABLES__`
ORDER BY table_size_gb DESC



-- GET ALL TABLES COLUMNS FROM A DATASET
SELECT
    table_catalog AS project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    column_name
FROM `rj-smfp.compras_materiais_servicos_sigma_staging.INFORMATION_SCHEMA.COLUMNS`


-- GET PROJECT COST

WITH t AS (
    SELECT
        resource.labels.project_id  AS project_id,
        resource.labels.dataset_id  AS dataset_id,
        protopayload_auditlog.authenticationInfo.principalEmail AS user,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime AS startTime,
        CAST(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime AS DATE) AS date,
        protopayload_auditlog.requestMetadata.callerSuppliedUserAgent AS userAgent,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query AS query,
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes/1024/1024/1024/1024*5 AS cost_usd, -- bytes to TB, then 5 dollars per TB (US)
        protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes/1024/1024/1024/1024*5*5.87 AS cost_brl, -- 5,87 reais per dollar
    FROM `rj-smas.logs_bq.cloudaudit_googleapis_com_data_access_*`
)
SELECT
    project_id,
    dataset_id,
    user,
    date,
    SUM(cost_brl) AS cost_brl
FROM t
GROUP BY 1,2,3,4
ORDER BY cost_brl DESC