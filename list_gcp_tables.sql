-- https://cloud.google.com/bigquery/docs/reference/standard-sql/scripting#execute_immediate
-- https://cloud.google.com/bigquery/docs/information-schema-datasets#schemata_view
-- List all tables and their respective datasets related to a project in GCP.

BEGIN

DECLARE sql string;
DECLARE query STRING DEFAULT (
        SELECT STRING_AGG(
            (SELECT """
                SELECT
                    project_id,
                    dataset_id,
                    table_id,
                    row_count,
                    size_bytes,
                    -- Convert bytes to GB.
                    ROUND(size_bytes/pow(10,6),2) as size_mb,
                    ROUND(size_bytes/pow(10,9),2) as size_gb,
                    ROUND(size_bytes/pow(10,12),2) as size_tb,
                    -- Convert UNIX EPOCH to a timestamp.
                    TIMESTAMP_MILLIS(creation_time) AS creation_time,
                    TIMESTAMP_MILLIS(last_modified_time) as last_modified_time,                
                    CASE 
                        WHEN type = 1 THEN 'table'
                        WHEN type = 2 THEN 'view'
                        WHEN type = 3 THEN 'external table'
                        ELSE NULL
                    END AS type,
                    TIMESTAMP(CURRENT_DATETIME("America/Sao_Paulo")) AS process_time_br
                FROM `""" || s || 
                """.__TABLES__`"""), 
                " UNION ALL ")
        FROM UNNEST((SELECT ARRAY_AGG(SCHEMA_NAME) FROM region-us.INFORMATION_SCHEMA.SCHEMATA)) AS s
);

SET sql = 'CREATE OR REPLACE TABLE `project-id.dataset_id.gcp_tables` AS ' || query;
EXECUTE IMMEDIATE sql;

END
