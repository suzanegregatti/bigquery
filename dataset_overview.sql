WITH tables AS (

    SELECT
        project_id,
        dataset_id,
        table_id,
        row_count,
        size_bytes,
        ROUND(size_bytes/pow(10,9), 4) as size_gb,
        TIMESTAMP_MILLIS(creation_time) AS creation_time,
        TIMESTAMP_MILLIS(last_modified_time) as last_modified_time
    FROM bigquery-public-data.census_bureau_usa.__TABLES__

),

id_columns AS (

    SELECT
        table_name,
        ARRAY_TO_STRING(ARRAY_AGG(column_name), '\n') AS id_columns
    FROM
        bigquery-public-data.census_bureau_usa.INFORMATION_SCHEMA.COLUMNS
    WHERE        
        REGEXP_CONTAINS(column_name, r'^id|_id|id_')
    GROUP BY
        table_name

),

data_columns AS (

    SELECT
        table_name,
        ARRAY_TO_STRING(ARRAY_AGG(column_name), '\n') AS data_columns
    FROM
        bigquery-public-data.census_bureau_usa.INFORMATION_SCHEMA.COLUMNS
    WHERE
        NOT(REGEXP_CONTAINS(column_name, r'^id|_id|id_'))        
    GROUP BY
        table_name
)

SELECT
    tables.project_id,
    tables.dataset_id,
    tables.table_id,
    ( 'https://console.cloud.google.com/bigquery?project=' || tables.project_id ||
      '&d=' || tables.dataset_id ||
      '&p=' || tables.project_id ||
      '&page=table&t=' || tables.table_id
    ) AS table_link,
    id_columns.id_columns,
    data_columns.data_columns, 
    tables.row_count,
    tables.size_bytes,
    tables.size_gb,
    tables.creation_time,
    tables.last_modified_time
FROM tables
LEFT JOIN id_columns
    ON tables.table_id = id_columns.table_name
LEFT JOIN data_columns
    ON tables.table_id = data_columns.table_name