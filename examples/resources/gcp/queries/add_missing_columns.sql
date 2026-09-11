-- Add whichever columns of @schema the table does not have yet.
-- Reading INFORMATION_SCHEMA first keeps a run with nothing to add free:
-- BigQuery charges every ALTER TABLE against a limit of 1500 table metadata
-- updates per table per day, including ones that change nothing.
FOR col IN (
    SELECT name, type
    FROM UNNEST(JSON_QUERY_ARRAY(@schema, '$.fields')) AS f,
    UNNEST([STRUCT(
        JSON_VALUE(f, '$.name') AS name,
        JSON_VALUE(f, '$.type') AS type
    )])
    WHERE name NOT IN (
        SELECT column_name
        FROM `my-project-id.analytics.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table_name
    )
)
DO
    EXECUTE IMMEDIATE FORMAT(
        "ALTER TABLE `my-project-id.analytics.%s` ADD COLUMN `%s` %s",
        @table_name, col.name, col.type);
END FOR;
