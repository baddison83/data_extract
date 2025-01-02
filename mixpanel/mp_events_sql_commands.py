
def create_command(db_name, schema_name, table_name):
    statement = f"""CREATE OR REPLACE TABLE {db_name}.{schema_name}.{table_name} (
              event STRING,
              properties VARIANT
              )"""

    return statement


def copy_command(snow_db_name,
                 snow_schema_name,
                 snow_table_name,
                 snow_stage_name,
                 bucket_name,
                 path_name,
                 filename_in_s3,
                 aws_access,
                 aws_secret):
    statement = f"""COPY INTO {snow_db_name}.{snow_schema_name}.{snow_table_name} (event, properties)
                    FROM (SELECT $1:event,
                         $1:properties
                 FROM @{snow_db_name}.{snow_schema_name}.{snow_stage_name}/{filename_in_s3} t)
            ON_ERROR = 'continue';;
           """
    return statement


def insert_command(insert_table, snow_db, snow_schema, snow_table):
    statement = f"""
    INSERT INTO {insert_table}
    SELECT TO_TIMESTAMP_TZ(REPLACE(Properties:time, '"', '')) AS ts,
           REPLACE(Properties:distinct_id, '"', '') AS distinct_id,
           event AS name,
           object_delete(Properties, 'time', 'distinct_id') AS properties
    FROM {snow_db}.{snow_schema}.{snow_table};
    """
    return statement
