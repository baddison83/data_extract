
def create_command(snow_db_name, snow_schema_name, snow_table_name):
    return f"""CREATE OR REPLACE TABLE {snow_db_name}.{snow_schema_name}.{snow_table_name} (
              attributed_touch_type STRING,
              attributed_touch_time STRING,
              install_time STRING,
              event_time STRING,
              event_name STRING,
              event_value STRING,
              event_revenue STRING,
              event_revenue_currency STRING,
              event_revenue_used STRING,
              event_source STRING,
              is_receipt_validated STRING,
              partner STRING,
              media_source STRING,
              channel STRING,
              keywords STRING,
              campaign STRING,
              campaign_id STRING,
              adset STRING,
              adset_id STRING,
              ad STRING,
              ad_id STRING,
              adt_type STRING,
              site_id STRING,
              sub_site_id STRING,
              sub_param_1 STRING,
              sub_param_2 STRING,
              sub_param_3 STRING,
              sub_param_4 STRING,
              sub_param_5 STRING,
              cost_model STRING,
              cost_value STRING,
              cost_currency STRING,
              contributor_1_partner STRING,
              contributor_1_media_source STRING,
              contributor_1_campaign STRING,
              contributor_1_touch_type STRING,
              contributor_1_touch_time STRING,
              contributor_2_partner STRING,
              contributor_2_media_source STRING,
              contributor_2_campaign STRING,
              contributor_2_touch_type STRING,
              contributor_2_touch_time STRING,
              contributor_3_partner STRING,
              contributor_3_media_source STRING,
              contributor_3_campaign STRING,
              contributor_3_touch_type STRING,
              contributor_3_touch_time STRING,
              region STRING,
              country_code STRING,
              state STRING,
              city STRING,
              postal_code STRING,
              dma STRING,
              ip STRING,
              wifi BOOLEAN,
              operator STRING,
              carrier STRING,
              language STRING,
              appsflyer_id STRING,
              advertising_id STRING,
              idfa STRING,
              android_id STRING,
              customer_user_id STRING,
              imei STRING,
              idfv STRING,
              platform STRING,
              device_type STRING,
              os_version STRING,
              app_version STRING,
              sdk_version STRING,
              app_id STRING,
              app_name STRING,
              bundle_id STRING,
              is_retargeting BOOLEAN,
              retargeting_conversion_type STRING,
              attribution_lookback STRING,
              reengagement_windo STRING,
              is_primary_attribution STRING,
              user_agent STRING,
              http_referrer STRING,
              original_url STRING,
              conversion_type STRING,
              match_type STRING,
              deeplink_url STRING,
              campaign_type STRING,
              device_download_type STRING,
              device_model STRING,
              is_lat BOOLEAN,
              device_category STRING,
              app_type STRING,
              att STRING,
              engagement_type STRING,
              install_type STRING,
              device STRING
              )"""


def copy_command(snow_db_name,
                 snow_schema_name,
                 snow_table_name,
                 bucket_name,
                 path_name,
                 filename_in_s3,
                 aws_access,
                 aws_secret
                 ):

    statement = f"""COPY INTO {snow_db_name}.{snow_schema_name}.{snow_table_name}
           FROM s3://{bucket_name}/{path_name}/{filename_in_s3} credentials=(AWS_KEY_ID='{aws_access}' AWS_SECRET_KEY='{aws_secret}')
           FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' 
                          FIELD_DELIMITER = ','
                          RECORD_DELIMITER = '\n'
                          FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                          REPLACE_INVALID_CHARACTERS = TRUE
                          SKIP_BLANK_LINES = TRUE
                          SKIP_HEADER = 1);
           """

    # print(statement)
    return statement


def insert_command(insert_table, snow_db_name, snow_schema_name, snow_table_name):
    statement = f"""
    INSERT INTO {insert_table}
        SELECT * FROM {snow_db_name}.{snow_schema_name}.{snow_table_name};
    """
    return statement
