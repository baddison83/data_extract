import logging
import mp_events_sql_commands as sql
import os
import sys

current_directory = os.path.dirname(os.path.realpath(__file__))
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(parent_directory)

import helpers as h
from snowflake_operations import SnowflakeEngine


logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def main(start_date=None, end_date=None):
    env_vars = h.load_environment_variables()
    aws_access = env_vars['AWS_ACCESS']
    aws_secret = env_vars['AWS_SECRET']
    snow_user = env_vars['SNOWFLAKE_USER']
    snow_pwd = env_vars['SNOWFLAKE_PASSWORD']
    snow_acct = env_vars['SNOWFLAKE_ACCOUNT']
    snow_wh = env_vars['SNOWFLAKE_WAREHOUSE']

    from_dt, to_dt = h.get_date_range(start_date, end_date)
    snow_from_dt = from_dt.replace('-', '_')
    snow_to_dt = to_dt.replace('-', '_')

    snow = SnowflakeEngine(snow_user, snow_pwd, snow_acct, snow_wh)
    snow.set_db('RAW_DATA_LOADS_DB')
    snow.set_schema('MIXPANEL_TEMP')
    snow.set_table(f'MP_EVENTS_{snow_from_dt}_to_{snow_to_dt}')
    snow.set_stage('S3_MIXPANEL_EVENTS')
    snow.make_engine()

    bucket_name = 'rawdataexports'
    path = 'mixpanel/events'
    filename_in_s3 = f'mp_events_{from_dt}_to_{to_dt}.gz'

    commands = [
        sql.create_command(snow.db, snow.schema, snow.table),
        sql.copy_command(snow.db, snow.schema, snow.table, snow.stage, bucket_name, path, filename_in_s3, aws_access, aws_secret),
        sql.insert_command('RAW_DATA_LOADS_DB.MIXPANEL.MP_EVENTS', snow.db, snow.schema, snow.table)
    ]

    snow.execute_commands(commands)


if __name__ == "__main__":
    start_date, end_date = h.date_argv(sys.argv)
    main(start_date, end_date)
