import logging
import mp_people_sql_commands as sql
import os
import sys
from datetime import date, datetime, timedelta

current_directory = os.path.dirname(os.path.realpath(__file__))
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(parent_directory)

import helpers as h
from snowflake_operations import SnowflakeEngine


logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def main():
    env_vars = h.load_environment_variables()
    snow_user = env_vars['SNOWFLAKE_USER']
    snow_pwd = env_vars['SNOWFLAKE_PASSWORD']
    snow_acct = env_vars['SNOWFLAKE_ACCOUNT']
    snow_wh = env_vars['SNOWFLAKE_WAREHOUSE']

    dt1 = str(date.today() + timedelta(days=-1))
    snow_from_dt = dt1.replace('-', '_')

    snow = SnowflakeEngine(snow_user, snow_pwd, snow_acct, snow_wh)
    snow.set_db('RAW')
    snow.set_schema('MP_TEMP')
    snow.set_table(f'MP_PEOPLE_LAST_SEEN_{snow_from_dt}')
    snow.set_stage('S3_MIXPANEL_PEOPLE')
    snow.make_engine()

    filename_in_s3 = f'mp_people_last_seen_{dt1}.gz'

    commands = [
        sql.create_command(snow.db, snow.schema, snow.table),
        sql.copy_command(snow.db, snow.schema, snow.table, snow.stage, filename_in_s3),
        sql.delete_command(snow.db, snow.schema, snow.stage, filename_in_s3),
        sql.insert_command(snow.db, snow.schema, snow.table)
    ]

    snow.execute_commands(commands)


if __name__ == "__main__":
    main()
