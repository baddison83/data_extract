import appsflyer_extract_to_s3 as extract
import appsflyer_load_to_snowflake as load
import sys
from datetime import datetime


def main(start_date=None, end_date=None):
    extract.main(start_date, end_date)
    load.main(start_date, end_date)


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        start_date_str = sys.argv[1]
        end_date_str = sys.argv[2]
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    else:
        start_date = None
        end_date = None

    main(start_date, end_date)