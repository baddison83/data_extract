import boto3
import io
import logging
import os
import requests
import pandas as pd
import sys

current_directory = os.path.dirname(os.path.realpath(__file__))
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(parent_directory)

import helpers as h

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def appsflyer_reports():
    return {'installs_report': 'new_paid_install',
            'organic_installs_report': 'organic_install',
            'installs-retarget': 'retarget_install'}


def get_request_response(af_device, report, from_date, to_date, headers):
    base = f"https://hq1.appsflyer.com/api/raw-data/export/app/{af_device}/{report}/v5?"
    date_params = f"from={from_date}&to={to_date}"
    additional_params = ("&additional_fields=conversion_type,match_type,deeplink_url,campaign_type,device_download_time,"
                  "device_model,is_lat,device_category,app_type,att,engagement_type"
                  )

    url = base + date_params + additional_params
    logger.info(f"Pull url: {url}")

    try:
        logger.info("Submitting appsflyer api pull request...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f"Error submitting AppsFlyer API request: {e}")
        return None


def response_to_pandas(response, report, device):
    if response is None:
        return None

    try:
        logger.info("Converting api response content to pandas...")
        if response.status_code == 200:
            url_data = response.content.decode('utf-8')
            raw_data = pd.read_csv(io.StringIO(url_data))
            raw_data['install_type'] = appsflyer_reports().get(report, '')
            raw_data['device'] = device
            return raw_data
        else:
            logger.error(f"Error: Received non-200 status code: {response.status_code}")
            return None
    except pd.errors.ParseError as e:
        logger.error(f"Error parsing CSV data: {e}")
        return None


def retrieve_data(device, device_id, headers, from_dt, to_dt, reports):
    responses = [get_request_response(device_id, r, from_dt, to_dt, headers) for r in reports]
    return [response_to_pandas(res, rep, device) for res, rep in zip(responses, reports.keys())]


def concat_ios_android(ios_data, android_data):
    logger.info('Concatenating dataframe...')
    return pd.concat([*ios_data, *android_data], ignore_index=True)


def s3_put_pandas_as_csv_buffer(s3_client, df, bucket_name, filename_in_s3):
    logger.info('Uploading appsflyer csv to s3...')
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name,
            Key=filename_in_s3
            )
        logger.info("Upload completed successfully")
        return response
    except Exception as e:
        logger.error(f"Error uploading CSV to S3: {e}")
        raise


def main(start_date=None, end_date=None):
    try:
        env_vars = h.load_environment_variables()
        af_pull_api_key = env_vars['AF_PULL_API_KEY']
        aws_access = env_vars['AWS_ACCESS']
        aws_secret = env_vars['AWS_SECRET']

        headers = {
            "accept": "text/csv",
            "authorization": f"Bearer {af_pull_api_key}"
        }

        from_dt, to_dt = h.get_date_range(start_date, end_date)

        devices = {'ios': 'id1264195462', 'android': 'com.Likewise.apps.Likewise'}
        reports = appsflyer_reports()

        ios_data = retrieve_data('ios', devices['ios'], headers, from_dt, to_dt, reports)
        android_data = retrieve_data('android', devices['android'], headers, from_dt, to_dt, reports)

        dataframe = concat_ios_android(ios_data, android_data)

        bucket_name = 'rawdataexports'
        filename_in_s3 = f'appsflyer/appsflyer_installs_{from_dt}_to_{to_dt}.csv'

        s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
        s3_put_pandas_as_csv_buffer(s3, dataframe, bucket_name, filename_in_s3)

    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    start_date, end_date = h.date_argv(sys.argv)
    main(start_date, end_date)
