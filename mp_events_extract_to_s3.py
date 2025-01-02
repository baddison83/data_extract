import base64
import boto3
import json
import logging
import os
import sys
import urllib.request
import urllib.parse

current_directory = os.path.dirname(os.path.realpath(__file__))
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
sys.path.append(parent_directory)

import helpers as h

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def unicode_urlencode(params):
    if isinstance(params, dict):
        params = list(params.items())
    for i, param in enumerate(params):
        if isinstance(param[1], list):
            params[i] = (
                param[0],
                json.dumps(param[1]),
            )

    result = urllib.parse.urlencode(
        [(k, isinstance(v, str) and v.encode("utf-8") or v) for k, v in params]
    )
    return result


def mp_get_request(path_components, params, mp_secret, headers=None):
    base_url = "https://data.mixpanel.com/api"
    base = [base_url, "2.0"]
    request_url = "/".join(base + path_components)
    basic_credentials = f"{mp_secret}:"
    encoded_credentials = base64.b64encode(basic_credentials.encode("utf-8")).decode("utf-8")
    request_url += "?" + unicode_urlencode(params)
    data = None

    if headers is None:
        headers = {}
    headers["Authorization"] = f"Basic {encoded_credentials}"

    logger.info(f"Creating MP API request object")
    return urllib.request.Request(request_url, data, headers, method="GET")


def open_request_object(request):
    try:
        logger.info(f"Opening url request...")
        return urllib.request.urlopen(request, timeout=1200)
    except Exception as e:
        logger.error(f'An error occurred while opening request object: {e}')


def s3_put_object(s3_client, body, bucket, filename_in_s3, encoding=None):
    if encoding == 'gzip':
        logger.info(f"Reading the API response")
        body = body.read()

    try:
        logger.info(f"Uploading {filename_in_s3} to S3 bucket {bucket}")
        s3_client.put_object(Body=body,
                             Bucket=bucket,
                             Key=filename_in_s3,
                             ContentEncoding=encoding
                             )
        logger.info(f"Successfully uploaded {filename_in_s3} to S3 bucket {bucket}")
    except Exception as e:
        logger.error(f'An error occurred while putting object to S3: {e}')


def main(start_date=None, end_date=None):
    try:
        env_vars = h.load_environment_variables()
        mp_api_secret = env_vars['MP_API_SECRET']
        aws_access = env_vars['AWS_ACCESS']
        aws_secret = env_vars['AWS_SECRET']

        from_dt, to_dt = h.get_date_range(start_date, end_date)

        req = mp_get_request(path_components=["export"],
                             params={'from_date': f'{from_dt}', 'to_date': f'{to_dt}'},
                             mp_secret=mp_api_secret,
                             headers={"Accept-encoding": "gzip"}
                             )

        response = open_request_object(req)

        bucket_name = 'rawdataexports'
        filename_in_s3 = f'mixpanel/events/mp_events_{from_dt}_to_{to_dt}.gz'

        s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)
        s3_put_object(s3, response, bucket_name, filename_in_s3, encoding='gzip')

    except Exception as e:
        logger.error(f'An unexpected error occurred: {e}')


if __name__ == "__main__":
    start_date, end_date = h.date_argv(sys.argv)
    main(start_date, end_date)
