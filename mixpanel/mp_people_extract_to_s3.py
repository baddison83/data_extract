import base64
import boto3
import gzip
import io
import json
import logging
import os
import sys
import urllib.request
import urllib.parse
from datetime import date, datetime, timedelta

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
    base_url = "https://mixpanel.com/api"
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


def get_response_values(dictionary):
    session_id = dictionary['session_id']
    total_entries = dictionary['total']
    page_size = dictionary['page_size']
    page = dictionary['page']
    results = dictionary['results']
    return session_id, total_entries, page_size, page, results


def get_remaining_results(params, sesh, entries, page_size, page, res, mp_api_secret):
    params['session_id'] = sesh
    params['page'] = page + 1

    while page_size * page < entries:
        logger.info(f'Total entries: {entries}')
        p = params['page']
        logger.info(f"This is the page number about to be called {p}")
        req = mp_get_request(path_components=["engage"],
                             params=params,
                             mp_secret=mp_api_secret
                             )

        response = urllib.request.urlopen(req, timeout=1200)
        dikt = json.loads(response.read().decode("utf-8"))

        res.extend(dikt['results'])
        logger.info(f"length of results list: {len(res)}")

        page = dikt['page']
        logger.info(f'this is the page number that was just called: {page}')
        params['page'] = page + 1

    return res


def gzip_data_to_buffer(data):
    buffer = io.BytesIO()

    with gzip.GzipFile(fileobj=buffer, mode='w') as f:
        for item in data:
            f.write(bytes(str(item) + '\n', 'utf-8'))

    buffer.seek(0)
    return buffer


def s3_put_object(s3_client, obj, bucket, filename_in_s3):
    try:
        logger.info(f"Uploading {filename_in_s3} to s3...")
        s3_client.put_object(Bucket=bucket, Key=filename_in_s3 + '.gz', Body=obj)
        logger.info("Upload Successful")
    except Exception as e:
        logger.error(f'An error occurred uploading s3 object: {e}')


def main():
    env_vars = h.load_environment_variables()
    mp_api_secret = env_vars['MP_API_SECRET']
    aws_access = env_vars['AWS_ACCESS']
    aws_secret = env_vars['AWS_SECRET']

    dt1 = str(date.today() + timedelta(days=-1))
    dt2 = str(date.today() + timedelta(days=0))
    selector = f'((properties["$last_seen"] >= "{dt1}T00:00:00") and (properties["$last_seen"] < "{dt2}T00:00:00"))'
    # selector = 'properties["Registration Date"] >= "2024-02-05T00:00:00"'

    parameters = {'selector': selector,
                  'output_properties': h.output_props
                  }

    logger.info(f'Querying MP api for users last seen on {dt1}')
    req = mp_get_request(path_components=["engage"],
                         params=parameters,
                         mp_secret=mp_api_secret
                         )
    response = open_request_object(req)
    dikt = json.loads(response.read().decode("utf-8"))

    session_id, total_entries, page_size, page, results = get_response_values(dikt)
    results = get_remaining_results(parameters, session_id, total_entries, page_size, page, results, mp_api_secret)

    for d in results:
        d['$properties'] = {key: value for key, value in d['$properties'].items() if value is not None}

    buff = gzip_data_to_buffer(results)
    s3 = boto3.client('s3', aws_access_key_id=aws_access, aws_secret_access_key=aws_secret)

    bucket = 'rawdataexports'
    filename_in_s3 = f'mixpanel/people/mp_people_last_seen_{dt1}'
    # filename_in_s3 = f'mixpanel/people/mp_people_registration_date_{dt1}'
    s3_put_object(s3, buff, bucket, filename_in_s3)


if __name__ == "__main__":
    main()
