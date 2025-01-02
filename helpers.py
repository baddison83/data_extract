import json
import logging
import os
from datetime import date, timedelta, datetime
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

output_props = '["$city", \
"$region", \
"$country_code", \
"$email", \
"firstName", \
"lastName", \
"User Id", \
"user ID", \
"User Handle", \
"handle", \
"birthYear", \
"Birth year", \
"gender", \
"Gender", \
"$media_source", \
"$campaign", \
"media_source", \
"campaign", \
"ad", \
"adset", \
"utm_campaign", \
"utm_content", \
"utm_medium", \
"utm_source", \
"Registration Date", \
"Registration Day", \
"Streaming Services", \
"genresScreenplay", \
"genresBooks", \
"genresPodcasts", \
"subGenresScreenplay", \
"subGenresBooks", \
"subGenresPodcasts", \
"genresNewsletterScreenplay", \
"genresNewsletterBooks", \
"genresNewsletterPodcasts", \
"subGenresNewsletterScreenplay", \
"subGenresNewsletterBooks", \
"subGenresNewsletterPodcasts", \
"genre", \
"Phone", \
"phoneNumber", \
"dateOfBirth", \
"emailOptIn", \
"mlPredictChurnW2Priority", \
"anonUserId", \
"Registration Platform", \
"Streaming Devices", \
"email", \
"dmi_lead_id"]'


def date_argv(lst):
    if len(lst) >= 3:
        start_date_str = lst[1]
        end_date_str = lst[2]
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    else:
        start_date = None
        end_date = None

    return start_date, end_date


def load_environment_variables():
    load_dotenv()
    return {
        'AF_PULL_API_KEY': os.environ.get('AF_PULL_API_KEY'),
        'MP_API_SECRET': os.environ.get('MP_API_SECRET'),
        'AWS_ACCESS': os.environ.get('AWS_ACCESS'),
        'AWS_SECRET': os.environ.get('AWS_SECRET'),
        'SNOWFLAKE_USER': os.environ.get('SNOWFLAKE_USER'),
        'SNOWFLAKE_PASSWORD': os.environ.get('SNOWFLAKE_PASSWORD'),
        'SNOWFLAKE_ACCOUNT': os.environ.get('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_WAREHOUSE': os.environ.get('SNOWFLAKE_WAREHOUSE')
    }


def get_date_range(start_date=None, end_date=None):
    if start_date is None:
        start_date = date.today() - timedelta(days=1)
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    logger.info(f"Start date: {start_date}, end date: {end_date}")
    return start_date.isoformat(), end_date.isoformat()


def get_engage_page(self, params):
    response = self.request(self.formatted_api, ["engage"], params)
    data = json.loads(response)
    if "results" in data:
        return data
    else:
        return
