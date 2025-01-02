
def create_command(db_name, schema_name, table_name):
    statement = f"""CREATE OR REPLACE TABLE {db_name}.{schema_name}.{table_name} (
              distinct_id STRING,
              properties VARIANT
              )"""

    return statement


def copy_command(snow_db_name,
                 snow_schema_name,
                 snow_table_name,
                 snow_stage_name,
                 filename_in_s3):
    statement = f"""COPY INTO {snow_db_name}.{snow_schema_name}.{snow_table_name} (distinct_id, properties)
                    FROM (SELECT REPLACE($1:"$distinct_id", '"', ''), $1:"$properties"
                 FROM @{snow_db_name}.{snow_schema_name}.{snow_stage_name}/{filename_in_s3} t)
            ON_ERROR = 'continue';;
           """
    return statement


def delete_command(snow_db_name, snow_schema_name, snow_stage_name, filename_in_s3):
    statement = f"""
    DELETE FROM RAW_DATA_LOADS_DB.MIXPANEL.MP_PEOPLE
    WHERE distinct_id IN (SELECT REPLACE($1:"$distinct_id", '"', '') AS distinct_id
                    FROM @{snow_db_name}.{snow_schema_name}.{snow_stage_name}/{filename_in_s3});
    """
    return statement


def insert_command(snow_db_name, snow_schema_name, snow_table_name):
    statement = f"""
    INSERT INTO RAW_DATA_LOADS_DB.MIXPANEL.MP_PEOPLE
        SELECT 
        distinct_id,
        REPLACE(Properties:"$last_seen", '"', '') AS last_seen,
        REPLACE(Properties:"$city", '"', '') AS city,
        REPLACE(Properties:"$region", '"', '') AS region,
        REPLACE(Properties:"$country_code", '"', '') AS country_code,
        COALESCE(REPLACE(Properties:"$email", '"', ''), REPLACE(Properties:"email", '"', '')) AS email,
        REPLACE(Properties:"firstName", '"', '') AS first_name,
        REPLACE(Properties:"lastName", '"', '') AS last_name,
        COALESCE(REPLACE(Properties:"User Id", '"', ''), REPLACE(Properties:"user ID", '"', '')) AS user_id,
        COALESCE(REPLACE(Properties:"User Handle", '"', ''), REPLACE(Properties:"handle", '"', '')) AS handle,
        COALESCE(REPLACE(Properties:"birthYear", '"', ''), REPLACE(Properties:"Birth year", '"', '')) AS birth_year,
        COALESCE(REPLACE(Properties:"gender", '"', ''), REPLACE(Properties:"Gender", '"', '')) AS gender,
        COALESCE(REPLACE(Properties:"$media_source", '"', ''), REPLACE(Properties:"media_source", '"', '')) AS media_source,
        COALESCE(REPLACE(Properties:"$campaign", '"', ''), REPLACE(Properties:"campaign", '"', '')) AS campaign,
        REPLACE(Properties:"ad", '"', '') AS ad,
        REPLACE(Properties:"adset", '"', '') AS adset,
        REPLACE(Properties:"utm_campaign", '"', '') AS utm_campaign,
        REPLACE(Properties:"utm_content", '"', '') AS utm_content,
        REPLACE(Properties:"utm_medium", '"', '') AS utm_medium,
        REPLACE(Properties:"urm_source", '"', '') AS utm_source,
        REPLACE(Properties:"Registration Date", '"', '') AS registration_date,
        REPLACE(Properties:"Registration Day", '"', '') AS registration_day,
        Properties:"Streaming Services" AS streaming_services,
        Properties:"genresScreenplay" AS genres_screenplay,
        Properties:"genresBooks" AS genres_books,
        Properties:"genresPodcasts" AS genres_podcasts,
        Properties:"subGenresScreenplay" AS sub_genres_screenplay,
        Properties:"subGenresBooks" AS sub_genres_books,
        Properties:"subGenresPodcasts" AS sub_genres_podcasts,
        Properties:"genresNewsletterScreenplay" AS genres_newsletter_screenplay,
        Properties:"genresNewsletterBooks"AS genres_newsletter_books,
        Properties:"genresNewsletterPodcasts" AS genres_newsletter_podcasts,
        Properties:"subGenresNewsletterScreenplay" AS sub_genres_newsletter_screenplay,
        Properties:"subGenresNewsletterBooks" AS sub_genres_newsletter_books,
        Properties:"subGenresNewsletterPodcasts" AS sub_genres_newsletter_podcasts,
        Properties:"genre" AS genre,
        REPLACE(Properties:"Phone", '"', '') AS phone,
        REPLACE(Properties:"phoneNumber", '"', '') AS phone_number,
        REPLACE(Properties:"dateOfBirth", '"', '') AS date_of_birth,
        REPLACE(Properties:"emailOptIn", '"', '') AS email_opt_in,
        REPLACE(Properties:"mlPredictChurnW2Priority", '"', '') AS ml_predict_churn_w2_priority,
        REPLACE(Properties:"anonUserId", '"', '') AS anon_user_id,
        REPLACE(Properties:"Registration Platform", '"', '') AS registration_platform,
        Properties:"Streaming Devices" AS streaming_devices,
        REPLACE(Properties:"dmi_lead_id", '"', '') AS dmi_lead_id,
        FROM {snow_db_name}.{snow_schema_name}.{snow_table_name};
        """
    return statement
