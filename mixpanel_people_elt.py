import mp_people_extract_to_s3 as extract
import mp_people_load_to_snowflake as load


def main():
    extract.main()
    load.main()


if __name__ == "__main__":
    main()
