import pandas as pd
import os
import sys
from read import get_json_reader
from write import load_db_table


def process_table(BASE_DIR, conn, table_name):
    json_reader = get_json_reader(BASE_DIR, table_name)
    for df in json_reader:
        load_db_table(df, conn, table_name, df.columns[0])


def main():
    BASE_DIR = r'C:\DataEngineer\data-engineering-essentials\Internal\bootcamp\data-copier\data\retail_db_json'
    configs = dict(os.environ.items())
    conn = f"postgresql://{configs['DB_USER']}:{configs['DB_PASSWORD']}@{configs['DB_HOST']}:{configs['DB_PORT']}/{configs['DB_NAME']}"

    table_names = sys.argv[1].split(',')
    for table_name in table_names:
        process_table(BASE_DIR, conn, table_name)


if __name__ == '__main__':
    main()
