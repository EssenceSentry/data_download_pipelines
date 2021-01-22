import csv
import os
import re
import sys
import time

import boto3
from boto3.dynamodb.conditions import Key
from download_pipelines.helper_utils import progress_bar
from download_pipelines.logging_utils import set_logger
from sqlalchemy import create_engine


logger = set_logger(__name__)


def mysql_query(connection_string, query):
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        proxy = connection.execute(query)
    return [item for item in proxy]


def get_column_names(db_connection_str, table):
    columns_query = f"SELECT COLUMN_NAME col FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table}'"
    return list(
        sorted((t[0] for t in mysql_query(db_connection_str, columns_query)),
               key=lambda x: x.lower()))


def get_column(db_connection_str, column_name, count_by_column, table):
    query = f"SELECT {column_name}, COUNT({count_by_column}) FROM buyerMLS_dev_backup.{table} GROUP BY {column_name} " \
            f"ORDER BY 2 DESC LIMIT 1000"
    return mysql_query(db_connection_str, query)


def columns_to_csv(db_connection_str, table, count_column, file_path):
    column_names = get_column_names(db_connection_str, table)
    logger.info("Columns: [%s]" % ", ".join(column_names))
    column_list = []
    length = len(column_names)
    before, after, deltas, str_len = 0, 0, [], 0
    for i, column in enumerate(column_names):
        deltas, str_len = progress_bar(i, length, before, after, deltas,
                                       str_len, column)
        before = time.time()
        items = get_column(db_connection_str, column, count_column, table)
        values = [t[0] for t in items]
        column_list.append(values)
        after = time.time()
    progress_bar(length, length)
    to_csv(column_names, column_list, file_path)


def to_csv(column_names, column_list, file_path):
    length = max(len(c) for c in column_list)

    def get(_list, j, default=""):
        try:
            return str(_list[j])
        except IndexError:
            return default

    logger.info("Saving to %s" % file_path)
    with open(file_path, "w") as f:
        writer = csv.writer(f,
                            delimiter="\t",
                            quotechar="\"",
                            quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(column_names)
        deltas, str_len = [], 0
        for i in range(length):
            writer.writerow(get(c, i) for c in column_list)
    logger.info("Saved to file://%s" % file_path)


def download_tables_to_csv_file(tables=[("mls_listings", "ListingID")],
                                path=""):
    connection_str = os.getenv("DB_CONNECTION_STRING")
    for table, count_column in tables:
        columns_to_csv(connection_str, table, count_column,
                       f"{path}{table}_db.csv")


def config_from_dynamo(vendor, brokerage=None):
    def establish_connection():
        session = boto3.session.Session(region_name="us-east-2")
        dynamodb = session.resource("dynamodb")
        return dynamodb.Table('STAGING_VendorSDKConfigs')

    vendor_condition = Key('vendor').eq(vendor.upper())
    key_condition = vendor_condition & Key('brokerage').eq(
        brokerage.upper()) if brokerage else vendor_condition
    table = establish_connection()
    sys.stderr.flush()
    return table.query(KeyConditionExpression=key_condition)


def download_configs_from_dynamo(vendors, path):
    data = [{
        vendor: config_from_dynamo(vendor).get("Items")
    } for vendor in vendors]
    string = str(data)
    string = re.sub(r"'", r'"', string)
    string = re.sub(r"\{((\"\w+\",?\s*)*)}", r"[\1]", string)
    string = re.sub(r"True", r"true", string)
    string = re.sub(r"False", r"false", string)

    with open(f"{path}configs.json", "w") as f:
        f.write(string)

    return data
