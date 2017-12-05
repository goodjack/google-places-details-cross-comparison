import argparse
import json
import os
import sys
from datetime import datetime
from os.path import dirname, join

import pymysql.cursors
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path, override=True)

PLACE_DETAILS_LANG = os.environ.get("PLACE_DETAILS_LANG")
PLACE_DETAILS_TABLE = os.environ.get("PLACE_DETAILS_TABLE")


def get_mysql_connection():
    MYSQL_HOST = os.environ.get("MYSQL_HOST")
    MYSQL_USER = os.environ.get("MYSQL_USER")
    MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
    MYSQL_DB = os.environ.get("MYSQL_DB")
    MYSQL_CHARSET = os.environ.get("MYSQL_CHARSET")

    connection = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        charset=MYSQL_CHARSET,
        cursorclass=pymysql.cursors.DictCursor)

    return connection


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', help='The limit order by created_at')
    parser.add_argument('-o', help='The offset order by created_at')
    args = parser.parse_args()

    if args.l is not None:
        limit = int(args.l)
    else:
        limit = None

    if args.o is not None:
        offset = int(args.o)
    else:
        offset = None

    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM " + PLACE_DETAILS_TABLE + " ORDER BY `created_at`"

            if limit is not None:
                sql += " LIMIT "
                if offset is not None:
                    sql += str(offset) + ", "
                sql += str(limit)

            cursor.execute(sql)
            place = cursor.fetchone()
            while place is not None:
                # TODO: do something...
                place = cursor.fetchone()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
