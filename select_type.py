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
TYPES_FILE = os.environ.get("TYPES_FILE")


def get_file_types_set():
    file_types = set()

    with open(TYPES_FILE) as input_file:
        for line in input_file:
            file_type = line.strip()

            if file_type != '':
                file_types.add(file_type)

    return file_types


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


def find_types(place, file_types):
    place_results = json.loads(place['results'])
    place_types = place_results.get('result').get('types')

    return bool(file_types.intersection(place_types))


def insert_find_result(connection, place_id, language, find_result):
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE `" + PLACE_DETAILS_TABLE + "` SET `find_result` = %s WHERE `place_id` = %s AND `language` = %s"
            cursor.execute(sql, (find_result, place_id, language))

        connection.commit()
    finally:
        pass


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

    file_types = get_file_types_set()
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM `" + PLACE_DETAILS_TABLE + "` ORDER BY `created_at`"

            if limit is not None:
                sql += " LIMIT "
                if offset is not None:
                    sql += str(offset) + ", "
                sql += str(limit)

            print("Execute SQL...", datetime.now())
            cursor.execute(sql)

            place = cursor.fetchone()
            while place is not None:
                place_id = place.get('place_id')
                language = place.get('language')
                print("Place ID:", place_id, "Language:", language,
                      datetime.now())

                if find_types(place, file_types):
                    insert_find_result(connection, place_id, language, 1)
                place = cursor.fetchone()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
