#!/usr/bin/env python2

import sys
import csv
import glob
import re
from functools import partial
from collections import namedtuple
import sqlite3
from absl import app
from absl import flags
from csvvalidator import CSVValidator
from csvvalidator import RecordError
from csvvalidator import write_problems
import pytesseract
import cv2

FLAGS = flags.FLAGS
flags.DEFINE_string('stat_file', 'stat.txt', 'Stat file to validate')
flags.DEFINE_string('db_file', ':memory:', 'Stat database file to load')

ROWID = 'oid'
DATE = 'date'
WPM = 'wpm'
HIGH = 'high'

TABLE_NAME = 'stat'
FIELD_NAMES = (ROWID, DATE, WPM, HIGH)

SQL_CREATE_TABLE = '''CREATE TABLE IF NOT EXISTS {table} (
    {date} date NOT NULL CHECK ({date} IS strftime('%Y-%m-%d', {date})),
    {wpm} integer NOT NULL CHECK ({wpm} > 0),
    {high} integer CHECK ({high} > 0),
    CONSTRAINT check_running_max CHECK ({high} >= {wpm})
);'''.format(table=TABLE_NAME, date=DATE, wpm=WPM, high=HIGH)
SQL_CREATE_TRIGGER = '''CREATE TRIGGER IF NOT EXISTS running_max
    AFTER INSERT ON {table} FOR EACH ROW
    BEGIN
        UPDATE {table}
        SET {high} = NEW.{wpm}
        WHERE {rowid} = NEW.{rowid}
          AND {rowid} = 1;
        UPDATE {table}
        SET {high} = max(NEW.{wpm}, (SELECT high FROM {table} WHERE {rowid} = NEW.{rowid} - 1))
        WHERE {rowid} = NEW.{rowid}
          AND {rowid} > 1;
    END;'''.format(table=TABLE_NAME, rowid=ROWID, high=HIGH, wpm=WPM)
SQL_QUERY_TABLE_BY_ROWID = 'SELECT {rowid}, * FROM {table} WHERE {rowid}=?'.format(
    table=TABLE_NAME, rowid=ROWID)
SQL_INSERT = '''INSERT INTO {table}({date}, {wpm})
    VALUES(?, ?)'''.format(table=TABLE_NAME, date=DATE, wpm=WPM)

StatRecord = namedtuple('StatRecord', ' '.join(FIELD_NAMES))

def get_validator():
    validator = CSVValidator(FIELD_NAMES)
    # basic header and record length checks
    validator.add_header_check('EX1', 'bad header')
    validator.add_record_length_check('EX2', 'unexpected record length')
    return validator

def get_record_from_image(img_file):
    date = re.split(r'\.|_', img_file)[0]
    img_content = pytesseract.image_to_string(cv2.imread(img_file))
    wpm = int(re.search(r'(\d+) *WPM', img_content).group(1))
    return date, wpm

def get_record_from_db(stat_db, rowid):
    cur = stat_db.cursor()
    cur.execute(SQL_QUERY_TABLE_BY_ROWID, (rowid,))
    return cur.fetchone()

def check_record_db(stat_db, row):
    actual = StatRecord(int(row[ROWID]), row[DATE], int(row[WPM]), int(row[HIGH]))
    db_row = get_record_from_db(stat_db, getattr(actual, ROWID))
    if not db_row:
        raise RecordError('EX3', 'Row not found among the generated records.')
    expected = StatRecord(*db_row)
    if actual != expected:
        raise RecordError('EX4', 'Row does not match the generated record.'
                          'Expected: {}, Actual: {}'.format(expected, actual))

def main(_):
    validator = get_validator()
    with open(FLAGS.stat_file, 'r') as input_csv_file, \
         sqlite3.connect(FLAGS.db_file) as stat_db:
        stat_db.cursor().execute(SQL_CREATE_TABLE)
        stat_db.cursor().execute(SQL_CREATE_TRIGGER)
        for rowid, img_file in enumerate(sorted(glob.glob('*.png')), 1):
            if get_record_from_db(stat_db, rowid):
                continue
            stat_db.cursor().execute(SQL_INSERT, get_record_from_image(img_file))
        validator.add_record_check(partial(check_record_db, stat_db))
        data = csv.reader(input_csv_file, delimiter=',')
        problems = validator.validate(data)
        write_problems(problems, sys.stdout)

if __name__ == '__main__':
    app.run(main)
