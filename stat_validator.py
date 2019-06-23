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
from csvvalidator import datetime_string
from csvvalidator import write_problems
import pytesseract
import cv2

FLAGS = flags.FLAGS
flags.DEFINE_string('stat_file', 'stat.txt', 'Stat file to validate')
flags.DEFINE_string('db_file', ':memory:', 'Stat database file to load')

ENTRY = 'entry'
DATE = 'date'
WPM = 'wpm'
HIGH = 'high'

TABLE_NAME = 'stat'
FIELD_NAMES = (ENTRY, DATE, WPM, HIGH)

SQL_CREATE_TABLE = '''CREATE TABLE IF NOT EXISTS {table} (
    {entry} integer NOT NULL CHECK ({entry} > 0),
    {date} date NOT NULL CHECK ({date} IS strftime('%Y-%m-%d', {date})),
    {wpm} integer NOT NULL CHECK ({wpm} > 0),
    {high} integer NOT NULL CHECK ({high} > 0),
    PRIMARY KEY ({entry}),
    CONSTRAINT check_running_max CHECK ({high} >= {wpm})
);'''.format(table=TABLE_NAME, entry=ENTRY, date=DATE, wpm=WPM, high=HIGH)
SQL_QUERY_TABLE_BY_ENTRY = 'SELECT * FROM {} WHERE entry=?'.format(TABLE_NAME)
SQL_INSERT = '''INSERT INTO {}({}, {}, {}, {})
                VALUES(?, ?, ?, ?)'''.format(TABLE_NAME, *FIELD_NAMES)

StatRecord = namedtuple('StatRecord', ' '.join(FIELD_NAMES))

def get_validator():
    validator = CSVValidator(FIELD_NAMES)

    # basic header and record length checks
    validator.add_header_check('EX1', 'bad header')
    validator.add_record_length_check('EX2', 'unexpected record length')

    # some simple value checks
    validator.add_value_check(ENTRY, int, 'EX3', 'entry must be an integer')
    validator.add_value_check(DATE, datetime_string('%Y-%m-%d'),
                              'EX4', 'invalid date')
    validator.add_value_check(WPM, int, 'EX5', 'wpm must be an integer')
    validator.add_value_check(HIGH, int, 'EX6', 'high must be an integer')
    return validator

def get_record(stat_db, entry):
    cur = stat_db.cursor()
    cur.execute(SQL_QUERY_TABLE_BY_ENTRY, (entry,))
    return cur.fetchone()

def check_record_db(stat_db, row):
    actual = StatRecord(int(row[ENTRY]), row[DATE], int(row[WPM]), int(row[HIGH]))
    db_row = get_record(stat_db, actual.entry)  # pylint: disable=no-member
    if not db_row:
        raise RecordError('EX7', 'Row not found among the generated records.')
    expected = StatRecord(*db_row)
    if actual != expected:
        raise RecordError('EX8', 'Row does not match the generated record.'
                          'Expected: {}, Actual: {}'.format(expected, actual))

def main(_):
    validator = get_validator()
    with open(FLAGS.stat_file, 'r') as input_csv_file, \
         sqlite3.connect(FLAGS.db_file) as stat_db:
        stat_db.cursor().execute(SQL_CREATE_TABLE)
        high = 0
        for entry, img_file in enumerate(sorted(glob.glob('*.png')), 1):
            if get_record(stat_db, entry):
                continue
            date = re.split(r'\.|_', img_file)[0]
            img = cv2.imread(img_file)
            img_content = pytesseract.image_to_string(img)
            wpm = int(re.search(r'(\d+) *WPM', img_content).group(1))
            high = max(high, wpm)
            record = StatRecord(entry, date, wpm, high)
            stat_db.cursor().execute(SQL_INSERT, record)
        validator.add_record_check(partial(check_record_db, stat_db))
        data = csv.reader(input_csv_file, delimiter=',')
        problems = validator.validate(data)
        write_problems(problems, sys.stdout)

if __name__ == '__main__':
    app.run(main)
