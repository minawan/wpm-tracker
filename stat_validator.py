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

ENTRY = 'entry'
DATE = 'date'
WPM = 'wpm'
HIGH = 'high'

TABLE_NAME = 'stat'
FIELD_NAMES = (ENTRY, DATE, WPM, HIGH)

SQL_CREATE_TABLE = '''CREATE TABLE IF NOT EXISTS {} (
                          {} integer PRIMARY KEY,
                          {} text,
                          {} integer,
                          {} integer
                      );'''.format(TABLE_NAME, *FIELD_NAMES)
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
    check_positive = lambda n: int(n) > 0
    validator.add_value_check(ENTRY, int, 'EX3', 'entry must be an integer')
    validator.add_value_predicate(ENTRY, check_positive, code='EX9.1',
                                  message='entry is nonpositive')
    validator.add_value_check(DATE, datetime_string('%Y-%m-%d'),
                              'EX4', 'invalid date')
    validator.add_value_check(WPM, int, 'EX5', 'wpm must be an integer')
    validator.add_value_predicate(WPM, check_positive, code='EX9.2',
                                  message='wpm is nonpositive')
    validator.add_value_check(HIGH, int, 'EX6', 'high must be an integer')
    validator.add_value_predicate(HIGH, check_positive, code='EX9.3',
                                  message='high is nonpositive')
    check_high_wpm = lambda row: int(row[HIGH]) >= int(row[WPM])
    validator.add_record_predicate(check_high_wpm, code='EX8',
                                   message='high is less than wpm')
    return validator

def check_record_db(stat_db, row):
    actual = StatRecord(int(row[ENTRY]), row[DATE], int(row[WPM]), int(row[HIGH]))
    cur = stat_db.cursor()
    cur.execute(SQL_QUERY_TABLE_BY_ENTRY,
                (actual.entry,))  # pylint: disable=no-member
    db_row = cur.fetchone()
    if not db_row:
        raise RecordError('EX11', 'Row not found among the generated records.')
    expected = StatRecord(*db_row)
    if actual != expected:
        raise RecordError('EX11', 'Row does not match the generated record.'
                          'Expected: {}, Actual: {}'.format(expected, actual))

def main(_):
    validator = get_validator()
    with open(FLAGS.stat_file, 'r') as input_csv_file, \
         sqlite3.connect(':memory:') as stat_db:
        stat_db.cursor().execute(SQL_CREATE_TABLE)
        high = 0
        for entry, img_file in enumerate(sorted(glob.glob('*.png')), 1):
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
