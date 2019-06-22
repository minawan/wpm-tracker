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

FIELD_NAMES = (ENTRY, DATE, WPM, HIGH)

StatRecord = namedtuple('StatRecord', ' '.join(FIELD_NAMES))

def generate_stat():
    high = 0
    stat = dict()
    for entry, img_file in enumerate(sorted(glob.glob('*.png')), 1):
        date = re.split(r'\.|_', img_file)[0]
        img = cv2.imread(img_file)
        img_content = pytesseract.image_to_string(img)
        wpm = int(re.search(r'(\d+) *WPM', img_content).group(1))
        high = max(high, wpm)
        stat[entry] = StatRecord(entry, date, wpm, high)
    return stat

def check_high_wpm(row):
    high = int(row[HIGH])
    wpm = int(row[WPM])
    if high < wpm:
        raise RecordError('EX8', 'high is less than wpm')

def check_positive(row):
    entry = int(row[ENTRY])
    wpm = int(row[WPM])
    high = int(row[HIGH])

    if entry <= 0:
        raise RecordError('EX9.1', 'entry is nonpositive')
    if wpm <= 0:
        raise RecordError('EX9.2', 'wpm is nonpositive')
    if high <= 0:
        raise RecordError('EX9.3', 'high is nonpositive')

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
    validator.add_record_check(check_high_wpm)
    validator.add_record_check(check_positive)
    return validator

def validate_stat(validator, filename, output_stream):
    # validate the data and write problems to stdout
    with open(filename, 'r') as input_csv_file:
        data = csv.reader(input_csv_file, delimiter=',')
        problems = validator.validate(data)
        write_problems(problems, output_stream)

def check_record(stat, row):
    actual = StatRecord(int(row[ENTRY]), row[DATE], int(row[WPM]), int(row[HIGH]))
    try:
        expected = stat[actual.entry]  # pylint: disable=no-member
        if actual != expected:
            raise RecordError('EX10', 'Row does not match the generated record.'
                              'Expected: {}, Actual: {}'.format(expected, actual))
    except KeyError:
        raise RecordError('EX10', 'Row not found among the generated records.')

def populate_db(stat_db, stat):
    for _, record in stat.items():
        stat_db.cursor().execute('''INSERT INTO stat(entry, date, wpm, high)
                                    VALUES(?, ?, ?, ?)''', record)

def check_record_db(stat_db, row):
    actual = StatRecord(int(row[ENTRY]), row[DATE], int(row[WPM]), int(row[HIGH]))
    cur = stat_db.cursor()
    cur.execute('SELECT * FROM stat WHERE entry=?',
                (actual.entry,))  # pylint: disable=no-member
    db_row = cur.fetchone()
    if not db_row:
        raise RecordError('EX11', 'Row not found among the generated records.')
    expected = StatRecord(*db_row)
    if actual != expected:
        raise RecordError('EX11', 'Row does not match the generated record.'
                          'Expected: {}, Actual: {}'.format(expected, actual))

def main(_):
    stat = generate_stat()
    validator = get_validator()
    validator.add_record_check(partial(check_record, stat))
    with sqlite3.connect(':memory:') as stat_db:
        stat_db.cursor().execute('''CREATE TABLE IF NOT EXISTS stat (
                                        entry integer PRIMARY KEY,
                                        date text,
                                        wpm integer,
                                        high integer
                                    );''')
        populate_db(stat_db, stat)
        validator.add_record_check(partial(check_record_db, stat_db))
        validate_stat(validator, FLAGS.stat_file, sys.stdout)

if __name__ == '__main__':
    app.run(main)
