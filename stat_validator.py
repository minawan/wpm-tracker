#!/usr/bin/env python2

import sys
import csv
import cv2
import glob
import pytesseract
import re
from collections import namedtuple
from csvvalidator import CSVValidator
from csvvalidator import RecordError
from csvvalidator import datetime_string
from csvvalidator import write_problems

ENTRY = 'entry'
DATE = 'date'
WPM = 'wpm'
HIGH = 'high'

field_names = (ENTRY, DATE, WPM, HIGH)

StatRecord = namedtuple('StatRecord', ' '.join(field_names))

def generate_stat():
  print ','.join(field_names)
  high = 0
  stat = dict()
  for entry, img_file in enumerate(sorted(glob.glob('*.png')), 1):
    date = img_file.split('.')[0].split('_')[0] #re.split('._', img_file)
    img_content = pytesseract.image_to_string(cv2.imread(img_file)).split('\n')
    wpm = int(''.join(ch for ch in next(line for line in img_content if line.find('WPM') != -1) if ch.isdigit()))
    high = max(high, wpm)
    stat[entry] = StatRecord(entry, date, wpm, high)
  return stat

def check_high_wpm(row):
    high = int(row[HIGH])
    wpm = int(row[WPM])
    if high < wpm:
        raise RecordError('EX8', 'high is less than wpm')

def get_validator():
  validator = CSVValidator(field_names)

  # basic header and record length checks
  validator.add_header_check('EX1', 'bad header')
  validator.add_record_length_check('EX2', 'unexpected record length')

  # some simple value checks
  validator.add_value_check(ENTRY, int,
                            'EX3', 'entry must be an integer')
  validator.add_value_check(DATE, datetime_string('%Y-%m-%d'),
                            'EX4', 'invalid date')
  validator.add_value_check(WPM, int,
                            'EX5', 'wpm must be an integer')
  validator.add_value_check(HIGH, int,
                            'EX6', 'high must be an integer')
  validator.add_record_check(check_high_wpm)
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
    expected = stat[actual.entry]
    if actual != expected:
      raise RecordError('EX9', 'Row does not match the generated record. Expected: {}, Actual: {}'.format(expected, actual))
  except KeyError:
    raise RecordError('EX10', 'Row not found among the generated records.')

def main():
  global stat
  stat = generate_stat()
  validator = get_validator()
  validator.add_record_check(lambda row: check_record(stat, row))
  validate_stat(validator, sys.argv[1], sys.stdout)

if __name__ == '__main__':
  main()
