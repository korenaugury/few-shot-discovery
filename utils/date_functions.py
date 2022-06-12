import calendar
import copy
import time
import re
import pandas as pd
from datetime import datetime, timedelta


def get_current_timestamp():
    return datetime.now().timestamp()


def get_current_date():
    return datetime_to_str(datetime.today())


def time_string_to_timestamp(time_string):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M').timestamp()


def full_time_string_to_timestamp(time_string):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S').timestamp()


def timestamp_to_date_string(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')


def timestamp_to_full_date_string(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def get_current_utc_timestamp():
    return datetime.utcnow().timestamp()


def get_current_utc_date():
    return datetime.utcnow().strftime('%Y-%m-%d')


def utc_time_string_to_timestamp(utc_time_string):
    to_utc_delta = get_current_timestamp() - get_current_utc_timestamp()
    return datetime.strptime(utc_time_string, '%Y-%m-%d %H:%M').timestamp() + to_utc_delta


def utc_full_time_string_to_timestamp(utc_time_string):
    to_utc_delta = get_current_timestamp() - get_current_utc_timestamp()
    return datetime.strptime(utc_time_string, '%Y-%m-%d %H:%M:%S').timestamp() + to_utc_delta


def timestamp_to_utc_date_string(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')


def timestamp_to_utc_full_date_string(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def move_date(date_str, days_delta):

    datetime_obj = _get_datetime_obj(date_str)

    return datetime_to_str(datetime_obj + timedelta(days=days_delta))

def calc_days_delta(start_date, end_date):

    start_date = _get_datetime_obj(start_date)
    end_date = _get_datetime_obj(end_date)

    return (end_date - start_date).days

def datetime_to_str(datetime_obj):
    return _get_datetime_obj(datetime_obj).strftime("%Y-%m-%d")

def str_datetime_to_timestamp(datetime_str):
    return int(datetime.timestamp(datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')))

def str_to_datetime(date_str):
    date_format = '%Y-%m-%d %H:%M:%S'
    if len(date_str) <= 10:
        date_format = '%Y-%m-%d'

    return datetime.strptime(copy.deepcopy(date_str), date_format)

def _get_datetime_obj(datetime_or_str):

    if type(datetime_or_str) == datetime:
        return datetime_or_str
    return str_to_datetime(datetime_or_str)

def date_string_to_timestamp(date_str):
    return time.mktime(str_to_datetime(date_str).timetuple())

def extract_month_from_date(date_str):
    return str_to_datetime(date_str).month

def extract_year_from_date(date_str):
    return str_to_datetime(date_str).year

def extract_weekday_from_date(date_str):
    return calendar.day_name[str_to_datetime(date_str).weekday()]

def calc_all_dates_between_two_dates(start_date_str, end_date_str):

    start_date = str_to_datetime(start_date_str)
    end_date = str_to_datetime(end_date_str)

    delta = end_date - start_date

    dates = []
    for days_to_move in range(delta.days + 1):
        dates.append(
            datetime_to_str(start_date + timedelta(days=days_to_move))
        )

    return dates

def extract_years_in_date_range(start_date_str, end_date_str):

    years = set()

    for date_str in calc_all_dates_between_two_dates(start_date_str, end_date_str):
        years.add(extract_year_from_date(date_str))

    return list(years)

def extract_months_in_date_range(start_date_str, end_date_str):

    months = set()

    for date_str in calc_all_dates_between_two_dates(start_date_str, end_date_str):
        months.add(extract_month_from_date(date_str))

    return list(months)

def get_days_between(start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    return abs((end_date - start_date).days)


def fix_date_str_format(date_str: str):
    if pd.isnull(date_str):
        return

    wrong_date_str_format = re.compile('.{2}/.{2}/.{4}')
    wrong_datetime_str_format = re.compile('.{2}/.{2}/.{4} .*:.*')
    wrong_datetime_with_sec_str_format = re.compile('.{2}/.{2}/.{4} .*:.*:.*')

    right_date_str_format = re.compile('.{4}-.{2}-.{2}')
    right_datetime_str_format = re.compile('.{4}-.{2}-.{2} .*:.*')
    right_datetime_with_sec_str_format = re.compile('.{4}-.{2}-{2} .*:.*:.*')

    if wrong_date_str_format.match(date_str):
        if wrong_date_str_format.match(date_str).regs[0][1] == len(date_str):
            return date_str[6:10] + '-' + date_str[3:5] + '-' + date_str[0:2]
    if (wrong_datetime_str_format.match(date_str) is not None) or \
            (wrong_datetime_with_sec_str_format.match(date_str) is not None):
        return date_str[6:10] + '-' + date_str[3:5] + '-' + date_str[0:2] + date_str[10:]

    if (right_date_str_format.match(date_str) is None) \
            and (right_datetime_str_format.match(date_str) is None) \
            and (right_datetime_with_sec_str_format.match(date_str) is None):
        print(f"Can't fix date_str format for {date_str}")
    return date_str




