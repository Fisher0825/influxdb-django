import datetime


def utc2local(utc_time_str):
    utc_date = datetime.datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
    local_date = utc_date + datetime.timedelta(hours=8)
    local_date_str = datetime.datetime.strftime(local_date, '%Y-%m-%d %H:%M:%S')
    return local_date_str


def utc2localtime(utc_time_str):
    utc_date = datetime.datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    local_date = utc_date + datetime.timedelta(hours=8)
    local_date_str = datetime.datetime.strftime(local_date, '%H:%M:%S')
    return local_date_str


def datetime2utc(datetime_or_str):
    if not isinstance(datetime_or_str, str) and isinstance(datetime_or_str, datetime.datetime):
        local_date_time = datetime_or_str
    else:
        local_date_time = datetime.datetime.strptime(datetime_or_str, "%Y-%m-%d %H:%M:%S")
    utc_data_time = local_date_time - datetime.timedelta(hours=8)
    utc_data_time_str = datetime.datetime.strftime(utc_data_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    return utc_data_time_str


def get_zero_today():
    now = datetime.datetime.now()
    # 获取今天零点
    zero_today = now - datetime.timedelta(hours=now.hour + 8, minutes=now.minute, seconds=now.second,
                                          microseconds=now.microsecond)
    return zero_today.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

