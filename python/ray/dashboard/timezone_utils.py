import logging
from datetime import datetime

logger = logging.getLogger(__name__)

timezones = [
    {"offset": "-12:00", "value": "Etc/+12"},
    {"offset": "-11:00", "value": "Pacific/Pago_Pago"},
    {"offset": "-10:00", "value": "Pacific/Honolulu"},
    {"offset": "-09:00", "value": "America/Anchorage"},
    {"offset": "-08:00", "value": "America/Los_Angeles"},
    {"offset": "-07:00", "value": "America/Phoenix"},
    {"offset": "-06:00", "value": "America/Guatemala"},
    {"offset": "-05:00", "value": "America/Bogota"},
    {"offset": "-04:00", "value": "America/Halifax"},
    {"offset": "-03:30", "value": "America/St_Johns"},
    {"offset": "-03:00", "value": "America/Sao_Paulo"},
    {"offset": "-02:00", "value": "America/Godthab"},
    {"offset": "-01:00", "value": "Atlantic/Azores"},
    {"offset": "+00:00", "value": "Europe/London"},
    {"offset": "+01:00", "value": "Europe/Amsterdam"},
    {"offset": "+02:00", "value": "Asia/Amman"},
    {"offset": "+03:00", "value": "Asia/Baghdad"},
    {"offset": "+03:30", "value": "Asia/Tehran"},
    {"offset": "+04:00", "value": "Asia/Dubai"},
    {"offset": "+04:30", "value": "Asia/Kabul"},
    {"offset": "+05:00", "value": "Asia/Karachi"},
    {"offset": "+05:30", "value": "Asia/Kolkata"},
    {"offset": "+05:45", "value": "Asia/Kathmandu"},
    {"offset": "+06:00", "value": "Asia/Almaty"},
    {"offset": "+06:30", "value": "Asia/Yangon"},
    {"offset": "+07:00", "value": "Asia/Bangkok"},
    {"offset": "+08:00", "value": "Asia/Shanghai"},
    {"offset": "+09:00", "value": "Asia/Irkutsk"},
    {"offset": "+09:30", "value": "Australia/Adelaide"},
    {"offset": "+10:00", "value": "Australia/Brisbane"},
    {"offset": "+11:00", "value": "Asia/Magadan"},
    {"offset": "+12:00", "value": "Pacific/Auckland"},
    {"offset": "+13:00", "value": "Pacific/Tongatapu"},
]


def get_current_timezone_info():
    current_tz = datetime.now().astimezone().tzinfo
    offset = current_tz.utcoffset(None)
    hours, remainder = divmod(offset.total_seconds(), 3600)
    minutes = remainder // 60
    sign = "+" if hours >= 0 else "-"
    current_offset = f"{sign}{abs(int(hours)):02d}:{abs(int(minutes)):02d}"

    current_timezone = next(
        (tz for tz in timezones if tz["offset"] == current_offset),
        {"offset": None, "value": None},
    )

    return current_timezone
