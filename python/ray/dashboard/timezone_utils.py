from datetime import datetime
import tzlocal


def get_current_timezone_info():
    current_tz = datetime.now().astimezone().tzinfo
    offset = current_tz.utcoffset(None)
    hours, remainder = divmod(offset.total_seconds(), 3600)
    minutes = remainder // 60
    sign = "+" if hours >= 0 else "-"
    current_offset = f"{sign}{abs(int(hours)):02d}:{abs(int(minutes)):02d}"

    current_timezone = {"offset": current_offset, "value": tzlocal.get_localzone_name()}

    return current_timezone
