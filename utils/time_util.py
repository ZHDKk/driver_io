from datetime import datetime

from logger import log


def get_milliseconds():
    """
    获取当前本地时间的毫秒数
    """
    return int(datetime.now().timestamp() * 1000)


def get_current_time():
    """
        获取当前本地时间%Y-%m-%d %H:%M:%S.%f
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def uint32_time_parse(timestamp, format):
    """
        timestamp：原始时间戳
        timestamp：需要转换的时间格式，比如（"%Y-%m-%d %H:%M:%S"）
    """
    try:
        # 获取当前日期，并将时间设置为00:00:00
        midnight_today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # 将datetime对象转换为时间戳（秒级）
        timestamp_seconds = midnight_today.timestamp()
        # 将秒级时间戳转换为毫秒时间戳
        timestamp_milliseconds = int(timestamp_seconds * 1000)
        now_time = timestamp_milliseconds + timestamp
        # 使用datetime.fromtimestamp()将秒级时间戳转换为datetime对象
        dt_object = datetime.fromtimestamp(now_time // 1000)
        # 使用strftime()方法将datetime对象格式化为字符串
        formatted_time = dt_object.strftime(format)
        # print("处理后的时间:", formatted_time)
        return formatted_time
    except Exception as e:
        log.warning(f"时间处理出错{e}，原始时间UInt32:{timestamp}")
        return None


def millis_2_time(milliseconds):
    """
        毫秒转时间
    """
    if not milliseconds:
        return
    seconds = milliseconds / 1000.0
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    # time_string = f"{int(hours):02}:{int(minutes):02}:{seconds:06.3f}"[:8]  # 格式化为HH:MM:SS.mmm
    time_string_no_millis = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"  # 格式化为HH:MM:SS
    return time_string_no_millis

def filter_timestamp(ts: str) -> str:
    """
    如果 ts 的年份 < 2025，则返回空字符串；否则返回原 ts。
    ts 格式假定为 'YYYY-MM-DD hh:mm:ss.xxx'
    """
    try:
        year = int(ts[:4])
    except (ValueError, TypeError):
        # 如果无法解析年份，也返回空
        return ""
    return ts if year >= 2025 else ""
