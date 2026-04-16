"""
时区工具类

提供统一的时区处理功能，确保所有时间字段使用 UTC+8 时区
"""

from datetime import datetime, timezone, timedelta


# 定义 UTC+8 时区
UTC_8 = timezone(timedelta(hours=8))


def get_now_utc8() -> datetime:
    """
    获取当前 UTC+8 时区的时间
    
    Returns:
        datetime: UTC+8 时区的当前时间
    """
    return datetime.now(UTC_8)


def to_utc8(dt: datetime) -> datetime:
    """
    将 datetime 转换为 UTC+8 时区
    
    Args:
        dt: 原始 datetime 对象
        
    Returns:
        datetime: UTC+8 时区的时间
    """
    if dt.tzinfo is None:
        # 如果没有时区信息，假设是 UTC+0
        dt = dt.replace(tzinfo=timezone.utc)
    
    return dt.astimezone(UTC_8)
