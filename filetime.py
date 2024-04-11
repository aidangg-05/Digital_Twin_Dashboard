from calendar import timegm
from datetime import datetime, timezone, timedelta
import pandas as pd

EPOCH_AS_FILETIME = 116444736000000000  # January 1, 1970 as filetime
HUNDREDS_OF_NS = 10000000

def to_datetime(series: pd.Series) -> pd.Series:
	"""
	Converts a Windows filetime number to a Python datetime. The new
	datetime object is timezone-naive but is equivalent to tzinfo=utc.
	"""
	def convert(filetime):
        # Get seconds and remainder in terms of Unix epoch
        s, ns100 = divmod(filetime - EPOCH_AS_FILETIME, HUNDREDS_OF_NS)
        # Convert to datetime object, with remainder as microseconds.
        utc = datetime.utcfromtimestamp(s).replace(microsecond=(ns100 // 10))
        offset = timedelta(hours=8)
        local_time = utc + offset
        # Format the datetime to only show date and time without the year
        formatted_time = local_time.strftime("%d %B %H:%M:%S")  # Example format: "9th April 12:34:56"
        return formatted_time

    return series.apply(convert)
