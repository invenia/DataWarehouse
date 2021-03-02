import enum
import json
import logging
from datetime import datetime, timedelta, timezone, tzinfo
from typing import NamedTuple, Tuple, Union

import dateutil.tz
import pytz


LOGGER = logging.getLogger(__name__)


class TYPES(enum.Enum):
    STR = str
    INT = int
    BOOL = bool
    FLOAT = float
    DATETIME = datetime
    TIMEDELTA = timedelta
    TZFILE_PYTZ = pytz.BaseTzInfo
    TZOFFSET_DATEUTIL = dateutil.tz.tz.tzoffset
    TZOFFSET_TZ = timezone
    # Adding these here for completeness sake, we don't need to support these types yet.
    # Note: Avoid using TZFILE_DATEUTIL, stick to using pytz instead which uses its own
    #   tzfile instead of the OS's. Most of our datetime utils are also written with
    #   pytz in mind.
    # TZFILE_DATEUTIL = dateutil.tz.tz.tzfile
    # TZOFFSET_PYTZ = pytz._FixedOffset


AllowedTypes = Union[
    str,
    int,
    bool,
    float,
    datetime,
    timedelta,
    pytz.BaseTzInfo,
    dateutil.tz.tz.tzoffset,
    timezone,
]


AllowedTZs = Union[
    pytz.BaseTzInfo,
    dateutil.tz.tz.tzoffset,
    timezone,
]


# constants
NAIVE_TAG = "Naive"


class Encoded(NamedTuple):
    val_str: str
    val_type: TYPES


def get_type(value: AllowedTypes) -> TYPES:
    """
    Gets the associated TYPES enum for the given value.

    Args:
        value: The value for which to determine the associated TYPES enum.

    Returns:
        The associated TYPES enum for the value.
    """
    if isinstance(value, pytz.BaseTzInfo):
        # Special case for pytz.BaseTzInfo subclasses, where direct Enum ref won't work.
        val_type = TYPES.TZFILE_PYTZ
    else:
        try:
            val_type = TYPES(type(value))
        except ValueError:
            LOGGER.error("Type '%s' for val '%s' is not supported.", type(value), value)
            raise
    return val_type


def encode(value: AllowedTypes) -> Encoded:
    """
    Encodes a value of the supported TYPES.

    Args:
        value: The value that is of the supported TYPES.

    Returns:
        The encode string and type wrapped in an Encoded named tuple.
    """
    val_type = get_type(value)

    if isinstance(value, bool):
        val_str = str(int(value))
    elif isinstance(value, (str, int, float)):
        val_str = str(value)
    elif isinstance(value, datetime):
        tz: Union[str, Tuple[str, str]]
        if value.tzinfo is None:
            tz = NAIVE_TAG
        else:
            tz_str, tz_type = encode(value.tzinfo)  # type: ignore
            tz = (tz_str, tz_type.name)
        val_str = json.dumps([value.isoformat(), tz])
    elif isinstance(value, timedelta):
        val_str = str(value.total_seconds())
    elif isinstance(value, pytz.BaseTzInfo):
        val_str = value.zone
    elif isinstance(value, dateutil.tz.tz.tzoffset):
        offset_secs = str(int(_get_fixed_utcoffset(value).total_seconds()))
        val_str = json.dumps([value.tzname(None), offset_secs])
    elif isinstance(value, timezone):
        val_str = str(int(_get_fixed_utcoffset(value).total_seconds()))
    else:
        raise TypeError(
            "Missing encoder for type '%s'. Please implement one.", val_type
        )

    return Encoded(val_str, val_type)


def decode(encoded: Encoded) -> AllowedTypes:
    """
    Decodes an encoded value of one of the supported TYPES.

    Args:
        encoded: The Encoded named tuple which contains the encoded string and type.

    Returns:
        The decoded value.
    """
    _str, _type = encoded
    if not isinstance(_type, TYPES):
        raise ValueError("Type '%s' is invalid.", _type)

    if _type == TYPES.STR:
        decoded = _str  # type: AllowedTypes
    elif _type == TYPES.INT:
        decoded = int(_str)
    elif _type == TYPES.FLOAT:
        decoded = float(_str)
    elif _type == TYPES.BOOL:
        decoded = bool(int(_str))
    elif _type == TYPES.DATETIME:
        dt_str, tz = json.loads(_str)
        decoded = datetime.fromisoformat(dt_str)
        if tz != NAIVE_TAG:
            tz = decode(Encoded(tz[0], TYPES[tz[1]]))
            if not isinstance(tz, tzinfo):
                raise ValueError("Decoded timezone has invalid type '%s'", type(tz))
            decoded = decoded.astimezone(tz)
    elif _type == TYPES.TIMEDELTA:
        decoded = timedelta(seconds=float(_str))
    elif _type == TYPES.TZFILE_PYTZ:
        decoded = pytz.timezone(_str)
    elif _type == TYPES.TZOFFSET_DATEUTIL:
        tzname, offset_secs = json.loads(_str)
        decoded = dateutil.tz.tzoffset(tzname, int(offset_secs))
    elif _type == TYPES.TZOFFSET_TZ:
        decoded = timezone(timedelta(seconds=int(_str)))
    else:
        raise TypeError("Missing decoder for type '%s'. Please implement one.", _type)

    return decoded


def _get_fixed_utcoffset(tz: tzinfo) -> timedelta:
    """Helper method to get the utc offset from a fixed offset timezone.
    Warning: This should only be used with fixed offset timezones.
    """
    _dt = datetime(1111, 1, 1)  # just an arbitrary datetime object
    offset_delta = tz.utcoffset(_dt)  # _dt will be ignored
    if offset_delta is None:
        raise ValueError("Expected an offset delta from %s but got 'None'", tz)
    return offset_delta
