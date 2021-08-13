import enum
import json
import logging
from datetime import datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from typing import NamedTuple, Tuple, Type, Union

import dateutil.tz
import pytz


LOGGER = logging.getLogger(__name__)

NONE_TYPE = type(None)
NONE_TYPE_STR = str(NONE_TYPE)


# Collection types are used by some parsers, eg. in ercot_electrically_similar_nodes.
# The warehouse will only use this to encode parser type maps, not values.
# The warehouse currently does not support collection-typed values.
class TYPE_MAP_TYPES(enum.Enum):
    TUPLE = tuple
    LIST = list


class TYPES(enum.Enum):
    NONE = NONE_TYPE
    STR = str
    INT = int
    BOOL = bool
    FLOAT = float
    DECIMAL = Decimal
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
    Type[NONE_TYPE],
    Type[str],
    Type[int],
    Type[bool],
    Type[float],
    Type[Decimal],
    Type[datetime],
    Type[timedelta],
    Type[pytz.BaseTzInfo],
    Type[dateutil.tz.tz.tzoffset],
    Type[timezone],
]

TzTypes = Union[
    pytz.BaseTzInfo,
    dateutil.tz.tz.tzoffset,
    timezone,
]

ValueTypes = Union[
    NONE_TYPE,
    str,
    int,
    bool,
    float,
    Decimal,
    datetime,
    timedelta,
    TzTypes,
]

# constants
NAIVE_TAG = "Naive"


class Encoded(NamedTuple):
    val_str: str
    val_type: TYPES

    @staticmethod
    def deserialize(string: str) -> "Encoded":
        val_str, val_type = json.loads(string)
        return Encoded(val_str, TYPES[val_type])

    def serialize(self) -> str:
        return json.dumps([self.val_str, self.val_type.name])


def get_type(value: ValueTypes) -> TYPES:
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


def encode(value: ValueTypes) -> Encoded:
    """
    Encodes a value of the supported TYPES.

    Args:
        value: The value that is of the supported TYPES.

    Returns:
        The encode string and type wrapped in an Encoded named tuple.
    """
    val_type = get_type(value)

    if value is None:
        val_str = NONE_TYPE_STR
    elif isinstance(value, bool):
        val_str = str(int(value))
    elif isinstance(value, (str, int, float, Decimal)):
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
        raise TypeError(f"Missing encoder for type '{val_type}'. Please implement one.")

    return Encoded(val_str, val_type)


def decode(encoded: Encoded) -> ValueTypes:
    """
    Decodes an encoded value of one of the supported TYPES.

    Args:
        encoded: The Encoded named tuple which contains the encoded string and type.

    Returns:
        The decoded value.
    """
    _str, _type = encoded
    if not isinstance(_type, TYPES):
        raise ValueError(f"Type '{_type}' is invalid.")

    if _type == TYPES.NONE and _str != NONE_TYPE_STR:
        raise ValueError(
            f"Encoded string '{_str}' for 'TYPES.NONE' is invalid. "
            f"None types must be encoded as '{NONE_TYPE_STR}'"
        )

    decoded: ValueTypes
    if _str == NONE_TYPE_STR:
        decoded = None
    elif _type == TYPES.STR:
        decoded = _str
    elif _type == TYPES.INT:
        decoded = int(_str)
    elif _type == TYPES.FLOAT:
        decoded = float(_str)
    elif _type == TYPES.DECIMAL:
        decoded = Decimal(_str)
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
        raise TypeError(f"Missing decoder for type '{_type}'. Please implement one.")

    return decoded


def _get_fixed_utcoffset(tz: tzinfo) -> timedelta:
    """Helper method to get the utc offset from a fixed offset timezone.
    Warning: This should only be used with fixed offset timezones.
    """
    _dt = datetime(1111, 1, 1)  # just an arbitrary datetime object
    offset_delta = tz.utcoffset(_dt)  # _dt will be ignored
    if offset_delta is None:
        raise ValueError(f"Expected an offset delta from {tz} but got 'None'")
    return offset_delta
