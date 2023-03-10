from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import dateutil.tz
import pytest
import pytz

from datawarehouse import types


class TestDataTypes:
    def test_get_type(self):
        supported_input = [
            "",
            1,
            True,
            1.2,
            Decimal("1.1111"),
            datetime(1111, 2, 3, tzinfo=timezone.utc),
            timedelta(0),
            pytz.timezone("Zulu"),
            dateutil.tz.tzoffset("UTC-5", -18000),
            timezone(timedelta(hours=4)),
            None,
        ]

        expected_types = [
            types.TYPES.STR,
            types.TYPES.INT,
            types.TYPES.BOOL,
            types.TYPES.FLOAT,
            types.TYPES.DECIMAL,
            types.TYPES.DATETIME,
            types.TYPES.TIMEDELTA,
            types.TYPES.TZFILE_PYTZ,
            types.TYPES.TZOFFSET_DATEUTIL,
            types.TYPES.TZOFFSET_TZ,
            types.TYPES.NONE,
        ]

        for i in range(len(supported_input)):
            assert types.get_type(supported_input[i]) == expected_types[i]

    def test_int(self):
        decoded = [
            0,
            987,
            2147483647,
            -55,
            -2147483647,
        ]
        encoded = [
            types.Encoded("0", types.TYPES.INT),
            types.Encoded("987", types.TYPES.INT),
            types.Encoded("2147483647", types.TYPES.INT),
            types.Encoded("-55", types.TYPES.INT),
            types.Encoded("-2147483647", types.TYPES.INT),
        ]

        for i in range(len(decoded)):
            assert types.encode(decoded[i]) == encoded[i]
            assert types.decode(encoded[i]) == decoded[i]

    def test_float(self):
        decoded = [
            0.0,
            5.0,
            987.123,
            2147483647.456,
            -9.0,
            -55.9,
            -2147483647.234,
        ]
        encoded = [
            types.Encoded("0.0", types.TYPES.FLOAT),
            types.Encoded("5.0", types.TYPES.FLOAT),
            types.Encoded("987.123", types.TYPES.FLOAT),
            types.Encoded("2147483647.456", types.TYPES.FLOAT),
            types.Encoded("-9.0", types.TYPES.FLOAT),
            types.Encoded("-55.9", types.TYPES.FLOAT),
            types.Encoded("-2147483647.234", types.TYPES.FLOAT),
        ]

        for i in range(len(decoded)):
            assert types.encode(decoded[i]) == encoded[i]
            assert types.decode(encoded[i]) == decoded[i]

    def test_decimal(self):
        decoded = [
            Decimal("0.0"),
            Decimal("-0"),
            Decimal("5"),
            Decimal("43798.9823475"),
            Decimal("0.001"),
            Decimal("-9"),
            Decimal("-78.1"),
            Decimal("1234567890.1234567890"),
            Decimal("0.00010000"),
            Decimal("1E+99"),
            Decimal("8.6545E-38"),
            Decimal("Infinity"),
            Decimal("-Infinity"),
        ]
        encoded = [
            types.Encoded("0.0", types.TYPES.DECIMAL),
            types.Encoded("-0", types.TYPES.DECIMAL),
            types.Encoded("5", types.TYPES.DECIMAL),
            types.Encoded("43798.9823475", types.TYPES.DECIMAL),
            types.Encoded("0.001", types.TYPES.DECIMAL),
            types.Encoded("-9", types.TYPES.DECIMAL),
            types.Encoded("-78.1", types.TYPES.DECIMAL),
            types.Encoded("1234567890.1234567890", types.TYPES.DECIMAL),
            types.Encoded("0.00010000", types.TYPES.DECIMAL),
            types.Encoded("1E+99", types.TYPES.DECIMAL),
            types.Encoded("8.6545E-38", types.TYPES.DECIMAL),
            types.Encoded("Infinity", types.TYPES.DECIMAL),
            types.Encoded("-Infinity", types.TYPES.DECIMAL),
        ]

        for i in range(len(decoded)):
            assert types.encode(decoded[i]) == encoded[i]
            assert types.decode(encoded[i]) == decoded[i]

        decoded = Decimal("NaN")
        encoded = types.Encoded("NaN", types.TYPES.DECIMAL)

        assert types.encode(decoded) == encoded
        assert types.decode(encoded).is_nan()

    def test_str(self):
        decoded = [
            "",
            "0.0",
            "2147483647.456",
            "123",
            "True",
            "None",
            "2020-01-01",
        ]
        encoded = [
            types.Encoded("", types.TYPES.STR),
            types.Encoded("0.0", types.TYPES.STR),
            types.Encoded("2147483647.456", types.TYPES.STR),
            types.Encoded("123", types.TYPES.STR),
            types.Encoded("True", types.TYPES.STR),
            types.Encoded("None", types.TYPES.STR),
            types.Encoded("2020-01-01", types.TYPES.STR),
        ]

        for i in range(len(decoded)):
            assert types.encode(decoded[i]) == encoded[i]
            assert types.decode(encoded[i]) == decoded[i]

    def test_bool(self):
        decoded = [
            True,
            False,
        ]
        encoded = [
            types.Encoded("1", types.TYPES.BOOL),
            types.Encoded("0", types.TYPES.BOOL),
        ]

        for i in range(len(decoded)):
            assert types.encode(decoded[i]) == encoded[i]
            assert types.decode(encoded[i]) == decoded[i]

    def test_timedelta(self):
        decoded = [
            timedelta(days=4),
            timedelta(hours=1),
            timedelta(hours=-24),
            timedelta(minutes=6),
            timedelta(seconds=-3),
            timedelta(milliseconds=23),
            timedelta(seconds=53, milliseconds=-443),
            timedelta(0),
        ]
        encoded = [
            types.Encoded(str(float(4 * 24 * 60 * 60)), types.TYPES.TIMEDELTA),
            types.Encoded(str(float(1 * 60 * 60)), types.TYPES.TIMEDELTA),
            types.Encoded(str(float(-24 * 60 * 60)), types.TYPES.TIMEDELTA),
            types.Encoded(str(float(6 * 60)), types.TYPES.TIMEDELTA),
            types.Encoded(str(float(-3)), types.TYPES.TIMEDELTA),
            types.Encoded(str(float(23 * 0.001)), types.TYPES.TIMEDELTA),
            types.Encoded(str(float(53 - 443 * 0.001)), types.TYPES.TIMEDELTA),
            types.Encoded(str(float(0)), types.TYPES.TIMEDELTA),
        ]

        for i in range(len(decoded)):
            assert types.encode(decoded[i]) == encoded[i]
            assert types.decode(encoded[i]) == decoded[i]

    def test_datetime(self):
        decoded = [
            datetime(1910, 12, 31, 23, 59, 59),
            datetime(1910, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            datetime(1910, 12, 31, 23, 59, 59, 123, tzinfo=timezone.utc),
            datetime(2020, 1, 1, 12, tzinfo=timezone(timedelta(hours=4))),
            datetime(2020, 1, 1, 12, tzinfo=timezone(timedelta(hours=-4))),
            pytz.timezone("America/New_York").localize(datetime(2020, 1, 1, 12)),
            datetime(2020, 1, 1, 12, tzinfo=dateutil.tz.tzoffset("UTC-5", -18000)),
        ]

        tp = types.TYPES.DATETIME
        encoded = [
            types.Encoded('["1910-12-31T23:59:59", "Naive"]', tp),
            types.Encoded('["1910-12-31T23:59:59+00:00", ["0", "TZOFFSET_TZ"]]', tp),
            types.Encoded(
                '["1910-12-31T23:59:59.000123+00:00", ["0", "TZOFFSET_TZ"]]', tp
            ),
            types.Encoded(
                '["2020-01-01T12:00:00+04:00", ["14400", "TZOFFSET_TZ"]]', tp
            ),
            types.Encoded(
                '["2020-01-01T12:00:00-04:00", ["-14400", "TZOFFSET_TZ"]]', tp
            ),
            types.Encoded(
                '["2020-01-01T12:00:00-05:00", ["America/New_York", "TZFILE_PYTZ"]]', tp
            ),
            types.Encoded(
                # consequence of nested json.dumps
                '["2020-01-01T12:00:00-05:00", ["[\\"UTC-5\\", \\"-18000\\"]", "TZOFFSET_DATEUTIL"]]',  # noqa E501
                tp,
            ),
        ]

        for i in range(len(decoded)):
            assert types.encode(decoded[i]) == encoded[i]
            assert types.decode(encoded[i]) == decoded[i]

    def test_date(self):
        decoded = [
            date(1348, 1, 25),
            date(1727, 5, 31),
            date(438, 2, 15),
            date(2033, 8, 1),
        ]

        encoded = [
            types.Encoded("1348-01-25", types.TYPES.DATE),
            types.Encoded("1727-05-31", types.TYPES.DATE),
            types.Encoded("0438-02-15", types.TYPES.DATE),
            types.Encoded("2033-08-01", types.TYPES.DATE),
        ]

        for i in range(len(decoded)):
            assert types.encode(decoded[i]) == encoded[i]
            assert types.decode(encoded[i]) == decoded[i]

    def test_tzinfo(self):
        decoded = [
            pytz.utc,
            pytz.timezone("Zulu"),
            pytz.timezone("America/New_York"),
            pytz.timezone("America/Chicago"),
            pytz.timezone("America/Los_Angeles"),
            dateutil.tz.tzoffset("UTC", 0),
            dateutil.tz.tzoffset("A", -3600),
            dateutil.tz.tzoffset("B", 3600),
            dateutil.tz.tzoffset("UTC-5", -18000),
            dateutil.tz.tzoffset("UTC+03:00", 10800),
            timezone.utc,
            timezone(timedelta(0)),
            timezone(timedelta(hours=2)),
            timezone(timedelta(hours=-2)),
        ]
        encoded = [
            types.Encoded("UTC", types.TYPES.TZFILE_PYTZ),
            types.Encoded("Zulu", types.TYPES.TZFILE_PYTZ),
            types.Encoded("America/New_York", types.TYPES.TZFILE_PYTZ),
            types.Encoded("America/Chicago", types.TYPES.TZFILE_PYTZ),
            types.Encoded("America/Los_Angeles", types.TYPES.TZFILE_PYTZ),
            types.Encoded('["UTC", "0"]', types.TYPES.TZOFFSET_DATEUTIL),
            types.Encoded('["A", "-3600"]', types.TYPES.TZOFFSET_DATEUTIL),
            types.Encoded('["B", "3600"]', types.TYPES.TZOFFSET_DATEUTIL),
            types.Encoded('["UTC-5", "-18000"]', types.TYPES.TZOFFSET_DATEUTIL),
            types.Encoded('["UTC+03:00", "10800"]', types.TYPES.TZOFFSET_DATEUTIL),
            types.Encoded("0", types.TYPES.TZOFFSET_TZ),
            types.Encoded("0", types.TYPES.TZOFFSET_TZ),
            types.Encoded("7200", types.TYPES.TZOFFSET_TZ),
            types.Encoded("-7200", types.TYPES.TZOFFSET_TZ),
        ]

        _dt = datetime(1111, 1, 1)  # arbitrary datetime
        for i in range(len(decoded)):
            assert types.encode(decoded[i]) == encoded[i]
            assert types.decode(encoded[i]) == decoded[i]
            # Additionally check user defined tznames to make sure they match.
            if isinstance(decoded[i], types.TYPES.TZOFFSET_DATEUTIL.value):
                assert types.decode(encoded[i]).tzname(_dt) == decoded[i].tzname(_dt)

    def test_none(self):
        # test encoding a None value
        encoded = types.Encoded(types.NONE_TYPE_STR, types.TYPES.NONE)
        assert types.encode(None) == encoded

        # test decoding all types with None values
        for _type in types.TYPES:
            encoded = types.Encoded(types.NONE_TYPE_STR, _type)
            assert types.decode(encoded) is None

        # test invalid None encoding
        encoded = types.Encoded("None", types.TYPES.NONE)
        with pytest.raises(ValueError):
            types.decode(encoded)

    def test_invalid(self):
        invalid_types = [
            # unsupported timezones
            dateutil.tz.gettz("America/New_York"),
            dateutil.tz.gettz("UTC"),
            pytz.FixedOffset(-300),
            pytz.FixedOffset(300),
            # datetimes with unsupported timezones
            datetime(1910, 12, 31, tzinfo=dateutil.tz.gettz("America/New_York")),
            datetime(1910, 12, 31, tzinfo=pytz.FixedOffset(-300)),
            # other unsupported types
            b"1234",
            {"k": "v"},
            [123],
            (9, 0, "inv"),
        ]

        for i in invalid_types:
            with pytest.raises(ValueError):
                types.encode(i)


class TestEncoded:
    def test_serializer(self):
        deserialized = [
            types.Encoded(types.NONE_TYPE_STR, types.TYPES.NONE),
            types.Encoded("-7200", types.TYPES.TZOFFSET_TZ),
            types.Encoded("-7200", types.TYPES.INT),
            types.Encoded("Zulu", types.TYPES.TZFILE_PYTZ),
            types.Encoded("Zulu", types.TYPES.STR),
            types.Encoded("1", types.TYPES.BOOL),
            types.Encoded("1", types.TYPES.INT),
            types.Encoded("0.0", types.TYPES.FLOAT),
            types.Encoded("0.00", types.TYPES.DECIMAL),
            types.Encoded(
                '["2020-01-01T12:00:00-05:00", ["[\\"UTC-5\\", \\"-18000\\"]", "TZOFFSET_DATEUTIL"]]',  # noqa E501
                types.TYPES.DATETIME,
            ),
            types.Encoded("1495-06-01", types.TYPES.DATE),
        ]

        serialized = [
            '["<class \'NoneType\'>", "NONE"]',
            '["-7200", "TZOFFSET_TZ"]',
            '["-7200", "INT"]',
            '["Zulu", "TZFILE_PYTZ"]',
            '["Zulu", "STR"]',
            '["1", "BOOL"]',
            '["1", "INT"]',
            '["0.0", "FLOAT"]',
            '["0.00", "DECIMAL"]',
            '["[\\"2020-01-01T12:00:00-05:00\\", [\\"[\\\\\\"UTC-5\\\\\\", \\\\\\"-18000\\\\\\"]\\", \\"TZOFFSET_DATEUTIL\\"]]", "DATETIME"]',  # noqa E501
            '["1495-06-01", "DATE"]',
        ]

        for i in range(len(deserialized)):
            assert deserialized[i].serialize() == serialized[i]
            assert types.Encoded.deserialize(serialized[i]) == deserialized[i]
