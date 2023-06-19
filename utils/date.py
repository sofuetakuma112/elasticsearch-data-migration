from datetime import datetime, timezone
import unittest
import pytz


def parse_datetime(datetime_str, tz):
    # Define the timezone objects
    jst = pytz.timezone("Asia/Tokyo")
    utc = pytz.timezone("UTC")

    # Choose the correct timezone based on the tz argument
    if tz.lower() == "jst":
        tz_obj = jst
    elif tz.lower() == "utc":
        tz_obj = utc
    else:
        raise ValueError("Invalid timezone. Must be 'jst' or 'utc'.")

    datetime_formats = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ]

    for datetime_format in datetime_formats:
        try:
            parsed_datetime = datetime.strptime(datetime_str, datetime_format)

            if parsed_datetime.tzinfo is None:
                parsed_datetime = tz_obj.localize(parsed_datetime)
            return parsed_datetime
        except ValueError:
            continue

    raise ValueError(f"Failed to parse datetime: {datetime_str}")


class TestParseDatetime(unittest.TestCase):
    def test_parse_datetime_with_valid_formats(self):
        datetime_str = "2022-12-31T23:59:59.999"
        expected_datetime = datetime(
            2022, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc
        )
        result = parse_datetime(datetime_str, "utc")
        self.assertEqual(result, expected_datetime)

        datetime_str = "2022-12-31T23:59:59"
        expected_datetime = datetime(2022, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        result = parse_datetime(datetime_str, "utc")
        self.assertEqual(result, expected_datetime)

        datetime_str = "2022-12-31T23:59:59+00:00"
        expected_datetime = datetime(2022, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        result = parse_datetime(datetime_str, "utc")
        self.assertEqual(result, expected_datetime)

        datetime_str = "2022-12-31T23:59:59.999+00:00"
        expected_datetime = datetime(
            2022, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc
        )
        result = parse_datetime(datetime_str, "utc")
        self.assertEqual(result, expected_datetime)

        datetime_str = "2022-12-31T23:59:59Z"
        expected_datetime = datetime(2022, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        result = parse_datetime(datetime_str, "utc")
        self.assertEqual(result, expected_datetime)

        datetime_str = "2023-03-27T18:56:00.078045"
        expected_datetime = datetime(
            2023, 3, 27, 18, 56, 0, 78045, tzinfo=pytz.FixedOffset(540)
        )
        result = parse_datetime(datetime_str, "jst")
        self.assertEqual(result, expected_datetime)

        datetime_str = "2023-03-27T09:56:00.078045"
        expected_datetime = datetime(2023, 3, 27, 9, 56, 0, 78045, tzinfo=timezone.utc)
        result = parse_datetime(datetime_str, "utc")
        self.assertEqual(result, expected_datetime)

    def test_parse_datetime_with_invalid_format(self):
        datetime_str = "2022-12-31 23:59:59"  # Invalid format
        with self.assertRaises(ValueError):
            parse_datetime(datetime_str, "utc")

        datetime_str = "2022-12-31T23:59:59.999+00:00Z"  # Invalid format
        with self.assertRaises(ValueError):
            parse_datetime(datetime_str, "utc")

    def test_parse_datetime_with_invalid_timezone(self):
        datetime_str = "2022-12-31T23:59:59.999+00:00"
        with self.assertRaises(ValueError):
            parse_datetime(datetime_str, "pst")  # Invalid timezone


if __name__ == "__main__":
    unittest.main()
