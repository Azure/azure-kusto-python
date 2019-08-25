"""Converter tests of the client."""

import unittest
from datetime import timedelta
from azure.kusto.data._converters import to_datetime, to_timedelta


class ConverterTests(unittest.TestCase):
    """These are unit tests that should test custom converters used in."""

    def test_to_timestamp(self):
        """Happy path to test converter from TimeSpan to timedelta."""
        # @Daniel
        # Test hours, minutes and seconds
        self.assertEqual(to_timedelta("00:00:00"), timedelta(seconds=0))
        self.assertEqual(to_timedelta("00:00:03"), timedelta(seconds=3))
        self.assertEqual(to_timedelta("00:04:03"), timedelta(minutes=4, seconds=3))
        self.assertEqual(to_timedelta("02:04:03"), timedelta(hours=2, minutes=4, seconds=3))
        # Test milliseconds
        self.assertEqual(to_timedelta("00:00:00.099"), timedelta(milliseconds=99))
        self.assertEqual(to_timedelta("02:04:03.0123"), timedelta(hours=2, minutes=4, seconds=3, microseconds=12300))
        # Test days
        self.assertEqual(to_timedelta("01.00:00:00"), timedelta(days=1))
        self.assertEqual(to_timedelta("02.04:05:07"), timedelta(days=2, hours=4, minutes=5, seconds=7))
        # Test negative
        self.assertEqual(to_timedelta("-01.00:00:00"), -timedelta(days=1))
        self.assertEqual(to_timedelta("-02.04:05:07"), -timedelta(days=2, hours=4, minutes=5, seconds=7))
        # Test all together
        self.assertEqual(to_timedelta("00.00:00:00.000"), timedelta(seconds=0))
        self.assertEqual(
            to_timedelta("02.04:05:07.789"), timedelta(days=2, hours=4, minutes=5, seconds=7, milliseconds=789)
        )
        self.assertEqual(to_timedelta("03.00:00:00.111"), timedelta(days=3, milliseconds=111))
        # Test from Ticks
        self.assertEqual(to_timedelta(-80080008), timedelta(microseconds=-8008001))
        self.assertEqual(to_timedelta(10010001), timedelta(microseconds=1001000))

    def test_to_timestamp_fail(self):
        """
        Sad path to test TimeSpan to timedelta converter
        """
        self.assertRaises(ValueError, to_timedelta, "")
        self.assertRaises(ValueError, to_timedelta, "foo")
        self.assertRaises(ValueError, to_timedelta, "00")
        self.assertRaises(ValueError, to_timedelta, "00:00")
        self.assertRaises(ValueError, to_timedelta, "03.00:00:00.")
        self.assertRaises(ValueError, to_timedelta, "03.00:00:00.111a")

    def test_to_datetime(self):
        """ Tests datetime read by KustoResultIter """
        # @Daniel
        self.assertIsNotNone(to_datetime("2016-06-07T16:00:00Z"))

    def test_to_datetime_fail(self):
        """ Tests that invalid strings fails to convert to datetime """
        self.assertRaises(ValueError, to_datetime, "invalid")
