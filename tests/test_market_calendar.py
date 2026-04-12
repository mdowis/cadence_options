"""Tests for market_calendar.py. Uses known historical holiday dates."""

import os
import sys
import unittest
from datetime import date, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.market_calendar import (
    easter_sunday,
    good_friday,
    nyse_holidays,
    nyse_early_closes,
    is_us_holiday,
    is_early_close,
    is_trading_day,
    get_market_close_time,
    dst_start,
    dst_end,
    is_us_dst,
    et_offset_hours,
    _nth_weekday,
    _last_weekday,
    _sunday_observance,
)


class TestEaster(unittest.TestCase):
    """Verify Easter algorithm against known historical dates."""

    def test_known_easter_dates(self):
        # Known Easter Sundays (widely published)
        known = {
            2020: date(2020, 4, 12),
            2021: date(2021, 4, 4),
            2022: date(2022, 4, 17),
            2023: date(2023, 4, 9),
            2024: date(2024, 3, 31),
            2025: date(2025, 4, 20),
            2026: date(2026, 4, 5),
            2027: date(2027, 3, 28),
            2028: date(2028, 4, 16),
        }
        for year, expected in known.items():
            self.assertEqual(easter_sunday(year), expected,
                             f"Easter {year} should be {expected}")

    def test_good_friday_is_two_days_before_easter(self):
        self.assertEqual(good_friday(2026), date(2026, 4, 3))
        self.assertEqual(good_friday(2024), date(2024, 3, 29))


class TestWeekdayHelpers(unittest.TestCase):

    def test_third_monday_january_2026(self):
        # MLK Day 2026 = Jan 19 (3rd Monday)
        self.assertEqual(_nth_weekday(2026, 1, 0, 3), date(2026, 1, 19))

    def test_first_monday_september_2026(self):
        # Labor Day 2026 = Sep 7
        self.assertEqual(_nth_weekday(2026, 9, 0, 1), date(2026, 9, 7))

    def test_last_monday_may_2026(self):
        # Memorial Day 2026 = May 25
        self.assertEqual(_last_weekday(2026, 5, 0), date(2026, 5, 25))

    def test_fourth_thursday_november_2026(self):
        # Thanksgiving 2026 = Nov 26
        self.assertEqual(_nth_weekday(2026, 11, 3, 4), date(2026, 11, 26))


class TestSundayObservance(unittest.TestCase):

    def test_sunday_observed_monday(self):
        # Dec 25, 2022 was a Sunday
        self.assertEqual(_sunday_observance(date(2022, 12, 25)),
                         date(2022, 12, 26))

    def test_saturday_not_observed(self):
        # July 4, 2026 is a Saturday -- NYSE does not observe
        self.assertEqual(_sunday_observance(date(2026, 7, 4)),
                         date(2026, 7, 4))

    def test_weekday_unchanged(self):
        self.assertEqual(_sunday_observance(date(2024, 12, 25)),
                         date(2024, 12, 25))


class TestNYSEHolidays2024(unittest.TestCase):
    """2024 full-day NYSE closures (verifiable via NYSE published schedule)."""

    def setUp(self):
        self.holidays = nyse_holidays(2024)

    def test_new_years_day(self):
        self.assertIn(date(2024, 1, 1), self.holidays)

    def test_mlk_day(self):
        self.assertIn(date(2024, 1, 15), self.holidays)

    def test_presidents_day(self):
        self.assertIn(date(2024, 2, 19), self.holidays)

    def test_good_friday(self):
        self.assertIn(date(2024, 3, 29), self.holidays)

    def test_memorial_day(self):
        self.assertIn(date(2024, 5, 27), self.holidays)

    def test_juneteenth(self):
        self.assertIn(date(2024, 6, 19), self.holidays)

    def test_independence_day(self):
        self.assertIn(date(2024, 7, 4), self.holidays)

    def test_labor_day(self):
        self.assertIn(date(2024, 9, 2), self.holidays)

    def test_thanksgiving(self):
        self.assertIn(date(2024, 11, 28), self.holidays)

    def test_christmas(self):
        self.assertIn(date(2024, 12, 25), self.holidays)

    def test_total_count_2024(self):
        # 10 scheduled NYSE holidays in 2024
        self.assertEqual(len(self.holidays), 10)


class TestNYSEHolidays2025(unittest.TestCase):
    """2025 verification."""

    def setUp(self):
        self.holidays = nyse_holidays(2025)

    def test_jan_1_2025(self):
        self.assertIn(date(2025, 1, 1), self.holidays)  # Wed

    def test_good_friday_2025(self):
        self.assertIn(date(2025, 4, 18), self.holidays)

    def test_juneteenth_2025(self):
        self.assertIn(date(2025, 6, 19), self.holidays)  # Thu

    def test_july_4_2025(self):
        self.assertIn(date(2025, 7, 4), self.holidays)  # Fri

    def test_thanksgiving_2025(self):
        self.assertIn(date(2025, 11, 27), self.holidays)

    def test_christmas_2025(self):
        self.assertIn(date(2025, 12, 25), self.holidays)  # Thu


class TestNYSEHolidays2026(unittest.TestCase):
    """2026 -- tests observance rules since July 4 is a Saturday."""

    def setUp(self):
        self.holidays = nyse_holidays(2026)

    def test_mlk_day(self):
        self.assertIn(date(2026, 1, 19), self.holidays)

    def test_good_friday_2026(self):
        self.assertIn(date(2026, 4, 3), self.holidays)

    def test_memorial_day_2026(self):
        self.assertIn(date(2026, 5, 25), self.holidays)

    def test_july_4_2026_not_observed(self):
        """July 4 2026 is a Saturday. NYSE does not observe on Friday."""
        # The actual Saturday is technically in the set but market is
        # already closed on Saturday. Important: it should NOT shift
        # to July 3 (Friday).
        self.assertNotIn(date(2026, 7, 3), self.holidays)

    def test_thanksgiving_2026(self):
        self.assertIn(date(2026, 11, 26), self.holidays)

    def test_christmas_2026(self):
        self.assertIn(date(2026, 12, 25), self.holidays)  # Friday


class TestObservance(unittest.TestCase):
    """Verify Sunday-to-Monday observance logic."""

    def test_christmas_2022_observed_monday(self):
        # Dec 25, 2022 was Sunday -> observed Dec 26 (Monday)
        holidays = nyse_holidays(2022)
        self.assertIn(date(2022, 12, 26), holidays)
        self.assertNotIn(date(2022, 12, 25), holidays)

    def test_new_years_2023_observed_monday(self):
        # Jan 1, 2023 was Sunday -> observed Jan 2 (Monday)
        holidays = nyse_holidays(2023)
        self.assertIn(date(2023, 1, 2), holidays)


class TestJuneteenthPre2022(unittest.TestCase):

    def test_not_holiday_before_2022(self):
        holidays = nyse_holidays(2020)
        self.assertNotIn(date(2020, 6, 19), holidays)

    def test_is_holiday_from_2022(self):
        holidays = nyse_holidays(2022)
        self.assertIn(date(2022, 6, 20), holidays)  # observed Mon (19 was Sun)


class TestEarlyCloses(unittest.TestCase):

    def test_black_friday_2024(self):
        early = nyse_early_closes(2024)
        # Day after Thanksgiving 2024 = Nov 29
        self.assertIn(date(2024, 11, 29), early)

    def test_black_friday_2025(self):
        early = nyse_early_closes(2025)
        self.assertIn(date(2025, 11, 28), early)

    def test_july_3_2024(self):
        # July 4 2024 is Thursday -- July 3 (Wed) is early close
        early = nyse_early_closes(2024)
        self.assertIn(date(2024, 7, 3), early)

    def test_july_3_2025(self):
        # July 4 2025 is Friday (holiday). July 3 is Thursday -> early close
        early = nyse_early_closes(2025)
        self.assertIn(date(2025, 7, 3), early)

    def test_no_july_3_when_saturday_holiday(self):
        # July 4 2026 is Saturday. July 3 is Friday -- regular trading day
        early = nyse_early_closes(2026)
        self.assertNotIn(date(2026, 7, 3), early)

    def test_christmas_eve_2024(self):
        # Dec 24 2024 is Tuesday -> early close
        early = nyse_early_closes(2024)
        self.assertIn(date(2024, 12, 24), early)

    def test_christmas_eve_2022_saturday(self):
        # Dec 24 2022 is Saturday -- no trading, no early close
        early = nyse_early_closes(2022)
        self.assertNotIn(date(2022, 12, 24), early)

    def test_early_close_not_full_holiday(self):
        """An early-close date should not also appear as a full holiday."""
        for year in [2024, 2025, 2026]:
            early = nyse_early_closes(year)
            holidays = nyse_holidays(year)
            self.assertEqual(early & holidays, set(),
                             f"{year}: overlap between early and holiday")


class TestPublicPredicates(unittest.TestCase):

    def test_is_us_holiday(self):
        self.assertTrue(is_us_holiday(date(2024, 12, 25)))
        self.assertFalse(is_us_holiday(date(2024, 12, 26)))

    def test_is_early_close(self):
        self.assertTrue(is_early_close(date(2024, 11, 29)))  # day after TG
        self.assertFalse(is_early_close(date(2024, 12, 25)))  # full holiday

    def test_is_trading_day_weekday(self):
        # Wed, Apr 1, 2026 -- regular trading day
        self.assertTrue(is_trading_day(date(2026, 4, 1)))

    def test_is_trading_day_weekend(self):
        # Sat
        self.assertFalse(is_trading_day(date(2026, 4, 4)))

    def test_is_trading_day_holiday(self):
        self.assertFalse(is_trading_day(date(2026, 4, 3)))  # Good Friday

    def test_get_market_close_regular_day(self):
        # Wed Apr 1 2026 is a regular day
        self.assertEqual(get_market_close_time(date(2026, 4, 1)), time(16, 0))

    def test_get_market_close_early(self):
        # Black Friday 2024
        self.assertEqual(get_market_close_time(date(2024, 11, 29)), time(13, 0))

    def test_get_market_close_holiday(self):
        self.assertIsNone(get_market_close_time(date(2024, 12, 25)))

    def test_get_market_close_weekend(self):
        self.assertIsNone(get_market_close_time(date(2026, 4, 4)))


class TestDST(unittest.TestCase):
    """Verify US DST transition dates against published values."""

    def test_dst_start_dates(self):
        # DST starts 2nd Sunday of March
        self.assertEqual(dst_start(2024), date(2024, 3, 10))
        self.assertEqual(dst_start(2025), date(2025, 3, 9))
        self.assertEqual(dst_start(2026), date(2026, 3, 8))
        self.assertEqual(dst_start(2027), date(2027, 3, 14))
        self.assertEqual(dst_start(2028), date(2028, 3, 12))

    def test_dst_end_dates(self):
        # DST ends 1st Sunday of November
        self.assertEqual(dst_end(2024), date(2024, 11, 3))
        self.assertEqual(dst_end(2025), date(2025, 11, 2))
        self.assertEqual(dst_end(2026), date(2026, 11, 1))
        self.assertEqual(dst_end(2027), date(2027, 11, 7))

    def test_is_us_dst_midsummer(self):
        # July is solidly in DST
        self.assertTrue(is_us_dst(date(2024, 7, 15)))
        self.assertTrue(is_us_dst(date(2026, 8, 1)))

    def test_is_us_dst_midwinter(self):
        # January is solidly in EST
        self.assertFalse(is_us_dst(date(2024, 1, 15)))
        self.assertFalse(is_us_dst(date(2026, 12, 15)))

    def test_is_us_dst_day_before_start(self):
        # Day before transition is EST
        self.assertFalse(is_us_dst(date(2024, 3, 9)))
        self.assertFalse(is_us_dst(date(2026, 3, 7)))

    def test_is_us_dst_start_day(self):
        # The start day itself is considered DST (matches the afternoon state)
        self.assertTrue(is_us_dst(date(2024, 3, 10)))
        self.assertTrue(is_us_dst(date(2026, 3, 8)))

    def test_is_us_dst_end_day(self):
        # The end day is considered NOT DST (matches the afternoon state)
        self.assertFalse(is_us_dst(date(2024, 11, 3)))
        self.assertFalse(is_us_dst(date(2026, 11, 1)))

    def test_is_us_dst_day_before_end(self):
        # Saturday before fall-back is still DST
        self.assertTrue(is_us_dst(date(2024, 11, 2)))
        self.assertTrue(is_us_dst(date(2026, 10, 31)))

    def test_et_offset_hours_summer(self):
        self.assertEqual(et_offset_hours(date(2024, 7, 15)), -4)

    def test_et_offset_hours_winter(self):
        self.assertEqual(et_offset_hours(date(2024, 1, 15)), -5)


class TestNowET(unittest.TestCase):
    """Verify _now_et applies the correct DST-aware offset."""

    def test_summer_uses_minus_4(self):
        from cadence.process_controller import _now_et
        from unittest.mock import patch
        from datetime import datetime as dt, timezone
        # July 15 2024 at 18:00 UTC -> 14:00 EDT
        fake_utc = dt(2024, 7, 15, 18, 0, 0, tzinfo=timezone.utc)
        with patch("cadence.process_controller.datetime") as mock_dt:
            mock_dt.now.return_value = fake_utc
            # Keep real datetime for other uses inside the function
            mock_dt.side_effect = lambda *a, **kw: dt(*a, **kw)
            result = _now_et()
        self.assertEqual(result.hour, 14)
        self.assertEqual(result.minute, 0)

    def test_winter_uses_minus_5(self):
        from cadence.process_controller import _now_et
        from unittest.mock import patch
        from datetime import datetime as dt, timezone
        # Jan 15 2024 at 18:00 UTC -> 13:00 EST
        fake_utc = dt(2024, 1, 15, 18, 0, 0, tzinfo=timezone.utc)
        with patch("cadence.process_controller.datetime") as mock_dt:
            mock_dt.now.return_value = fake_utc
            mock_dt.side_effect = lambda *a, **kw: dt(*a, **kw)
            result = _now_et()
        self.assertEqual(result.hour, 13)
        self.assertEqual(result.minute, 0)

    def test_day_after_dst_start_uses_edt(self):
        from cadence.process_controller import _now_et
        from unittest.mock import patch
        from datetime import datetime as dt, timezone
        # March 11 2024 (Monday after DST start) at 14:30 UTC -> 10:30 EDT
        fake_utc = dt(2024, 3, 11, 14, 30, 0, tzinfo=timezone.utc)
        with patch("cadence.process_controller.datetime") as mock_dt:
            mock_dt.now.return_value = fake_utc
            mock_dt.side_effect = lambda *a, **kw: dt(*a, **kw)
            result = _now_et()
        self.assertEqual(result.hour, 10)
        self.assertEqual(result.minute, 30)

    def test_day_before_dst_start_uses_est(self):
        from cadence.process_controller import _now_et
        from unittest.mock import patch
        from datetime import datetime as dt, timezone
        # March 8 2024 (Friday before DST) at 14:30 UTC -> 09:30 EST
        fake_utc = dt(2024, 3, 8, 14, 30, 0, tzinfo=timezone.utc)
        with patch("cadence.process_controller.datetime") as mock_dt:
            mock_dt.now.return_value = fake_utc
            mock_dt.side_effect = lambda *a, **kw: dt(*a, **kw)
            result = _now_et()
        self.assertEqual(result.hour, 9)
        self.assertEqual(result.minute, 30)


class TestProcessControllerIntegration(unittest.TestCase):
    """Verify process_controller.is_market_open respects the calendar."""

    def test_market_closed_on_christmas(self):
        from cadence.process_controller import is_market_open
        from unittest.mock import patch
        from datetime import datetime as dt
        # Dec 25, 2024 at 10:00 AM ET -- should be closed (Christmas)
        with patch("cadence.process_controller._now_et",
                   return_value=dt(2024, 12, 25, 10, 0, 0)):
            self.assertFalse(is_market_open())

    def test_market_closed_on_good_friday(self):
        from cadence.process_controller import is_market_open
        from unittest.mock import patch
        from datetime import datetime as dt
        # Good Friday 2026 = April 3 -- even at 10:00 AM, market closed
        with patch("cadence.process_controller._now_et",
                   return_value=dt(2026, 4, 3, 10, 0, 0)):
            self.assertFalse(is_market_open())

    def test_early_close_at_1pm(self):
        from cadence.process_controller import is_market_open
        from unittest.mock import patch
        from datetime import datetime as dt
        # Black Friday 2024 at 12:30 -- should still be open
        with patch("cadence.process_controller._now_et",
                   return_value=dt(2024, 11, 29, 12, 30, 0)):
            self.assertTrue(is_market_open())
        # Same day at 13:30 -- should be closed
        with patch("cadence.process_controller._now_et",
                   return_value=dt(2024, 11, 29, 13, 30, 0)):
            self.assertFalse(is_market_open())

    def test_regular_day_10am_open(self):
        from cadence.process_controller import is_market_open
        from unittest.mock import patch
        from datetime import datetime as dt
        # Apr 1 2026 is Wednesday, regular day
        with patch("cadence.process_controller._now_et",
                   return_value=dt(2026, 4, 1, 10, 0, 0)):
            self.assertTrue(is_market_open())


if __name__ == "__main__":
    unittest.main(verbosity=2)
