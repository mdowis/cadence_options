"""US equity market holiday calendar.

All NYSE holidays are deterministic given the year -- either fixed dates
(with Sunday-to-Monday observance), fixed weekday positions (e.g., 3rd
Monday of January), or derived from Easter (Good Friday).

This module computes them at runtime without any external data. It also
handles the three regular early-close days (1pm ET): the Friday after
Thanksgiving, July 3 (when July 4 is a weekday), and December 24
(when it is a weekday).
"""

from datetime import date, time, timedelta


# ---- Easter calculation (Anonymous Gregorian algorithm) ----

def easter_sunday(year):
    """Return the date of Easter Sunday for the given year (Gregorian)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def good_friday(year):
    """Good Friday is two days before Easter Sunday."""
    return easter_sunday(year) - timedelta(days=2)


# ---- Weekday-position helpers ----

def _nth_weekday(year, month, weekday, n):
    """Return the date of the nth occurrence of `weekday` in month/year.
    weekday: 0=Mon, 1=Tue, ..., 6=Sun. n: 1-based (1=first, 2=second, ...).
    """
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year, month, weekday):
    """Return the last `weekday` in month/year."""
    # Start from the last day of the month and walk back
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last = next_month - timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def _sunday_observance(d):
    """NYSE observes Sunday holidays on the following Monday.
    Saturday holidays are not observed (market already closed)."""
    if d.weekday() == 6:  # Sunday
        return d + timedelta(days=1)
    return d


# ---- NYSE holiday calendar ----

def nyse_holidays(year):
    """Return the set of full-day NYSE market closures for the given year.

    Excludes presidential funerals and other ad-hoc closures, which are
    not deterministic. Includes all regularly scheduled holidays.
    """
    holidays = set()

    # New Year's Day (observed)
    holidays.add(_sunday_observance(date(year, 1, 1)))

    # MLK Day: 3rd Monday of January
    holidays.add(_nth_weekday(year, 1, 0, 3))

    # Presidents' Day: 3rd Monday of February
    holidays.add(_nth_weekday(year, 2, 0, 3))

    # Good Friday
    holidays.add(good_friday(year))

    # Memorial Day: last Monday of May
    holidays.add(_last_weekday(year, 5, 0))

    # Juneteenth (observed). Became a federal holiday in 2021, NYSE in 2022.
    if year >= 2022:
        holidays.add(_sunday_observance(date(year, 6, 19)))

    # Independence Day (observed)
    holidays.add(_sunday_observance(date(year, 7, 4)))

    # Labor Day: 1st Monday of September
    holidays.add(_nth_weekday(year, 9, 0, 1))

    # Thanksgiving: 4th Thursday of November
    holidays.add(_nth_weekday(year, 11, 3, 4))

    # Christmas (observed)
    holidays.add(_sunday_observance(date(year, 12, 25)))

    return holidays


# ---- Early close days ----

def nyse_early_closes(year):
    """Return the set of NYSE early-close days (1pm ET) for the year.

    Regular early closes:
    - Friday after Thanksgiving
    - July 3 (when July 4 falls on a weekday Mon-Fri)
    - December 24 (when it falls on a weekday Mon-Fri)
    """
    early = set()

    # Day after Thanksgiving
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    early.add(thanksgiving + timedelta(days=1))

    # July 3 (only when July 4 is a weekday and July 3 itself is a weekday)
    july_4 = date(year, 7, 4)
    july_3 = date(year, 7, 3)
    if july_4.weekday() < 5 and july_3.weekday() < 5:
        early.add(july_3)

    # December 24 if it is a weekday
    dec_24 = date(year, 12, 24)
    if dec_24.weekday() < 5:
        early.add(dec_24)

    # Exclude any early-close date that is actually a full holiday
    return early - nyse_holidays(year)


# ---- Public predicates ----

def is_us_holiday(d):
    """True if d is a full-day NYSE closure."""
    return d in nyse_holidays(d.year)


def is_early_close(d):
    """True if d is a 1pm-ET early close (and not a full holiday)."""
    return d in nyse_early_closes(d.year)


def is_trading_day(d):
    """True if d is a weekday and not a full holiday."""
    if d.weekday() >= 5:  # Sat/Sun
        return False
    return not is_us_holiday(d)


def get_market_close_time(d):
    """Return the NYSE close time for d as a datetime.time.

    Returns None if d is a full holiday or weekend.
    Returns time(13, 0) for early-close days.
    Returns time(16, 0) for regular trading days.
    """
    if not is_trading_day(d):
        return None
    if is_early_close(d):
        return time(13, 0)
    return time(16, 0)


# Regular market open is 9:30 ET every trading day.
MARKET_OPEN = time(9, 30)
