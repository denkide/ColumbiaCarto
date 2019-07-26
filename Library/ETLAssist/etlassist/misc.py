"""Simple helper objects for miscellaneous needs.

Do not put anything here which imports from other ETLAssist submodules!
"""
import datetime
import logging
import random
import types


__all__ = (
    'datestamp',
    'elapsed',
    'kwargs_cmp',
    "parity",
    "randomized",
    'timestamp',
    )
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def datestamp(fmt='%Y_%m_%d'):
    """Return string with current datestamp."""
    return timestamp(fmt)


def elapsed(start_time, logger=None, log_level=logging.INFO):
    """Return time-delta since start time."""
    span = datetime.datetime.now() - start_time
    if logger:
        logger.log(log_level, "Elapsed: %s hrs, %s min, %s sec.",
                   (span.days*24 + span.seconds//3600),
                   ((span.seconds//60)%60), (span.seconds%60))
    return span


def kwargs_cmp(cmp_prefix, **kwargs):
    """Return True if values match between kwarg and prefixed kwarg."""
    cmp_keys = (key for key in kwargs if not key.startswith(cmp_prefix)
                and cmp_prefix + key in kwargs)
    for key in cmp_keys:
        if kwargs[key] != kwargs[cmp_prefix + key]:
            return False
    return True


def parity(numbers):
    """Return proper parity for a collection of numbers.

    Args:
        numbers (iter): Collection of numbers.

    Returns:
        str: Type of parity - "even", "odd", or "mixed".
    """
    numbers_bitwise = {n & 1 for n in numbers}
    if not numbers_bitwise:
        _parity = None
    elif len(numbers_bitwise) == 1:
        _parity = {0: "even", 1: "odd"}[numbers_bitwise.pop()]
    else:
        _parity = "mixed"
    return _parity


def randomized(iterable):
    """Generate sequence items in random order.

    Args:
        iterable (iter): Collection of items to yield.

    Yields:
        object: Item from iterable.
    """
    if isinstance(iterable, types.GeneratorType):
        iterable = set(iterable)
    for item in random.sample(population=iterable, k=len(iterable)):
        yield item


def timestamp(fmt="%Y_%m_%d_T%H%M"):
    """Return string with current timestamp."""
    return datetime.datetime.now().strftime(fmt)
