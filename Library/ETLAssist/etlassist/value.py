"""Value-building, -deriving, and -cleaning objects."""
import logging
import string
import unicodedata

import dateutil.parser


__all__ = (
    'clean_whitespace',
    'clean_whitespace_without_clear',
    'concatenate_arguments',
    'datetime_from_string',
    'force_case',
    'force_lowercase',
    'force_title_case',
    'force_uppercase',
    'force_yn',
    'is_numeric',
    'maptaxlot_separated',
    'remove_diacritics',
    )
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def clean_whitespace(value, clear_empty_string=True):
    """Return value with whitespace stripped & deduplicated.

    Args:
        value (str): Value to clean.
        clear_empty_string (bool): Convert empty string results to NoneTypes if True.

    Returns
        str, NoneType: Cleaned value.

    """
    if value is not None:
        value = value.strip()
        for character in string.whitespace:
            while character*2 in value:
                value = value.replace(character*2, character)
    if clear_empty_string and not value:
        value = None
    return value


def clean_whitespace_without_clear(value):
    """Return value with whitespace stripped & deduplicated.

    Will not return NoneType if string is (or ends up) empty.
    """
    return clean_whitespace(value, clear_empty_string=False)


def concatenate_arguments(*args, **kwargs):
    """Return concatenated string from ordered arguments with separation.

    Ignores NoneTypes.

    Args:
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        separator (str): Character(s) used to separate the argument strings. Default is
            a single space " ".
    """
    separator = kwargs.get("separator", " ")
    return separator.join(str(arg).strip() for arg in args if arg is not None)


def datetime_from_string(value):
    """Return datetime object from input if possible, None if not."""
    try:
        result = dateutil.parser.parse(value) if value else None
    except ValueError:
        result = None
    return result


def force_case(value, case_method_name):
    """Return value converted to chosen casing."""
    if value:
        value = getattr(value, case_method_name.lower())()
    return value


def force_lowercase(value):
    """Return value converted to lowercase."""
    return force_case(value, 'lower')


def force_title_case(value):
    """Return value converted to title case."""
    return force_case(value, 'title')


def force_uppercase(value):
    """Return value converted to uppercase."""
    return force_case(value, 'upper')


def force_yn(value, default=None):
    """Return value if 'Y', 'y', 'N', or'n'; otherwise return default."""
    return value if value in ('n', 'N', 'y', 'Y') else default


def is_numeric(value, nonetype_ok=True):
    """Return True if value is numeric.

    Props to: http://pythoncentral.io/
        how-to-check-if-a-string-is-a-number-in-python-including-unicode/
    """
    try:
        float(value)
    except TypeError:
        if value is None and nonetype_ok:
            result = True
        else:
            LOG.exception("value %s type handling not defined.", value)
            raise
    except ValueError:
        result = False
    else:
        result = str(value).lower() not in ('nan', 'inf')
    return result


def maptaxlot_separated(maptaxlot, separator='-'):
    """Return map/taxlot string separated into parts."""
    if maptaxlot:
        result = concatenate_arguments(maptaxlot[:2], maptaxlot[2:4],
                                       maptaxlot[4:6], maptaxlot[6:8],
                                       maptaxlot[-5:], separator=separator)
    else:
        result = None
    return result


def remove_diacritics(value):
    """Return string with diacritics removed. Value must be unicode."""
    if value:
        value = u''.join(char for char in unicodedata.normalize('NFKD', value)
                         if not unicodedata.combining(char))
    return value
