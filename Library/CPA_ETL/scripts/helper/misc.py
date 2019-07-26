"""Miscellaneous implementation objects

Extends the `misc` submodule from the ETLAssist package.
"""
from collections import Counter
import datetime
import logging
import random

import sqlalchemy as sql

# arcetl imported locally to avoid slow imports when unused.
# import arcetl
from etlassist.misc import *  # pylint: disable=wildcard-import, unused-wildcard-import

from . import database  # pylint: disable=relative-beyond-top-level
from . import dataset  # pylint: disable=relative-beyond-top-level
from .model import (  # pylint: disable=relative-beyond-top-level
    RLIDAccount,
    RLIDMetadataDataCurrency,
    RLIDMetadataTaxYear,
    RLIDOwner,
    RLIDTaxYear,
)


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

IGNORE_PATTERNS_DATA_BACKUP = [
    "_old",
    "_outputview",
    "_to_delete",
    "_vw",
    "old_",
    "outputview_",
    "to_delete_",
    "vw_",
]
"""list: Patterns in dataset names to ignore when backing up data."""
IGNORE_PATTERNS_RLIDGEO_SNAPSHOT = IGNORE_PATTERNS_DATA_BACKUP + [
    "compress_log",
    "ElevationContour",
    "TaxCodeArea_2",
    "Taxlot_2",
]
"""list: Patterns in dataset names to ignore when making RLIDGeo snapshot."""
REAL_LOT_SQL = "taxlot not in ('11', '22', '33', '44', '55', '66', '77', '88', '99')"
"""str: SQL where-clause for real property taxlot subselection."""
TOLERANCE = {"area": 2.0, "xy": 0.02}
"""dict: Mapping of tolerance type to value (in feet)."""


def address_intid_to_uuid_map(address_where_sql=None):
    """Return mapping of site address geofeature integer ID to UUID.

    Args:
        address_where_sql (str): SQL where-clause for address subselection.

    Returns:
        dict
    """
    import arcetl

    intid_uuids = arcetl.attributes.as_iters(
        dataset.SITE_ADDRESS.path("pub"),
        field_names=["geofeat_id", "site_address_gfid"],
        dataset_where_sql=address_where_sql,
    )
    return dict(intid_uuids)


def current_tax_year():
    """Return the current tax year, as set in RLID.

    Returns:
        int
    """
    session = database.RLID.create_session()
    try:
        query = session.query(RLIDMetadataTaxYear.current_tax_year)
        year = int(query.one()[0])
    finally:
        session.close()
    return year


CURRENT_TAX_YEAR = current_tax_year()
"""int: The current tax year, as set in RLID."""


def rlid_owners(include_attributes=None):
    """Generate RLID owner IDs or tuples of owner IDs with extra attributes.

    Args:
        include_attributes (iter): Names attributes to include alongside the ID in a
            generated tuple. If empty or None, the ID will be yielded by itself (i.e.
            not in a tuple).

    Yields:
        int, tuple
    """
    if include_attributes is None:
        include_attributes = []
    session = database.RLID.create_session()
    try:
        query = session.query(
            RLIDOwner.owner_id,
            *(getattr(RLIDOwner, attribute) for attribute in include_attributes)
        )
        for owner in query:
            yield owner if include_attributes else owner[0]

    finally:
        session.close()


def rlid_accounts(include_attributes=None, **kwargs):
    """Generate RLID account IDs or tuples of owner IDs with other included attributes.

    Args:
        include_attributes (iter): Names attributes to include alongside the ID in a
            generated tuple. If empty or None, the ID will be yielded by itself (i.e.
            not in a tuple).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        exclude_inactive (bool): Exclude inactive accounts if True. Default is False.
        exclude_mobile_home (bool): Exclude mobile home accounts if True. Default is
            False.
        exclude_personal (bool): Exclude personal accounts if True. Default is False.
        exclude_utilities (bool): Exclude utility accounts if True. Default is False.

    Yields:
        int, tuple
    """
    if include_attributes is None:
        include_attributes = []
    filters = []
    if kwargs.get("exclude_inactive"):
        filters.append(
            sql.or_(
                RLIDAccount.active_this_year == "Y",
                RLIDAccount.new_acct_active_next_year == "Y",
            )
        )
    if kwargs.get("exclude_mobile_home"):
        filters.append(
            sql.or_(
                RLIDAccount.account_int < 4000000, RLIDAccount.account_int > 4999999
            )
        )
    if kwargs.get("exclude_personal"):
        filters.append(RLIDAccount.account_type != "PP")
    if kwargs.get("exclude_utilities"):
        filters.append(
            sql.or_(
                RLIDAccount.account_int < 8000000, RLIDAccount.account_int > 8999999
            )
        )
    session = database.RLID.create_session()
    try:
        query = session.query(
            RLIDAccount.account_int,
            *(getattr(RLIDAccount, attr) for attr in include_attributes)
        ).filter(*filters)
        for attrs in query:
            yield tuple(attrs) if include_attributes else attrs[0]

    finally:
        session.close()


def rlid_data_currency(data_description):
    """Return currency date for described data in RLID.

    Args:
        data_description (str): Description of data to check currency.

    Returns:
        datetime.datetime
    """
    session = database.RLID.create_session()
    try:
        query = session.query(RLIDMetadataDataCurrency.load_date).filter(
            RLIDMetadataDataCurrency.data_description == data_description
        )
        currency_date = query.one()[0]
    finally:
        session.close()
    return currency_date


def rlid_data_currency_setter(data_description, currency_date):
    """Set currency date for described data in RLID.

    Args:
        data_description (str): Description of data to check currency.

    Returns:
        datetime.datetime: Newly-set currency date for described data in RLID.
    """
    session = database.RLID.create_session()
    try:
        query = session.query(RLIDMetadataDataCurrency).filter(
            RLIDMetadataDataCurrency.data_description == data_description
        )
        row = query.one_or_none()
        try:
            row.writeoff_date = row.load_date = currency_date
        except AttributeError:
            raise AttributeError("No currency entry for {}".format(data_description))

        session.commit()
    finally:
        session.close()
    LOG.info(
        "Updated %s repository currency in RLID to %s.",
        data_description,
        currency_date.strftime("%Y-%m-%d %H:%M:%S"),
    )
    return currency_date


def rlid_tax_year_accounts(tax_year=CURRENT_TAX_YEAR, include_attributes=None):
    """Generate RLID account tax-year IDs or tuples of owner IDs with extra attributes.

    Args:
        tax_year (int): Tax year to generate accounts for.
        include_attributes (iter): Names attributes to include alongside the ID in a
            generated tuple. If empty or None, the ID will be yielded by itself (i.e.
            not in a tuple).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        exclude_inactive (bool): Exclude inactive accounts if True. Default is False.
        exclude_mobile_home (bool): Exclude mobile home accounts if True. Default is
            False.
        exclude_personal (bool): Exclude personal accounts if True. Default is False.
        exclude_utilities (bool): Exclude utility accounts if True. Default is False.

    Yields:
        int, tuple
    """
    if include_attributes is None:
        include_attributes = []
    session = database.RLID.create_session()
    try:
        query = session.query(
            RLIDTaxYear.account_int,
            *(getattr(RLIDTaxYear, attribute) for attribute in include_attributes)
        ).filter(RLIDTaxYear.tax_year == tax_year)
        for account in query:
            yield account if include_attributes else account[0]

    finally:
        session.close()


def taxlot_area_map():
    """Return mapping of maptaxlot to total area.

    Area will be in the linear unit of the taxlot geometry coordinate system.

    Returns:
        dict
    """
    import arcetl

    taxlot_area = {}
    for maptaxlot, area in arcetl.attributes.as_iters(
        dataset_path=dataset.TAXLOT.path("pub"), field_names=["maptaxlot", "shape@area"]
    ):
        if maptaxlot not in taxlot_area:
            taxlot_area[maptaxlot] = 0.0
        taxlot_area[maptaxlot] += area if area else 0.0
    return taxlot_area


def taxlot_prefixes(prefix_length, sort_style=None):
    """Generate prefixes of given length in taxlots.

    Args:
        prefix_length (int): Length of the taxlot prefixes to generate.
        sort_style (str): Slug of the sorting style to use. Valid sort styles:
            None: No sorting.
            count_ascending: Lowest-to-highest number of taxlots with prefix.
            count_descending: Highest-to-lowest number of taxlots with prefix.
            count_by_half_of_month: Lowest-to-highest number of taxlots with prefix in
                first half of month, highest-to-lowest in second half.
            random: Prefixes randomly generated.

    Yields:
        str
    """
    import arcetl

    maptaxlots = arcetl.attributes.as_iters(
        dataset_path=dataset.TAXLOT.path("pub"), field_names=["maptaxlot"]
    )
    prefix_count = Counter(
        maptaxlot[:prefix_length] for maptaxlot, in maptaxlots if maptaxlot
    )
    if sort_style is None:
        prefixes = (prefix for prefix in prefix_count)
    elif sort_style == "count_ascending":
        prefixes = (prefix for prefix in sorted(prefix_count, key=prefix_count.get))
    elif sort_style == "count_descending":
        prefixes = (
            prefix
            for prefix in sorted(prefix_count, key=prefix_count.get, reverse=True)
        )
    elif sort_style == "count_by_half_of_month":
        descending = datetime.datetime.now().day > 15
        prefixes = (
            prefix
            for prefix in sorted(prefix_count, key=prefix_count.get, reverse=descending)
        )
    elif sort_style == "random":
        prefixes = (
            prefix for prefix in random.sample(prefix_count.keys(), len(prefix_count))
        )
    else:
        raise ValueError("{} sorting style not implemented.".format(sort_style))

    for prefix in prefixes:
        yield prefix


def taxlot_subset_temp_copies(dataset_where_sql=None, subset_prefix_length=4, **kwargs):
    """Generate temporary copies of taxlot subsets.

    Defining subset as the first n digits in the map number.

    Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        subset_prefix_length (int): Length of maptaxlot prefix to create subsets for.

    Keyword Args:
        field_names (iter): Collection of field names to include in copy. If field_names
            not specified, all fields will be included.

    Yields:
        arcetl.TempDatasetCopy
    """
    import arcetl

    kwargs.setdefault("field_names", ["maptaxlot"])
    for prefix in taxlot_prefixes(subset_prefix_length, sort_style="random"):
        where_sql = "maptaxlot like '{}%' and ({})".format(prefix, dataset_where_sql)
        yield arcetl.TempDatasetCopy(dataset.TAXLOT.path("pub"), where_sql, **kwargs)
