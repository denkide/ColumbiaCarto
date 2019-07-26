"""Execution code for taxlot processing."""
import argparse
from collections import defaultdict
import functools
import logging
import os

import sqlalchemy as sql

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import dataset
from helper.misc import CURRENT_TAX_YEAR, REAL_LOT_SQL, TOLERANCE, rlid_owners
from helper.model import (
    RLIDAccount,
    RLIDExemption,
    RLIDImprovement,
    RLIDOwner,
    RLIDTaxYear,
)
from helper import path
from helper import transform
from helper import url
from helper.value import maptaxlot_separated


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# Helpers.


def account_count_map():
    """Return mapping of maptaxlot to number of accounts.

    Returns:
        dict
    """
    session = database.RLID.create_session()
    try:
        account_count_query = (
            session.query(
                RLIDAccount.maptaxlot, sql.func.count(RLIDAccount.account_int)
            )
            .filter(
                RLIDAccount.maptaxlot is not None,
                # Active accounts.
                sql.or_(
                    RLIDAccount.active_this_year == "Y",
                    RLIDAccount.new_acct_active_next_year == "Y",
                ),
                # Exclude personal property.
                RLIDAccount.account_type != "PP",
                # Exclude mobile home accounts.
                sql.or_(
                    RLIDAccount.account_int < 4000000, RLIDAccount.account_int > 4999999
                ),
                # Exclude utility accounts.
                sql.or_(
                    RLIDAccount.account_int < 8000000, RLIDAccount.account_int > 8999999
                ),
            )
            .group_by(RLIDAccount.maptaxlot)
        )
        account_count = dict(account_count_query)
    finally:
        session.close()
    return account_count


def eval_primary_account_all_values(account_details):
    """Evaluate primary account using land and improvement values.

    Used for lots with multiple developments.

    Args:
        account_details (dict): Mapping of account attribute name to value.

    Returns:
        int: Account number.
    """
    candidates = (
        account
        for account in account_details
        if account["rmv_land_value"] and account["rmv_imp_value"]
    )
    try:
        pick = next(candidates)
    except StopIteration:
        pick = {"account_int": None}
    for candidate in candidates:
        # Pick by most improvement value.
        if candidate["rmv_imp_value"] > pick["rmv_imp_value"]:
            pick = candidate
        # If equal improvement value, pick by most acreage.
        elif candidate["rmv_imp_value"] == pick["rmv_imp_value"]:
            if candidate["land_acres"] > pick["land_acres"]:
                pick = candidate
            # If equal acreage, pick by lowest account number.
            elif candidate["land_acres"] == pick["land_acres"]:
                if candidate["account_int"] < pick["account_int"]:
                    pick = candidate
    return pick["account_int"]


def eval_primary_account_imp_value(account_details):
    """Evaluate primary account using imrpovement value only.

    Used for structure-only lots atop other land lots.

    Args:
        account_details (dict): Mapping of account attribute name to value.

    Returns:
        int: Account number.
    """
    candidates = (
        account
        for account in account_details
        if not account["rmv_land_value"] and account["rmv_imp_value"]
    )
    try:
        pick = next(candidates)
    except StopIteration:
        pick = {"account_int": None}
    for candidate in candidates:
        # Pick by most improvement value.
        if candidate["rmv_imp_value"] > pick["rmv_imp_value"]:
            pick = candidate
        # If equal improvement value, pick by lowest account number.
        elif candidate["rmv_imp_value"] == pick["rmv_imp_value"]:
            if candidate["account_int"] < pick["account_int"]:
                pick = candidate
    return pick["account_int"]


def eval_primary_account_land_value(account_details):
    """Evaluate primary account using land value only.

    Used for lots with no improvements.

    Args:
        account_details (dict): Mapping of account attribute name to value.

    Returns:
        int: Account number.
    """
    candidates = (
        account
        for account in account_details
        if account["rmv_land_value"] and not account["rmv_imp_value"]
    )
    try:
        pick = next(candidates)
    except StopIteration:
        pick = {"account_int": None}
    for candidate in candidates:
        # Pick by most land value.
        if candidate["rmv_land_value"] > pick["rmv_land_value"]:
            pick = candidate
        # If equal land value, pick by most acreage.
        elif candidate["rmv_land_value"] == pick["rmv_land_value"]:
            if candidate["land_acres"] > pick["land_acres"]:
                pick = candidate
            # If equal acreage, pick by lowest account number.
            elif candidate["land_acres"] == pick["land_acres"]:
                if candidate["account_int"] < pick["account_int"]:
                    pick = candidate
    return pick["account_int"]


def eval_primary_account_no_values(account_details):
    """Evaluate primary account using neither land or improvement value.

     Used for lots with no assessment (e.g. government property).

    Args:
        account_details (dict): Mapping of account attribute name to value.

    Returns:
        int: Account number.
     """
    candidates = (
        account
        for account in account_details
        if not account["rmv_land_value"] and not account["rmv_imp_value"]
    )
    try:
        pick = next(candidates)
    except StopIteration:
        pick = {"account_int": None}
    for candidate in candidates:
        # Pick by most acreage.
        if candidate["land_acres"] > pick["land_acres"]:
            pick = candidate
        # If equal acreage, pick by lowest account number.
        elif candidate["land_acres"] == pick["land_acres"]:
            if candidate["account_int"] < pick["account_int"]:
                pick = candidate
    return pick["account_int"]


def eval_primary_owner(owner_details):
    """Evaluate primary owner using various details.

    Args:
        account_details (dict): Mapping of account attribute name to value.

    Returns:
        int: Owner ID.
    """
    candidates = (owner for owner in owner_details if owner["name"])
    try:
        pick = next(candidates)
    except StopIteration:
        pick = {"id": None}
    for candidate in candidates:
        # Pick EWEB before City of Eugene & others.
        # EWEB often has City of Eugene listed as co-owner on account of their charter,
        # but requests EWEB get listed as primary.
        if candidate["name"].upper() == "EWEB":
            pick = candidate
        # If no EWEB, pick City of Eugene before others.
        # Per COE request. City of Eugene often has specific departments listed as
        # co-owmer, but requests the city gets listed as primary.
        elif candidate["name"].upper() == "CITY OF EUGENE":
            pick = candidate
        # If no priority owner, pick by alphanumeric order.
        elif candidate["name"] < pick["name"]:
            pick = candidate
    return pick["id"]


def geofeature_id_map():
    """Return mapping of taxlot to geofeatures ID.

    Currently, geofeature IDs are represented by the lowest object ID in
    the maintenance dataset.

    Returns:
        dict
    """
    lot_geofeature_id = {}
    for maptaxlot, oid in arcetl.attributes.as_iters(
        dataset.TAXLOT.path("maint"), ["maptaxlot", "oid@"]
    ):
        if maptaxlot not in lot_geofeature_id or lot_geofeature_id[maptaxlot] > oid:
            lot_geofeature_id[maptaxlot] = oid
    return lot_geofeature_id


def largest_exemption_map(attribute_name):
    """Return mapping of account to largest exemption's attribute.

    Args:
        attribute_name (str): Name of attribute.

    Returns:
        dict
    """
    session = database.RLID.create_session()
    try:
        exemption_query = session.query(
            RLIDExemption.account_int,
            RLIDExemption.amt,
            getattr(RLIDExemption, attribute_name),
        ).filter(
            RLIDExemption.tax_year == CURRENT_TAX_YEAR, RLIDExemption.amt is not None
        )
        pick = {}
        for account, amount, value in exemption_query:
            if account not in pick:
                pick[account] = {"amount": amount, "key": value}
            elif amount > pick[account]["amount"]:
                pick[account] = {"amount": amount, "key": value}
    finally:
        session.close()
    largest_exemption = {account: detail["key"] for account, detail in pick.items()}
    return largest_exemption


def largest_improvement_map(attribute_name):
    """Return mapping of account to largest improvement's attribute.

    Args:
        attribute_name (str): Name of attribute.

    Returns:
        dict
    """
    session = database.RLID.create_session()
    try:
        improvement_query = session.query(
            RLIDImprovement.account_int,
            RLIDImprovement.total_finish_sqft,
            getattr(RLIDImprovement, attribute_name),
        )
        pick = {}
        for account, finish_sqft, value in improvement_query:
            finish_sqft = 0 if finish_sqft is None else finish_sqft
            if account not in pick:
                pick[account] = {"finish_sqft": finish_sqft, "key": value}
            elif finish_sqft > pick[account]["finish_sqft"]:
                pick[account] = {"finish_sqft": finish_sqft, "key": value}
    finally:
        session.close()
    largest_improvement = {account: detail["key"] for account, detail in pick.items()}
    return largest_improvement


def mapnumber_map():
    """Return mapping of maptaxlot to mapnumber.

    There are a few places where a split taxlot has different mapnumbers (usually a
    supplemental map that wasn't copied onto all features). If so, assign the
    supplemental map to the taxlot.

    Returns:
        dict: Mapping of maptaxlots to mapnumber.
    """
    lot_map_numbers = defaultdict(set)
    for maptaxlot, mapnumber in arcetl.attributes.as_iters(
        dataset.TAXLOT.path("maint"), field_names=["maptaxlot", "mapnumber"]
    ):
        if mapnumber:
            lot_map_numbers[maptaxlot].add(mapnumber)
    lot_map_number = {}
    for maptaxlot, mapnumbers in lot_map_numbers.items():
        s_mapnumbers = [mapnumber for mapnumber in mapnumbers if "S" in mapnumber]
        if s_mapnumbers:
            lot_map_number[maptaxlot] = sorted(s_mapnumbers)[0]
        else:
            lot_map_number[maptaxlot] = sorted(mapnumbers)[0]
    return lot_map_number


def owner_attribute_map(attribute_name):
    """Return mapping of owner ID to attribute.

    Args:
        attribute_name (str): Name of attribute.

    Returns:
        dict
    """
    return dict(rlid_owners(include_attributes=[attribute_name]))


def owner_count_map():
    """Return mapping of account number to number of owners.

    Returns:
        dict
    """
    session = database.RLID.create_session()
    try:
        owner_count_query = session.query(
            RLIDOwner.account_int, sql.func.count(RLIDOwner.owner_id)
        ).group_by(RLIDOwner.account_int)
        owner_count = dict(owner_count_query)
    finally:
        session.close()
    return owner_count


def primary_account_map():
    """Return mapping of taxlot to primary account.

    "Primary account" is not an official designation. Rather, it is a designation made
    for RLID purposes, using a series of evaluations to assign what is considered the
    most prominent account on the lot.

    Returns:
        dict
    """
    # t_tax_year = model.
    detail_keys = ["account_int", "rmv_land_value", "rmv_imp_value", "land_acres"]
    session = database.RLID.create_session()
    try:
        valid_accounts_query = session.query(RLIDAccount.account_int).filter(
            # Active accounts.
            sql.or_(
                RLIDAccount.active_this_year == "Y",
                RLIDAccount.new_acct_active_next_year == "Y",
            ),
            # Real property accounts.
            RLIDAccount.account_type == "RP",
            # Exclude mobile home accounts.
            sql.or_(
                RLIDAccount.account_int < 4000000, RLIDAccount.account_int > 4999999
            ),
            # Exclude utility accounts.
            sql.or_(
                RLIDAccount.account_int < 8000000, RLIDAccount.account_int > 8999999
            ),
            # I can"t remember what this is for.
            RLIDAccount.account_int == RLIDTaxYear.account_int,
        )
        account_details_query = session.query(
            RLIDTaxYear.maptaxlot, *(getattr(RLIDTaxYear, key) for key in detail_keys)
        ).filter(
            RLIDTaxYear.tax_year == CURRENT_TAX_YEAR, valid_accounts_query.exists()
        )
        taxlot_account_details = defaultdict(list)
        for values in account_details_query:
            taxlot_account_details[values[0]].append(dict(zip(detail_keys, values[1:])))
    finally:
        session.close()
    primary_account = {}
    for maptaxlot, account_details in taxlot_account_details.items():
        if len(account_details) == 1:
            primary_account[maptaxlot] = account_details[0]["account_int"]
            continue

        for func in [
            # Evaluation mode order is important!
            eval_primary_account_all_values,
            eval_primary_account_land_value,
            eval_primary_account_no_values,
            eval_primary_account_imp_value,
        ]:
            primary_account[maptaxlot] = func(account_details)
            if primary_account[maptaxlot]:
                break

    return primary_account


def primary_owner_map():
    """Return mapping of account to primary owner.

    "Primary owner" is not an official designation. Rather, it's a designation made for
    RLID purposes, using a series of passes to assign what is considered the most
    prominent owner on the account.

    Returns:
        dict
    """
    account_owners = defaultdict(list)
    for _id, name, acct in rlid_owners(
        include_attributes=["owner_name", "account_int"]
    ):
        account_owners[acct].append({"id": _id, "name": name})
    primary_owner = {}
    for account, owner_details in account_owners.items():
        if len(owner_details) == 1:
            primary_owner[account] = owner_details[0]["id"]
            continue

        for func in (
            # Evaluation mode order is important!
            eval_primary_owner,
        ):
            primary_owner[account] = func(owner_details)
            if primary_owner[account]:
                break

    return primary_owner


def tax_year_account_map(attribute_name):
    """Return mapping of account to assessment attribute.

    Args:
        attribute_name (str): Name of attribute.

    Returns:
        dict
    """
    session = database.RLID.create_session()
    try:
        tax_year_query = session.query(
            RLIDTaxYear.account_int, getattr(RLIDTaxYear, attribute_name)
        ).filter(RLIDTaxYear.tax_year == CURRENT_TAX_YEAR)
        tax_year_account = dict(tax_year_query)
    finally:
        session.close()
    return tax_year_account


def tax_year_sum_map(attribute_name):
    """Return mapping of taxlot to tax year assessment attribute sum.

    Args:
        attribute_name (str): Name of attribute.

    Returns:
        dict
    """
    session = database.RLID.create_session()
    try:
        tax_year_query = session.query(
            RLIDTaxYear.maptaxlot, getattr(RLIDTaxYear, attribute_name)
        ).filter(
            RLIDTaxYear.tax_year == CURRENT_TAX_YEAR,
            # Exclude mobile home accounts.
            sql.or_(
                RLIDTaxYear.account_int < 4000000, RLIDTaxYear.account_int > 4999999
            ),
            # Exclude utility accounts.
            sql.or_(
                RLIDTaxYear.account_int < 8000000, RLIDTaxYear.account_int > 8999999
            ),
        )
        tax_year_sum = defaultdict(int)
        for taxlot, value in tax_year_query:
            tax_year_sum[taxlot] += value if value else 0
    finally:
        session.close()
    return tax_year_sum


# ETLs.


def taxlot_etl():
    """Run ETL for taxlots."""
    with arcetl.ArcETL("Taxlots") as etl:
        etl.extract(dataset.TAXLOT.path("maint"))
        # Clean maintenance values.
        transform.clean_whitespace(
            etl,
            field_names=["maptaxlot", "mapnumber", "taxlot", "oldmaplot", "oldmaplot2"],
        )
        transform.force_uppercase(etl, field_names=["mapnumber"])
        transform.clear_non_numeric_text(
            etl, field_names=["maptaxlot", "taxlot", "floor", "oldmaplot", "oldmaplot2"]
        )
        etl.transform(
            arcetl.attributes.update_by_mapping,
            field_name="mapnumber",
            mapping=mapnumber_map,
            key_field_names=["maptaxlot"],
        )
        # Dissolve on core maintenance fields (except mapacres - recalc that later).
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=[
                "maptaxlot",
                "mapnumber",
                "taxlot",
                "floor",
                "oldmaplot",
                "oldmaplot2",
            ],
            tolerance=TOLERANCE["xy"],
        )
        transform.add_missing_fields(etl, dataset.TAXLOT, tags=["pub"])
        # Set tax map number without supplemental codes.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="map",
            function=(lambda x: x[:8] if x else None),
            field_as_first_arg=False,
            arg_field_names=["mapnumber"],
        )
        # Set supplemental tax map number (if present).
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="supplemental_map",
            function=(lambda x: x if x and "S" in x else None),
            field_as_first_arg=False,
            arg_field_names=["mapnumber"],
        )
        # Assign geometry attributes.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="mapacres",
            function=(lambda x: x / 43560.0),
            field_as_first_arg=False,
            arg_field_names=["shape@area"],
        )
        coordinate_system_xy_keys = {
            2914: {"x": "xcoord", "y": "ycoord"},
            4326: {"x": "longitude", "y": "latitude"},
        }
        for spatial_reference_id, xy_key in coordinate_system_xy_keys.items():
            for axis, key in xy_key.items():
                etl.transform(
                    arcetl.attributes.update_by_geometry,
                    field_name=key,
                    spatial_reference_item=spatial_reference_id,
                    geometry_properties=["centroid", axis],
                )
        # Assign overlays.
        overlay_kwargs = [
            # City attributes.
            {
                "field_name": "geocity",
                "overlay_field_name": "inccityabbr",
                "overlay_dataset_path": dataset.INCORPORATED_CITY_LIMITS.path(),
            },
            {
                "field_name": "annexhist",
                "overlay_field_name": "annexnum",
                "overlay_dataset_path": dataset.ANNEXATION_HISTORY.path("pub"),
            },
            # Have to do overlay rather than join because some features lack codes.
            {
                "field_name": "yearanx",
                "overlay_field_name": "annexyear",
                "overlay_dataset_path": dataset.ANNEXATION_HISTORY.path("pub"),
            },
            {
                "field_name": "ugb",
                "overlay_field_name": "ugbcity",
                "overlay_dataset_path": dataset.UGB.path("pub"),
            },
            # Planning & zoning attributes.
            {
                "field_name": "greenwy",
                "overlay_field_name": "greenway",
                "overlay_dataset_path": dataset.WILLAMETTE_RIVER_GREENWAY.path("pub"),
            },
            {
                "field_name": "nodaldev",
                "overlay_field_name": "nodearea",
                "overlay_dataset_path": dataset.NODAL_DEVELOPMENT_AREA.path("pub"),
            },
            {
                "field_name": "plandes_id",
                "overlay_field_name": "plandes_id",
                "overlay_dataset_path": dataset.PLAN_DESIGNATION.path("pub"),
            },
            {
                "field_name": "zoning_id",
                "overlay_field_name": "zoning_id",
                "overlay_dataset_path": dataset.ZONING.path("pub"),
            },
            {
                "field_name": "neighbor",
                "overlay_field_name": "NEIBORHD",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "boundary\\districts\\eug\\Boundary.gdb",
                    "EugNeighborhoods",
                ),
            },
            # Public safety attributes.
            {
                "field_name": "ambulance_district",
                "overlay_field_name": "asacode",
                "overlay_dataset_path": dataset.AMBULANCE_SERVICE_AREA.path("pub"),
            },
            {
                "field_name": "firedist",
                "overlay_field_name": "fireprotprov",
                "overlay_dataset_path": dataset.FIRE_PROTECTION_AREA.path("pub"),
            },
            # Election attributes.
            {
                "field_name": "electionpr",
                "overlay_field_name": "precntnum",
                "overlay_dataset_path": dataset.ELECTION_PRECINCT.path("pub"),
            },
            {
                "field_name": "ccward",
                "overlay_field_name": "ward",
                "overlay_dataset_path": dataset.CITY_WARD.path(),
            },
            {
                "field_name": "clpud_subdivision",
                "overlay_field_name": "SUBDIVISIO",
                "overlay_dataset_path": os.path.join(
                    path.LCOG_GIS_PROJECTS,
                    "UtilityDistricts\\CentralLincolnPUD\\Redistricting2012",
                    "CLPUD_Subdivisions.shp",
                ),
            },
            {
                "field_name": "cocommdist",
                "overlay_field_name": "commrdist",
                "overlay_dataset_path": (
                    dataset.COUNTY_COMMISSIONER_DISTRICT.path("pub")
                ),
            },
            {
                "field_name": "epud",
                "overlay_field_name": "boardid",
                "overlay_dataset_path": dataset.EPUD_SUBDISTRICT.path("pub"),
            },
            {
                "field_name": "hwpud_subdivision",
                "overlay_field_name": "BoardZone",
                "overlay_dataset_path": os.path.join(
                    path.LCOG_GIS_PROJECTS,
                    "UtilityDistricts\\HecetaWaterPUD\\NewBoardSubzones",
                    "HecetaData.gdb",
                    "ScenarioB",
                ),
            },
            {
                "field_name": "lcczone",
                "overlay_field_name": "lccbrdzone",
                "overlay_dataset_path": dataset.LCC_BOARD_ZONE.path("pub"),
            },
            {
                "field_name": "senatedist",
                "overlay_field_name": "sendist",
                "overlay_dataset_path": dataset.STATE_SENATOR_DISTRICT.path("pub"),
            },
            {
                "field_name": "strepdist",
                "overlay_field_name": "repdist",
                "overlay_dataset_path": (
                    dataset.STATE_REPRESENTATIVE_DISTRICT.path("pub")
                ),
            },
            {
                "field_name": "swcd",
                "overlay_field_name": "swcdist",
                "overlay_dataset_path": (
                    dataset.SOIL_WATER_CONSERVATION_DISTRICT.path("pub")
                ),
            },
            {
                "field_name": "swcdzone",
                "overlay_field_name": "swczone",
                "overlay_dataset_path": (
                    dataset.SOIL_WATER_CONSERVATION_DISTRICT.path("pub")
                ),
            },
            # Education attributes.
            {
                "field_name": "schooldist",
                "overlay_field_name": "district",
                "overlay_dataset_path": dataset.SCHOOL_DISTRICT.path("pub"),
            },
            {
                "field_name": "elem",
                "overlay_field_name": "attend",
                "overlay_dataset_path": dataset.ELEMENTARY_SCHOOL_AREA.path("pub"),
            },
            {
                "field_name": "middle",
                "overlay_field_name": "attend",
                "overlay_dataset_path": dataset.MIDDLE_SCHOOL_AREA.path("pub"),
            },
            {
                "field_name": "high",
                "overlay_field_name": "attend",
                "overlay_dataset_path": dataset.HIGH_SCHOOL_AREA.path("pub"),
            },
            # Transportation attributes.
            {
                "field_name": "cats",
                "overlay_field_name": "CATSBNDY",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "transport\\eug", "catsbndy.shp"
                ),
            },
            {
                "field_name": "ltddist",
                "overlay_field_name": "LTD",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "transport\\ltd", "2012 LTD Boundary.shp"
                ),
            },
            {
                "field_name": "ltdridesrc",
                "overlay_field_name": "LTD",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "transport\\ltd", "2015 RideSource Boundary.shp"
                ),
            },
            {
                "field_name": "trans_analysis_zone",
                "overlay_field_name": "TAZ_NUM",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "transport", "MTAZ16.shp"
                ),
            },
            # Natural attributes.
            {
                "field_name": "firmnumber",
                "overlay_field_name": "firm_pan",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\flood\\Flood.gdb", "FIRMPanel"
                ),
            },
            {
                "field_name": "soilkey",
                "overlay_field_name": "mukey",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\soils\\Soils.gdb", "Soil"
                ),
            },
            {
                "field_name": "wetland",
                "overlay_field_name": "WET_TYPE",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\eug\\Wetland", "wetlands.shp"
                ),
            },
            # Census attributes.
            {
                "field_name": "ctract",
                "overlay_field_name": "TRACT",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "federal\\census\\lane\\2010\\lc_census2010.gdb",
                    "lc_tracts2010",
                ),
            },
            {
                "field_name": "blockgr",
                "overlay_field_name": "BlockGroup",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "federal\\census\\lane\\2010\\lc_census2010.gdb",
                    "lc_blockgroups2010",
                ),
            },
        ]
        for kwargs in overlay_kwargs:
            etl.transform(
                arcetl.attributes.update_by_overlay,
                overlay_central_coincident=True,
                **kwargs
            )
        # Clean overlay values.
        transform.clean_whitespace(
            etl, field_names=["wetland", "ctract", "blockgr", "neighbor"]
        )
        transform.force_uppercase(etl, field_names=["cats", "ltddist", "ltdridesrc"])
        # Set default overlay values where missing.
        transform.force_yn(
            etl, field_names=["greenwy", "cats", "ltddist", "ltdridesrc"], default="N"
        )
        # Remove invalid overlay values.
        transform.clear_nonpositive(etl, field_names=["ctract", "blockgr"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="neighbor",
            function=(lambda x: x if x and int(x) != 99 else None),
        )
        # Assign joinable field values after overlays.
        join_kwargs = [
            # City attributes.
            {
                "field_name": "geocity_name",
                "join_field_name": "inccityname",
                "join_dataset_path": dataset.INCORPORATED_CITY_LIMITS.path(),
                "on_field_pairs": [("geocity", "inccityabbr")],
            },
            {
                "field_name": "ugb_name",
                "join_field_name": "ugbcityname",
                "join_dataset_path": dataset.UGB.path("pub"),
                "on_field_pairs": [("ugb", "ugbcity")],
            },
            # Planning & zoning attributes.
            {
                "field_name": "neighborhood_name",
                "join_field_name": "NAME",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "boundary\\districts\\eug\\Boundary.gdb",
                    "EugNeighborhoods",
                ),
                "on_field_pairs": [("neighbor", "NEIBORHD")],
            },
            {
                "field_name": "nodaldev_name",
                "join_field_name": "nodename",
                "join_dataset_path": dataset.NODAL_DEVELOPMENT_AREA.path("pub"),
                "on_field_pairs": [("nodaldev", "nodearea")],
            },
            {
                "field_name": "numlanduse",
                "join_field_name": "landusecount",
                "join_dataset_path": dataset.LAND_USE_AREA.path("pub"),
                "on_field_pairs": [("maptaxlot", "maptaxlot")],
            },
            {
                "field_name": "plandesjuris",
                "join_field_name": "planjuris",
                "join_dataset_path": dataset.PLAN_DESIGNATION.path("pub"),
                "on_field_pairs": [("plandes_id", "plandes_id")],
            },
            {
                "field_name": "plandes",
                "join_field_name": "plandes",
                "join_dataset_path": dataset.PLAN_DESIGNATION.path("pub"),
                "on_field_pairs": [("plandes_id", "plandes_id")],
            },
            {
                "field_name": "plandesdesc",
                "join_field_name": "plandesnam",
                "join_dataset_path": dataset.PLAN_DESIGNATION.path("pub"),
                "on_field_pairs": [("plandes_id", "plandes_id")],
            },
            {
                "field_name": "zoningjuris",
                "join_field_name": "zonejuris",
                "join_dataset_path": dataset.ZONING.path("pub"),
                "on_field_pairs": [("zoning_id", "zoning_id")],
            },
            {
                "field_name": "zoning",
                "join_field_name": "zonecode",
                "join_dataset_path": dataset.ZONING.path("pub"),
                "on_field_pairs": [("zoning_id", "zoning_id")],
            },
            {
                "field_name": "zoningdesc",
                "join_field_name": "zonename",
                "join_dataset_path": dataset.ZONING.path("pub"),
                "on_field_pairs": [("zoning_id", "zoning_id")],
            },
            {
                "field_name": "subarea",
                "join_field_name": "subarea",
                "join_dataset_path": dataset.ZONING.path("pub"),
                "on_field_pairs": [("zoning_id", "zoning_id")],
            },
            {
                "field_name": "overlay1",
                "join_field_name": "overlay1",
                "join_dataset_path": dataset.ZONING.path("pub"),
                "on_field_pairs": [("zoning_id", "zoning_id")],
            },
            {
                "field_name": "overlay2",
                "join_field_name": "overlay2",
                "join_dataset_path": dataset.ZONING.path("pub"),
                "on_field_pairs": [("zoning_id", "zoning_id")],
            },
            {
                "field_name": "overlay3",
                "join_field_name": "overlay3",
                "join_dataset_path": dataset.ZONING.path("pub"),
                "on_field_pairs": [("zoning_id", "zoning_id")],
            },
            {
                "field_name": "overlay4",
                "join_field_name": "overlay4",
                "join_dataset_path": dataset.ZONING.path("pub"),
                "on_field_pairs": [("zoning_id", "zoning_id")],
            },
            # Public safety attributes.
            {
                "field_name": "ambulance_service_area",
                "join_field_name": "asa",
                "join_dataset_path": dataset.AMBULANCE_SERVICE_AREA.path("pub"),
                "on_field_pairs": [("ambulance_district", "asacode")],
            },
            {
                "field_name": "ambulance_service_provider",
                "join_field_name": "provider",
                "join_dataset_path": dataset.AMBULANCE_SERVICE_AREA.path("pub"),
                "on_field_pairs": [("ambulance_district", "asacode")],
            },
            {
                "field_name": "fire_protection_provider",
                "join_field_name": "fpprovname",
                "join_dataset_path": dataset.FIRE_PROTECTION_AREA.path("pub"),
                "on_field_pairs": [("firedist", "fireprotprov")],
            },
            # Election attributes.
            {
                "field_name": "city_councilor",
                "join_field_name": "councilor",
                "join_dataset_path": dataset.CITY_WARD.path(),
                "on_field_pairs": [("ccward", "ward")],
            },
            {
                "field_name": "cocommdist_name",
                "join_field_name": "cmdistname",
                "join_dataset_path": dataset.COUNTY_COMMISSIONER_DISTRICT.path("pub"),
                "on_field_pairs": [("cocommdist", "commrdist")],
            },
            {
                "field_name": "county_commissioner",
                "join_field_name": "commrname",
                "join_dataset_path": dataset.COUNTY_COMMISSIONER_DISTRICT.path("pub"),
                "on_field_pairs": [("cocommdist", "commrdist")],
            },
            {
                "field_name": "eweb_commissioner_name",
                "join_field_name": "eweb_commissioner_name",
                "join_dataset_path": dataset.EWEB_COMMISSIONER.path("pub"),
                "on_field_pairs": [("ccward", "city_council_ward")],
            },
            {
                "field_name": "state_representative",
                "join_field_name": "repname",
                "join_dataset_path": dataset.STATE_REPRESENTATIVE_DISTRICT.path("pub"),
                "on_field_pairs": [("strepdist", "repdist")],
            },
            {
                "field_name": "state_senator",
                "join_field_name": "senname",
                "join_dataset_path": dataset.STATE_SENATOR_DISTRICT.path("pub"),
                "on_field_pairs": [("senatedist", "sendist")],
            },
            # Education attributes.
            {
                "field_name": "schooldist_name",
                "join_field_name": "names",
                "join_dataset_path": dataset.SCHOOL_DISTRICT.path("pub"),
                "on_field_pairs": [("schooldist", "district")],
            },
            {
                "field_name": "elem_name",
                "join_field_name": "elem_school",
                "join_dataset_path": dataset.ELEMENTARY_SCHOOL_AREA.path("pub"),
                "on_field_pairs": [("elem", "attend")],
            },
            {
                "field_name": "middle_name",
                "join_field_name": "middle_school",
                "join_dataset_path": dataset.MIDDLE_SCHOOL_AREA.path("pub"),
                "on_field_pairs": [("middle", "attend")],
            },
            {
                "field_name": "high_name",
                "join_field_name": "high_school",
                "join_dataset_path": dataset.HIGH_SCHOOL_AREA.path("pub"),
                "on_field_pairs": [("high", "attend")],
            },
            # Natural attributes.
            {
                "field_name": "firmprinted",
                "join_field_name": "panel_printed",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\flood\\Flood.gdb", "FIRMPanel"
                ),
                "on_field_pairs": [("firmnumber", "firm_pan")],
            },
            {
                "field_name": "firm_community_id",
                "join_field_name": "com_nfo_id",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\flood\\Flood.gdb", "CommunityInfo"
                ),
                "on_field_pairs": [("geocity", "community_code")],
            },
            {
                "field_name": "firm_community_post_firm_date",
                "join_field_name": "in_frm_dat",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\flood\\Flood.gdb", "CommunityInfo"
                ),
                "on_field_pairs": [("geocity", "community_code")],
            },
            {
                "field_name": "soiltype",
                "join_field_name": "musym",
                "join_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\soils\\Soils.gdb", "MUAggAtt"
                ),
                "on_field_pairs": [("soilkey", "mukey")],
            },
        ]
        for kwargs in join_kwargs:
            etl.transform(arcetl.attributes.update_by_joined_value, **kwargs)
        # Clean join values.
        transform.clean_whitespace(etl, field_names=["neighborhood_name"])
        # Remove Metro Plan designations, per City of Eugene request.
        transform.clear_all_values(
            etl,
            field_names=["plandes", "plandesdesc"],
            dataset_where_sql="plandesjuris = 'MTP'",
        )
        # Build values from functions.
        function_kwargs = [
            {
                "field_name": "maptaxlot_hyphen",
                "function": maptaxlot_separated,
                "arg_field_names": ["maptaxlot"],
            },
            {
                "field_name": "rlid_link",
                "function": (url.RLID_PROPERTY_SEARCH + "&as_maplot={}").format,
                "arg_field_names": ["maptaxlot"],
                "dataset_where_sql": REAL_LOT_SQL,
            },
        ]
        for kwargs in function_kwargs:
            etl.transform(
                arcetl.attributes.update_by_function, field_as_first_arg=False, **kwargs
            )
        # Build values from mappings.
        mapping_kwargs = [
            {
                "field_name": "taxlot_geofeature_id",
                "mapping": geofeature_id_map,
                "key_field_names": ["maptaxlot"],
            },
            {
                "field_name": "numaccnts",
                "mapping": account_count_map,
                "key_field_names": ["maptaxlot"],
                "default_value": 0,
            },
            {
                "field_name": "account_int",
                "mapping": primary_account_map,
                "key_field_names": ["maptaxlot"],
            },
            {
                "field_name": "numowners",
                "mapping": owner_count_map,
                "key_field_names": ["account_int"],
                "default_value": 0,
            },
            {
                "field_name": "owner_id",
                "mapping": primary_owner_map,
                "key_field_names": ["account_int"],
            },
            {
                "field_name": "ownname",
                "mapping": functools.partial(owner_attribute_map, "owner_name"),
                "key_field_names": ["owner_id"],
            },
            {
                "field_name": "addr1",
                "mapping": functools.partial(owner_attribute_map, "addr_line1"),
                "key_field_names": ["owner_id"],
            },
            {
                "field_name": "addr2",
                "mapping": functools.partial(owner_attribute_map, "addr_line2"),
                "key_field_names": ["owner_id"],
            },
            {
                "field_name": "addr3",
                "mapping": functools.partial(owner_attribute_map, "addr_line3"),
                "key_field_names": ["owner_id"],
            },
            {
                "field_name": "ownercity",
                "mapping": functools.partial(owner_attribute_map, "city"),
                "key_field_names": ["owner_id"],
            },
            {
                "field_name": "ownerprvst",
                "mapping": functools.partial(owner_attribute_map, "prov_state"),
                "key_field_names": ["owner_id"],
            },
            {
                "field_name": "ownerzip",
                "mapping": functools.partial(owner_attribute_map, "zip_code"),
                "key_field_names": ["owner_id"],
            },
            {
                "field_name": "ownercntry",
                "mapping": functools.partial(owner_attribute_map, "country"),
                "key_field_names": ["owner_id"],
            },
            {
                "field_name": "landval",
                "mapping": functools.partial(tax_year_sum_map, "rmv_land_value"),
                "key_field_names": ["maptaxlot"],
            },
            {
                "field_name": "impval",
                "mapping": functools.partial(tax_year_sum_map, "rmv_imp_value"),
                "key_field_names": ["maptaxlot"],
            },
            {
                "field_name": "totval",
                "mapping": functools.partial(tax_year_sum_map, "rmv_total_value"),
                "key_field_names": ["maptaxlot"],
            },
            {
                "field_name": "assdtotval",
                "mapping": functools.partial(tax_year_sum_map, "assd_total_value"),
                "key_field_names": ["maptaxlot"],
            },
            {
                "field_name": "exm_amt_reg_value",
                "mapping": functools.partial(tax_year_sum_map, "exm_amt_reg_value"),
                "key_field_names": ["maptaxlot"],
            },
            {
                "field_name": "taxable_value",
                "mapping": functools.partial(tax_year_sum_map, "taxable_value"),
                "key_field_names": ["maptaxlot"],
            },
            {
                "field_name": "acctno",
                "mapping": functools.partial(tax_year_account_map, "account_stripped"),
                "key_field_names": ["account_int"],
            },
            {
                "field_name": "taxcode",
                "mapping": functools.partial(tax_year_account_map, "tca"),
                "key_field_names": ["account_int"],
            },
            {
                "field_name": "txcdspl",
                "mapping": functools.partial(tax_year_account_map, "code_split_ind"),
                "key_field_names": ["account_int"],
                "default_value": "N",
            },
            {
                "field_name": "propcl",
                "mapping": functools.partial(tax_year_account_map, "prop_class"),
                "key_field_names": ["account_int"],
            },
            {
                "field_name": "propcldes",
                "mapping": functools.partial(tax_year_account_map, "prop_class_desc"),
                "key_field_names": ["account_int"],
            },
            {
                "field_name": "statcl",
                "mapping": functools.partial(tax_year_account_map, "stat_class"),
                "key_field_names": ["account_int"],
            },
            {
                "field_name": "statcldes",
                "mapping": functools.partial(tax_year_account_map, "stat_class_desc"),
                "key_field_names": ["account_int"],
            },
            {
                "field_name": "exemptdesc",
                "mapping": functools.partial(largest_exemption_map, "description"),
                "key_field_names": ["account_int"],
            },
            {
                "field_name": "bldgtype",
                "mapping": functools.partial(largest_improvement_map, "bldg_type"),
                "key_field_names": ["account_int"],
            },
            {
                "field_name": "yearblt",
                "mapping": functools.partial(largest_improvement_map, "year_built"),
                "key_field_names": ["account_int"],
            },
        ]
        for kwargs in mapping_kwargs:
            etl.transform(arcetl.attributes.update_by_mapping, **kwargs)
        etl.load(dataset.TAXLOT.path("pub"))


def taxlot_line_etl():
    """Run ETL for taxlot lines."""
    with arcetl.ArcETL("Taxlot Lines") as etl:
        etl.extract(dataset.TAXLOT_LINE.path("maint"))
        transform.add_missing_fields(etl, dataset.TAXLOT_LINE, tags=["pub"])
        # Set the line type description.
        etl.transform(
            arcetl.attributes.update_by_domain_code,
            field_name="linetypedesc",
            code_field_name="linetype",
            domain_name="LineType",
            domain_workspace_path=database.REGIONAL.path,
        )
        etl.load(dataset.TAXLOT_LINE.path("pub"))


# Jobs.


WEEKLY_JOB = Job("Taxlot_Datasets", etls=[taxlot_etl, taxlot_line_etl])


# Execution.


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument("pipelines", nargs="*", help="Pipeline(s) to run")
    available_names = {key for key in list(globals()) if not key.startswith("__")}
    pipeline_names = args.parse_args().pipelines
    if pipeline_names and available_names.issuperset(pipeline_names):
        pipelines = [globals()[arg] for arg in args.parse_args().pipelines]
        for pipeline in pipelines:
            execute_pipeline(pipeline)
    else:
        console = logging.StreamHandler()
        LOG.addHandler(console)
        if not pipeline_names:
            LOG.error("No pipeline arguments.")
        for arg in pipeline_names:
            if arg not in available_names:
                LOG.error("`%s` not available in exec.", arg)
        LOG.error(
            "Available objects in exec: %s",
            ", ".join("`{}`".format(name) for name in sorted(available_names)),
        )


if __name__ == "__main__":
    main()
