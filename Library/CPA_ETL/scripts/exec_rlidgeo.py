"""Execution code for RLIDGeo warehouse items."""
import argparse
from collections import Counter
from copy import copy
import datetime
import logging
import os
import tempfile
import uuid

import arcetl
import arcpy
from etlassist.pipeline import Job, execute_pipeline

from helper.communicate import send_email
from helper import database
from helper import dataset
from helper.misc import IGNORE_PATTERNS_RLIDGEO_SNAPSHOT, datestamp, taxlot_prefixes
from helper import path
from helper import transform


##TODO: Alter snapshot to go in a year-bucket folder.

LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

MESSAGE = {
    "skip_internal": "SKIPPING {dataset_name}: Internal application dataset.",
    "skip_no_changes": "SKIPPING {dataset_name}: No changes after initial load.",
    "skip_other": "SKIPPING {dataset_name}: Updated via other means.",
    "skip_view": "SKIPPING {dataset_name}: This is a view.",
}
"""dict: Mapping of keys to formatted message strings."""
DATASET_KWARGS = {}
"""dict: Tagged mappings of dataset name to keyword arguments for update."""
DATASET_KWARGS["primary"] = {
    "dbo.Road": {
        "update_last_load": True,
        "source_path": dataset.ROAD.path("pub"),
        "source_where_sql": "county = 'Lane'",
        "id_field_names": dataset.ROAD.id_field_names,
    },
    "dbo.Site_Address": {
        "update_last_load": True,
        "source_path": dataset.SITE_ADDRESS.path("pub"),
        "source_where_sql": "archived = 'N' and valid = 'Y'",
        "id_field_names": dataset.SITE_ADDRESS.id_field_names,
    },
    "dbo.Taxlot": {
        "update_last_load": True,
        "source_path": dataset.TAXLOT.path("pub"),
        "id_field_names": dataset.TAXLOT.id_field_names,
    },
}
DATASET_KWARGS["secondary"] = {
    "dbo.AmbulanceServiceArea": {
        "source_path": dataset.AMBULANCE_SERVICE_AREA.path("pub"),
        "id_field_names": dataset.AMBULANCE_SERVICE_AREA.id_field_names,
    },
    "dbo.AnnexHist": {
        "source_path": dataset.ANNEXATION_HISTORY.path("pub"),
        # No unique ID: use all fields.
        "id_field_names": dataset.ANNEXATION_HISTORY.field_names,
    },
    "dbo.BikeFacility": {
        "source_path": dataset.BIKE_FACILITY.path("pub"),
        "id_field_names": dataset.BIKE_FACILITY.id_field_names,
    },
    "dbo.Bridge": {
        "source_path": dataset.BRIDGE.path("pub"),
        "id_field_names": dataset.BRIDGE.id_field_names,
    },
    "dbo.Building": {
        "source_path": dataset.BUILDING.path("pub"),
        "id_field_names": dataset.BUILDING.id_field_names,
    },
    "dbo.CityWard": {
        "source_path": dataset.CITY_WARD.path(),
        "id_field_names": dataset.CITY_WARD.id_field_names,
    },
    "dbo.CobZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'COB'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.CotZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'COT'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.CountyBoundary": {
        "source_path": dataset.COUNTY_BOUNDARY.path(),
        "id_field_names": dataset.COUNTY_BOUNDARY.id_field_names,
    },
    "dbo.CountyCommissionerDist": {
        "source_path": dataset.COUNTY_COMMISSIONER_DISTRICT.path("pub"),
        "id_field_names": dataset.COUNTY_COMMISSIONER_DISTRICT.id_field_names,
    },
    "dbo.CreZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'CRE'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.DunZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'DUN'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.ElectionPrecinct": {
        "source_path": dataset.ELECTION_PRECINCT.path("pub"),
        "id_field_names": dataset.ELECTION_PRECINCT.id_field_names,
    },
    "dbo.Elementary_School_Areas": {
        "source_path": dataset.ELEMENTARY_SCHOOL_AREA.path("pub"),
        "id_field_names": dataset.ELEMENTARY_SCHOOL_AREA.id_field_names,
    },
    "dbo.Elementary_School_Lines": {
        "source_path": dataset.ELEMENTARY_SCHOOL_LINE.path(),
        # No fields.
        "id_field_names": ["shape@wkb"],
    },
    "dbo.ElevationContour20Ft": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.EmergencyServiceZone": {
        "source_path": dataset.EMERGENCY_SERVICE_ZONE.path(),
        "id_field_names": dataset.EMERGENCY_SERVICE_ZONE.id_field_names,
    },
    "dbo.EPUDSubdistrict": {
        "source_path": dataset.EPUD_SUBDISTRICT.path("pub"),
        "id_field_names": dataset.EPUD_SUBDISTRICT.id_field_names,
    },
    "dbo.EugZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'EUG'",
        # Unlike other city zoning, Eugene has `subarea` as part of ID.
        "id_field_names": dataset.ZONING.id_field_names + ["subarea"],
    },
    "dbo.EWEBCommissioner": {
        "source_path": dataset.EWEB_COMMISSIONER.path("pub"),
        "id_field_names": dataset.EWEB_COMMISSIONER.id_field_names,
    },
    "dbo.FireProtectionArea": {
        "source_path": dataset.FIRE_PROTECTION_AREA.path("pub"),
        "id_field_names": dataset.FIRE_PROTECTION_AREA.id_field_names,
    },
    "dbo.FloZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'FLO'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.High_School_Areas": {
        "source_path": dataset.HIGH_SCHOOL_AREA.path("pub"),
        "id_field_names": dataset.HIGH_SCHOOL_AREA.id_field_names,
    },
    "dbo.High_School_Lines": {
        "source_path": dataset.HIGH_SCHOOL_LINE.path(),
        # No fields.
        "id_field_names": ["shape@wkb"],
    },
    "dbo.IncCityLimits": {
        "source_path": dataset.INCORPORATED_CITY_LIMITS.path(),
        "id_field_names": dataset.INCORPORATED_CITY_LIMITS.id_field_names,
    },
    "dbo.JunZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'JUN'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.LandUse": {
        "source_path": dataset.LAND_USE_AREA.path("pub"),
        "id_field_names": dataset.LAND_USE_AREA.id_field_names,
    },
    "dbo.LanduseCodesDetailed": {
        "source_path": dataset.LAND_USE_CODES_DETAILED.path("pub"),
        "id_field_names": dataset.LAND_USE_CODES_DETAILED.id_field_names,
    },
    "dbo.LanduseCodesUsecodes": {
        "source_path": dataset.LAND_USE_CODES_USE_CODES.path("pub"),
        "id_field_names": dataset.LAND_USE_CODES_USE_CODES.id_field_names,
    },
    "dbo.LCCBoardZone": {
        "source_path": dataset.LCC_BOARD_ZONE.path("pub"),
        "id_field_names": dataset.LCC_BOARD_ZONE.id_field_names,
    },
    "dbo.LowZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'LOW'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.Metadata_Dataset_Currency": {"no_update_message": MESSAGE["skip_view"]},
    "dbo.Metadata_Dataset_Update": {"no_update_message": MESSAGE["skip_other"]},
    "dbo.MetroPlanBoundary": {
        "source_path": dataset.METRO_PLAN_BOUNDARY.path("pub"),
        "id_field_names": dataset.METRO_PLAN_BOUNDARY.id_field_names,
    },
    "dbo.Middle_School_Areas": {
        "source_path": dataset.MIDDLE_SCHOOL_AREA.path("pub"),
        "id_field_names": dataset.MIDDLE_SCHOOL_AREA.id_field_names,
    },
    "dbo.Middle_School_Lines": {
        "source_path": dataset.MIDDLE_SCHOOL_LINE.path(),
        # No fields.
        "id_field_names": ["shape@wkb"],
    },
    "dbo.MSAG_Range": {"no_update_message": MESSAGE["skip_other"]},
    "dbo.NodalDevelopmentArea": {
        "source_path": dataset.NODAL_DEVELOPMENT_AREA.path("pub"),
        "id_field_names": dataset.NODAL_DEVELOPMENT_AREA.id_field_names,
    },
    "dbo.OakZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'OAK'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.PlanDesignation": {
        "source_path": dataset.PLAN_DESIGNATION.path("pub"),
        # Omit county designations from RLIDGeo.
        "source_where_sql": "planjuris <> 'LC'",
        "id_field_names": dataset.PLAN_DESIGNATION.id_field_names,
    },
    "dbo.Plat": {
        "source_path": dataset.PLAT.path("pub"),
        "id_field_names": dataset.PLAT.id_field_names,
    },
    "dbo.PLSSDLC": {
        "source_path": dataset.PLSS_DLC.path("pub"),
        "id_field_names": dataset.PLSS_DLC.id_field_names,
    },
    "dbo.PLSSQuarterSection": {
        "source_path": dataset.PLSS_QUARTER_SECTION.path("pub"),
        "id_field_names": dataset.PLSS_QUARTER_SECTION.id_field_names,
    },
    "dbo.PLSSSection": {
        "source_path": dataset.PLSS_SECTION.path("pub"),
        "id_field_names": dataset.PLSS_SECTION.id_field_names,
    },
    "dbo.PLSSTownship": {
        "source_path": dataset.PLSS_TOWNSHIP.path("pub"),
        "id_field_names": dataset.PLSS_TOWNSHIP.id_field_names,
    },
    "dbo.PSAPArea": {
        "source_path": dataset.PSAP_AREA.path("pub"),
        "id_field_names": dataset.PSAP_AREA.id_field_names,
    },
    "dbo.RoadGeneralArterial": {
        "source_path": dataset.ROAD_GENERAL_ARTERIAL.path(),
        # No unique ID + intersection breaks.
        "id_field_names": dataset.ROAD_GENERAL_ARTERIAL.field_names + ["shape@wkb"],
    },
    "dbo.RoadHighway": {
        "source_path": dataset.ROAD_HIGHWAY.path(),
        # No unique ID + intersection breaks.
        "id_field_names": dataset.ROAD_HIGHWAY.field_names + ["shape@wkb"],
    },
    "dbo.RoadIntersection": {
        "source_path": dataset.ROAD_INTERSECTION.path(),
        "id_field_names": dataset.ROAD_INTERSECTION.id_field_names,
    },
    "dbo.School_Districts": {
        "source_path": dataset.SCHOOL_DISTRICT.path("pub"),
        "id_field_names": dataset.SCHOOL_DISTRICT.id_field_names,
    },
    "dbo.SDE_compress_log": {"no_update_message": MESSAGE["skip_internal"]},
    "dbo.SoilWaterConservationDist": {
        "source_path": dataset.SOIL_WATER_CONSERVATION_DISTRICT.path("pub"),
        "id_field_names": dataset.SOIL_WATER_CONSERVATION_DISTRICT.id_field_names,
    },
    "dbo.SprZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'SPR'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.StateRepDist": {
        "source_path": dataset.STATE_REPRESENTATIVE_DISTRICT.path("pub"),
        "id_field_names": dataset.STATE_REPRESENTATIVE_DISTRICT.id_field_names,
    },
    "dbo.StateSenDist": {
        "source_path": dataset.STATE_SENATOR_DISTRICT.path("pub"),
        "id_field_names": dataset.STATE_SENATOR_DISTRICT.id_field_names,
    },
    "dbo.TaxCodeArea": {
        "source_path": dataset.TAX_CODE_AREA.path("pub"),
        # No unique ID (unless can clean `source` + drop `yearcreated`).
        "id_field_names": dataset.TAX_CODE_AREA.field_names,
    },
    "dbo.TaxCodeArea_2009_07_31": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeArea_2010_07_30": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeArea_2011_07_29": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeArea_2012_08_10": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeArea_2013_08_05": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeArea_2014_08_21": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeArea_2015_08_21": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeArea_2016_08_15": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeArea_2017_08_28": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeArea_2018_08_29": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxCodeDetail": {
        "source_path": dataset.TAX_CODE_DETAIL.path("pub"),
        "id_field_names": dataset.TAX_CODE_DETAIL.id_field_names,
    },
    "dbo.Taxlot_2009_07_31": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.Taxlot_2010_07_30": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.Taxlot_2011_07_29": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.Taxlot_2012_08_10": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.Taxlot_2013_08_05": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.Taxlot_2014_08_21": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.Taxlot_2015_08_21": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.Taxlot_2016_08_15": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.Taxlot_2017_08_28": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.Taxlot_2018_08_29": {"no_update_message": MESSAGE["skip_no_changes"]},
    "dbo.TaxlotFireProtection": {
        "source_path": dataset.TAXLOT_FIRE_PROTECTION.path(),
        "subset_where_sqls": (
            "maptaxlot like '{}%'".format(prefix) for prefix in taxlot_prefixes(2)
        ),
        "id_field_names": dataset.TAXLOT_FIRE_PROTECTION.id_field_names,
    },
    "dbo.TaxlotFloodHazard": {
        "source_path": dataset.TAXLOT_FLOOD_HAZARD.path(),
        "subset_where_sqls": (
            "maptaxlot like '{}%'".format(prefix) for prefix in taxlot_prefixes(2)
        ),
        "id_field_names": dataset.TAXLOT_FLOOD_HAZARD.id_field_names,
    },
    "dbo.TaxlotLines": {
        "source_path": dataset.TAXLOT_LINE.path("pub"),
        # No unique ID + crossing breaks.
        # "id_field_names": dataset.TAXLOT_LINE.field_names + ["shape@wkb"],
    },
    "dbo.TaxlotOwnerRealActive": {
        "source_path": dataset.TAXLOT_OWNER.path("pub"),
        # No distinct ID (some owners have multiple entries with slight variation).
        "id_field_names": dataset.TAXLOT_OWNER.field_names,
    },
    "dbo.TaxlotSoil": {
        "source_path": dataset.TAXLOT_SOIL.path(),
        "subset_where_sqls": (
            "maptaxlot like '{}%'".format(prefix) for prefix in taxlot_prefixes(2)
        ),
        "id_field_names": dataset.TAXLOT_SOIL.id_field_names,
    },
    "dbo.TaxlotZoning": {
        "source_path": dataset.TAXLOT_ZONING.path(),
        "subset_where_sqls": (
            "maptaxlot like '{}%'".format(prefix) for prefix in taxlot_prefixes(2)
        ),
        "id_field_names": dataset.TAXLOT_ZONING.id_field_names,
    },
    "dbo.UGB": {
        "source_path": dataset.UGB.path("pub"),
        "id_field_names": dataset.UGB.id_field_names,
    },
    "dbo.UGBLine": {
        "source_path": dataset.UGB_LINE.path(),
        # No fields.
        "id_field_names": ["shape@wkb"],
    },
    "dbo.VenZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'VEN'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.WesZoning": {
        "source_path": dataset.ZONING.path("pub"),
        "source_where_sql": "zonejuris = 'WES'",
        "id_field_names": dataset.ZONING.id_field_names,
    },
    "dbo.WillametteRiverGreenway": {
        "source_path": dataset.WILLAMETTE_RIVER_GREENWAY.path("pub"),
        "id_field_names": dataset.WILLAMETTE_RIVER_GREENWAY.id_field_names,
    },
    "dbo.ZIPCode": {
        "source_path": dataset.ZIP_CODE_AREA.path("pub"),
        "id_field_names": dataset.ZIP_CODE_AREA.id_field_names,
    },
}
"""dict: Mapping of dataset name to keyword arguments for update."""
KWARGS_ISSUES_MESSAGE = {
    "recipients": ["jblair@lcog.org"],
    "reply_to": "jblair@lcog.org",
}
"""dict: Issues message keyword arguments for etlassist.send_email."""


# Helpers.


def record_dataset_update(
    dataset_name, feature_count, metadata_path, checked=None, truncate_and_load=False
):
    """Record line in update metadata about update to given dataset.

    Args:
        metadata_path (str): Path to the update metadata table.
        dataset_name (str): Name of the dataset.
        feature_count (collections.Counter): Counter of features for each
            update type.
        checked (datetime.datetime): Timestamp for when dataset was checked
            for updating. If checked is None, will use current date-time.
        truncate_and_load (bool): True if update was a truncate-and-load style of
            update, False otherwise. If True, will omit deleted rows from
            updated_row_count.
    """
    LOG.info("Recording update metadata for %s.", dataset_name)
    row = {
        "update_id": "{" + str(uuid.uuid4()) + "}",
        "dataset_name": dataset_name,
        "checked": checked if checked else datetime.datetime.now(),
    }
    for action in arcetl.features.UPDATE_TYPES:
        row[action + "_row_count"] = feature_count[action]
    for meta_action, actions in {
        "total": ["inserted", "altered", "unchanged"],
        "updated": ["deleted", "inserted", "altered"],
    }.items():
        row[meta_action + "_row_count"] = sum(
            feature_count[action] for action in actions
        )
    # This style of updating will duplicate the values if deletes treated as updates.
    if truncate_and_load:
        row["updated_row_count"] -= feature_count["deleted"]
    arcetl.features.insert_from_dicts(
        metadata_path, insert_features=[row], field_names=row.keys(), log_level=None
    )


def update_repository_dataset(repository, dataset_name, **kwargs):
    """Update dataset in repository.

    Args:
        repository (etlassist.Database): Repository database object instance.
        dataset_name (str): Name of dataset to update.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        source_path (str): Path of source dataset.
        source_where_sql (str): SQL where-clause for source dataset subselection.
        id_field_names (iter): Feature identifier fields.
        no_update_message (str): Text for message indicating dataset gets no updates,
            formatted to use dataset keyword arguments (if necessary).
        use_edit_session (bool): Updates are done in an edit session if True. Default is
            False.

    Returns:
        collections.Counter: Counts for each update type.
    """
    dataset_path = os.path.join(repository.path, dataset_name)
    kwargs.setdefault("dataset_name", dataset_name)
    kwargs.setdefault("use_edit_session", False)
    feature_count = Counter()
    if "no_update_message" in kwargs:
        LOG.info(kwargs["no_update_message"].format(**kwargs))
        feature_count["unchanged"] = arcetl.dataset.feature_count(dataset_path)
        return feature_count

    elif "id_field_names" in kwargs:
        kwargs["update_dataset_path"] = kwargs.pop("source_path")
        kwargs["update_where_sql"] = kwargs.pop("source_where_sql", None)
        feature_count.update(arcetl.features.update_from_path(dataset_path, **kwargs))
    else:
        kwargs["insert_dataset_path"] = kwargs.pop("source_path")
        kwargs["insert_where_sql"] = kwargs.pop("source_where_sql", None)
        feature_count.update(arcetl.features.delete(dataset_path))
        feature_count.update(arcetl.features.insert_from_path(dataset_path, **kwargs))
    record_dataset_update(
        dataset_name,
        feature_count,
        metadata_path=os.path.join(repository.path, "dbo.Metadata_Dataset_Update"),
        truncate_and_load=("id_field_names" not in kwargs),
    )
    return feature_count


# ETLs.


##TODO: Add `ignore_last_load` argument.
def datasets_update(dataset_keys=["primary", "secondary"]):
    """Run update for RLIDGeo warehouse datasets."""
    repository = {"current": database.RLIDGEO, "last_load": database.RLIDGEO_LASTLOAD}
    LOG.info("Start: Update datasets in %s warehouse.", repository["current"].name)
    for key in dataset_keys:
        ##TODO: Add a sort key of key.lower().
        for dataset_name, kwargs in sorted(DATASET_KWARGS[key].items()):
            if kwargs.get("update_last_load"):
                last_load_kwargs = copy(kwargs)
                last_load_kwargs["source_path"] = os.path.join(
                    repository["current"].path, dataset_name
                )
                last_load_kwargs["source_where_sql"] = None
                update_repository_dataset(
                    repository["last_load"], dataset_name, **last_load_kwargs
                )
            update_repository_dataset(repository["current"], dataset_name, **kwargs)
    LOG.info("End: Update.")


def datasets_primary_update():
    """Run update for RLIDGeo warehouse primary datasets."""
    datasets_update(["primary"])


def datasets_secondary_update():
    """Run update for RLIDGeo warehouse secondary datasets."""
    datasets_update(["secondary"])


def msag_update():
    """Run update for the Master Street Address Guide dataset in RLIDGeo warehouse."""
    LOG.info("Start: Update MSAG dataset in RLIDGeo warehouse.")
    unretired_where_sql = "expiration_date is null"
    # Do not update MSAG if there will be a significant change.
    count = {
        "current": arcetl.features.count(dataset.MSAG_RANGE.path("current")),
        "previous": arcetl.features.count(
            dataset.MSAG_RANGE.path("master"), dataset_where_sql=unretired_where_sql
        ),
    }
    if abs(count["current"] - count["previous"]) > count["previous"] * 0.01:
        send_email(
            subject="RLIDGeo MSAG Update Issues",
            body="""
                <p>
                    Greater than 1% change in number of un-expired ranges; not applying
                    changes.
                </p>
            """,
            body_format="HTML",
            **KWARGS_ISSUES_MESSAGE
        )

    msag_keys = [
        "msag_id",
        "emergency_service_number",
        "parity_code",
        "parity",
        "from_house_number",
        "to_house_number",
        "pre_direction_code",
        "street_name",
        "street_type_code",
        "city_code",
        "city_name",
        "effective_date",
        "shape@",
    ]
    msag_id_range = {
        feature["msag_id"]: feature
        for feature in arcetl.attributes.as_dicts(
            dataset.MSAG_RANGE.path("current"), field_names=msag_keys
        )
    }
    feature_count = Counter(deleted=0)
    most_recent_date = max(val["effective_date"] for val in msag_id_range.values())
    master_cursor = arcpy.da.UpdateCursor(
        dataset.MSAG_RANGE.path("master"),
        field_names=["msag_id", "expiration_date", "shape@"],
        where_clause=unretired_where_sql,
    )
    # Collect IDs for add-check later.
    master_ids = set()
    LOG.info("Start: Update geometry & expiration dates.")
    with master_cursor:
        for msag_id, expiration_date, geom in master_cursor:
            master_ids.add(msag_id)
            if msag_id not in msag_id_range:
                LOG.info("Range (ID=%s) not in current: set expiration date.", msag_id)
                expiration_date = most_recent_date
            elif geom is None or not geom.equals(msag_id_range[msag_id]["shape@"]):
                LOG.info("Range (ID=%s) changed geometry.", msag_id)
                geom = msag_id_range[msag_id]["shape@"]
            else:
                feature_count["unchanged"] += 1
                continue
            master_cursor.updateRow((msag_id, expiration_date, geom))
            feature_count["altered"] += 1
    LOG.info("End: Update.")
    LOG.info("Start: Insert new ranges from current MSAG.")
    master_cursor = arcpy.da.InsertCursor(
        dataset.MSAG_RANGE.path("master"), field_names=msag_keys
    )
    with master_cursor:
        for msag_id, range_ in msag_id_range.items():
            if msag_id not in master_ids:
                LOG.info("Range (ID=%s) new in current: adding.", msag_id)
                master_cursor.insertRow(tuple(range_[key] for key in msag_keys))
                feature_count["inserted"] += 1
    LOG.info("End: Insert.")
    record_dataset_update(
        os.path.basename(dataset.MSAG_RANGE.path("master")),
        feature_count,
        metadata_path=dataset.METADATA_DATASET_UPDATE.path("RLIDGeo"),
    )
    LOG.info("End: Update.")


def snapshot_etl():
    """Run ETL for snapshot of the RLIDGeo geodatabase."""
    name = "RLIDGeo_" + datestamp()
    xml_path = arcetl.workspace.create_geodatabase_xml_backup(
        geodatabase_path=database.RLIDGEO.path,
        output_path=os.path.join(tempfile.gettempdir(), name + ".xml"),
        include_data=False,
        include_metadata=True,
    )
    snapshot_path = arcetl.workspace.create_file_geodatabase(
        geodatabase_path=os.path.join(path.REGIONAL_DATA, "history", name + ".gdb"),
        xml_workspace_path=xml_path,
        include_xml_data=False,
    )
    os.remove(xml_path)
    # Push datasets to snapshot (ignore certain patterns).
    for name in arcetl.workspace.dataset_names(database.RLIDGEO.path):
        copy_name = name.split(".")[-1]
        if any(
            pattern.lower() in copy_name.lower()
            for pattern in IGNORE_PATTERNS_RLIDGEO_SNAPSHOT
        ):
            # Need to delete ignored dataset in snapshot (created by the XML).
            arcetl.dataset.delete(os.path.join(snapshot_path, copy_name))
            continue

        transform.etl_dataset(
            source_path=os.path.join(database.RLIDGEO.path, name),
            output_path=os.path.join(snapshot_path, copy_name),
        )
    arcetl.workspace.compress(snapshot_path)


##TODO: Send message if dataset listed Metadata_Dataset_Update not in RLIDGeo.
##TODO: Send message if any domains exist in RLIDGeo workspace.
def warehouse_issues():
    """Run check for issues in RLIDGeo update."""
    all_dataset_kwargs = {}
    for tag in DATASET_KWARGS:
        all_dataset_kwargs.update(DATASET_KWARGS[tag])
    datasets = {
        "existing": sorted(
            name.split(".", 1)[-1]
            for name in arcetl.workspace.dataset_names(database.RLIDGEO.path)
        ),
        "listed": sorted(all_dataset_kwargs),
        "unlisted": [],
        "listed_not_exist": [],
    }
    datasets["existing_lower"] = {name.lower() for name in datasets["existing"]}
    datasets["listed_lower"] = {name.lower() for name in datasets["listed"]}
    for dataset_name in datasets["existing"]:
        if dataset_name.lower() not in datasets["listed_lower"]:
            datasets["unlisted"].append(dataset_name)
    for dataset_name in datasets["listed"]:
        if dataset_name.lower() not in datasets["existing_lower"]:
            datasets["listed_not_exist"].append(dataset_name)
    message_body = ""
    if datasets["unlisted"]:
        message_body += "<h2>Datasets not listed in DATASET_KWARGS</h2><ul>{}</ul>".format(
            "".join("<li>{}</li>".format(item) for item in datasets["unlisted"])
        )
    if datasets["listed_not_exist"]:
        message_body += "<h2>Datasets listed in DATASET_KWARGS that do not exist</h2><ul>{}</ul>".format(
            "".join("<li>{}</li>".format(item) for item in datasets["listed_not_exist"])
        )
    if message_body:
        LOG.info("Found update issues: sending email.")
        send_email(
            subject="RLIDGeo Update Issues",
            body=message_body,
            body_format="HTML",
            **KWARGS_ISSUES_MESSAGE
        )
    else:
        LOG.info("No update issues found.")


# Jobs.


MONTHLY_JOB = Job("RLIDGeo_Monthly", etls=[snapshot_etl])

WEEKLY_JOB = Job(
    "RLIDGeo_Weekly",
    etls=[
        datasets_primary_update,
        datasets_secondary_update,
        msag_update,
        warehouse_issues,
    ],
)


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
