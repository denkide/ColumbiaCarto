"""Execution code for land use processing."""
import argparse
from collections import Counter, defaultdict
import logging
import os

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import dataset
from helper.misc import TOLERANCE
from helper import path
from helper import transform
from helper.value import maptaxlot_separated


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# Helpers.


def total_acres():
    """Return mapping of total acres for a taxlot & land use pair.

    Returns:
        collections.defauldict: Mapping of (maptaxlot, land use) tuple to area in acres.
    """
    acres = defaultdict(float)
    feats = arcetl.attributes.as_iters(
        dataset.LAND_USE_AREA.path("maint"), ["maptaxlot", "landuse", "shape@area"]
    )
    for maptaxlot, land_use, sqft in feats:
        acres[maptaxlot, land_use] += sqft / 43560.0
    return acres


def total_units():
    """Return counter of total units for a taxlot & land use pair.

    Returns:
        collections.Counter: Mapping of (maptaxlot, land use) tuple to total units.
    """
    lot_use_units = Counter()
    feats = arcetl.attributes.as_iters(
        dataset.LAND_USE_AREA.path("maint"), ["maptaxlot", "landuse", "units"]
    )
    for maptaxlot, land_use, units in feats:
        if units and units > 0:
            lot_use_units[maptaxlot, land_use] += units
        else:
            lot_use_units[maptaxlot, land_use] += 0
    return lot_use_units


# ETLs.

def building_etl():
    """Run ETL for building footprints."""
    with arcetl.ArcETL("Buildings") as etl:
        etl.extract(dataset.BUILDING.path("maint"))
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="height",
            function=(lambda x: None if x is None or x <= 0 else x),
        )
        etl.update(
            dataset.BUILDING.path("pub"),
            id_field_names=dataset.BUILDING.id_field_names,
            use_edit_session=False,
        )


def land_use_area_etl():
    """Run ETL for land use areas."""
    with arcetl.ArcETL("Land Use Areas") as etl:
        etl.extract(dataset.LAND_USE_AREA.path("maint"))
        # Clean maintenance values.
        transform.clean_whitespace(etl, field_names=["maptaxlot"])
        transform.clear_non_numeric_text(etl, field_names=["maptaxlot"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="landuse",
            function=(lambda x: 0 if x is None or x < 0 else x),
        )
        # Remove features with missing core identifiers.
        for name in dataset.LAND_USE_AREA.id_field_names:
            etl.transform(
                arcetl.features.delete, dataset_where_sql="{} is null".format(name)
            )
        # Dissolve on core maintenance fields that are used in publication.
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.LAND_USE_AREA.id_field_names,
            tolerance=TOLERANCE["xy"],
        )
        transform.add_missing_fields(etl, dataset.LAND_USE_AREA, tags=["pub"])
        # Assign geometry attributes.
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
            # Public safety attributes.
            {
                "field_name": "firedist",
                "overlay_field_name": "fireprotprov",
                "overlay_dataset_path": dataset.FIRE_PROTECTION_AREA.path("pub"),
            },
            # Election attributes.
            {
                "field_name": "lcczone",
                "overlay_field_name": "lccbrdzone",
                "overlay_dataset_path": dataset.LCC_BOARD_ZONE.path("pub"),
            },
            # Education attributes.
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
                "field_name": "ltddist",
                "overlay_field_name": "LTD",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "transport\\ltd\\2012 LTD Boundary.shp"
                ),
            },
            # Natural attributes.
            {
                "field_name": "flood",
                "overlay_field_name": "fld_zone",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA, "natural\\flood\\Flood.gdb\\FloodHazardArea"
                ),
            },
            # Census attributes.
            {
                "field_name": "ctract",
                "overlay_field_name": "TRACT",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "federal\\census\\lane\\2010\\lc_census2010.gdb\\lc_tracts2010",
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
            # Other district attributes.
            {
                "field_name": "neighbor",
                "overlay_field_name": "NEIBORHD",
                "overlay_dataset_path": os.path.join(
                    path.REGIONAL_DATA,
                    "boundary\\districts\\eug\\Boundary.gdb\\EugNeighborhoods",
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
        transform.clean_whitespace(etl, field_names=["ctract", "blockgr", "neighbor"])
        transform.force_uppercase(etl, field_names=["ltddist"])
        # Set default overlay values where missing.
        transform.force_yn(etl, field_names=["greenwy", "ltddist"], default="N")
        # Remove invalid overlay values.
        transform.clear_nonpositive(etl, field_names=["ctract", "blockgr"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="neighbor",
            function=(lambda x: x if x and int(x) != 99 else None),
        )
        # Assign joinable field values after overlays.
        join_kwargs = [
            # Core attributes.
            {
                "field_name": "landusedes",
                "join_field_name": "ludesc",
                "join_dataset_path": dataset.LAND_USE_CODES_DETAILED.path("pub"),
                "on_field_pairs": [("landuse", "landuse")],
            },
            {
                "field_name": "usecode",
                "join_field_name": "usecode",
                "join_dataset_path": dataset.LAND_USE_CODES_DETAILED.path("pub"),
                "on_field_pairs": [("landuse", "landuse")],
            },
            {
                "field_name": "usecodedes",
                "join_field_name": "ucname",
                "join_dataset_path": dataset.LAND_USE_CODES_USE_CODES.path("pub"),
                "on_field_pairs": [("usecode", "usecode")],
            },
        ]
        for kwargs in join_kwargs:
            etl.transform(arcetl.attributes.update_by_joined_value, **kwargs)
        # Build values from functions.
        function_kwargs = [
            {
                "field_name": "mapnumber",
                "function": (lambda x: x[:8] if x else None),
                "arg_field_names": ["maptaxlot"],
            },
            {
                "field_name": "taxlot",
                "function": (lambda x: x[-5:] if x else None),
                "arg_field_names": ["maptaxlot"],
            },
            {
                "field_name": "maptaxlot_hyphen",
                "function": maptaxlot_separated,
                "arg_field_names": ["maptaxlot"],
            },
        ]
        for kwargs in function_kwargs:
            etl.transform(
                arcetl.attributes.update_by_function, field_as_first_arg=False, **kwargs
            )
        # Build values from mappings.
        mapping_kwargs = [
            {
                "field_name": "units",
                "mapping": total_units,
                "key_field_names": ["maptaxlot", "landuse"],
            },
            {
                "field_name": "acres",
                "mapping": total_acres,
                "key_field_names": ["maptaxlot", "landuse"],
            },
        ]
        for kwargs in mapping_kwargs:
            etl.transform(arcetl.attributes.update_by_mapping, **kwargs)
        etl.transform(
            arcetl.attributes.update_by_feature_match,
            field_name="landusecount",
            id_field_names=["maptaxlot"],
            update_type="match_count",
        )
        etl.load(dataset.LAND_USE_AREA.path("pub"))


def detailed_land_use_codes_etl():
    """Run ETL for land use codes."""
    with arcetl.ArcETL("Detailed Land Use Codes") as etl:
        etl.extract(dataset.LAND_USE_CODES_DETAILED.path("maint"))
        etl.load(dataset.LAND_USE_CODES_DETAILED.path("pub"))


def general_land_use_codes_etl():
    """Run ETL for general land use codes."""
    with arcetl.ArcETL("General Land Use Codes") as etl:
        etl.extract(dataset.LAND_USE_CODES_USE_CODES.path("maint"))
        etl.load(dataset.LAND_USE_CODES_USE_CODES.path("pub"))


# Jobs.


WEEKLY_JOB = Job(
    "Land_Use_Datasets",
    etls=[
        # Pass 1.
        building_etl,
        detailed_land_use_codes_etl,
        general_land_use_codes_etl,
        # Pass 2.
        land_use_area_etl,
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
