"""Execution code for planning & development dataset processing."""
import argparse
import functools
import logging
import os

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import dataset
from helper.misc import (
    REAL_LOT_SQL,
    TOLERANCE,
    taxlot_area_map,
    taxlot_subset_temp_copies,
)
from helper import path
from helper import transform


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

LANE_ZONING_STAGING_PATH = os.path.join(path.REGIONAL_STAGING, "LCZoning")


# Helpers.


def concatenate_zoning_overlays(**overlay):
    """Return concatenation of flagged zoning overlay codes (value=Y).

    Args:
        **overlay (dict): Mapping of overlay name to value.

    Returns:
        str
    """
    overlay_codes = [
        code.upper().replace("OVER", "", 1)
        for code, value in overlay.items()
        if value == "Y"
    ]
    return ",".join(overlay_codes) if overlay_codes else None


def county_zone_name_map():
    """Return dictionary of zone codes mapping to names.

    Returns:
        dict: Mapping of zone code to name.
    """
    rows = arcetl.attributes.as_iters(
        dataset.ZONING_COUNTY.path("maint"), field_names=["ZONE_", "ZONE_NAME"]
    )
    zone_name = dict(rows)
    # Fill-in UGB zones not in regular dataset.
    rows = arcetl.attributes.as_iters(
        dataset.ZONING_COUNTY.path("insert"), field_names=["ZONE_", "ZONE_NAME"]
    )
    for code, name in rows:
        if code not in zone_name:
            zone_name[code] = name
    return zone_name


def split_zoned_taxlots():
    """Return set of maptaxlot values for lots listed as split-zoned.

    Returns:
        set: Collection of taxlots with split-zoning.
    """
    lot_iters = arcetl.attributes.as_iters(
        dataset.TAXLOT_SPLIT_ZONING.path(), field_names=["maptaxlot"]
    )
    return set(maptaxlot for maptaxlot, in lot_iters)


def taxlot_centroid_zoning_map():
    """Return mapping of maptaxlot to zoning details at the centroid.

    Returns:
        dict: Mapping of maptaxlot to zoning combo at its centroid.
    """
    ##TODO: Add alloverlays (as zoning_overlays) to taxlot, replace overlay# usage with.
    key_change_map = {"zoningjuris": "zonejuris", "zoning": "zonecode"}
    taxlots = arcetl.attributes.as_dicts(
        dataset.TAXLOT.path("pub"),
        field_names=[
            "maptaxlot",
            "zoningjuris",
            "zoning",
            "subarea",
            "overlay1",
            "overlay2",
            "overlay3",
            "overlay4",
        ],
        dataset_where_sql=REAL_LOT_SQL,
    )
    zoning_map = {taxlot["maptaxlot"]: taxlot for taxlot in taxlots}
    for zoning in zoning_map.values():
        # Rename keys to match zoning dataset.
        for old_key, new_key in key_change_map.items():
            zoning[new_key] = zoning.pop(old_key)
    return zoning_map


def zone_jurisdiction_name_map():
    """Return mapping of zone jurisdiction codes to names.

    Returns:
        dict: Mapping of jurisdiction code to name.
    """
    juris_name = dict(
        arcetl.attributes.as_iters(
            dataset.CITY.path(),
            field_names=["citynameabbr", "cityname"],
            dataset_where_sql="isinccity = 'Y'",
        )
    )
    # For some reason RLID uses wrong Westfir code here.
    juris_name["WES"] = juris_name.pop("WEF")
    juris_name["LC"] = "Lane County"
    return juris_name


def zoning_matches_taxlot_yn(taxlot_zoning_map, **feature):
    """Return "Y" if feature zoning matches taxlot centroid zoning, "N" otherwise.

    Args:
        taxlot_zoning_map (dict): Mapping of maptaxlot to zoning combo.
        **feature: Attributes of a feature to determine if matches centroid zoning.

    Returns:
        str: "Y" or "N".
    """
    matches = all(
        feature[key] == val
        for key, val in taxlot_zoning_map[feature["maptaxlot"]].items()
    )
    return "Y" if matches else "N"


# ETLs.


def metro_plan_boundary_etl():
    """Run ETL for the Metro Plan boundary."""
    with arcetl.ArcETL("Metro Plan Boundary") as etl:
        etl.extract(dataset.METRO_PLAN_BOUNDARY.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.METRO_PLAN_BOUNDARY.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.METRO_PLAN_BOUNDARY.path("pub"))


def nodal_development_area_etl():
    """Run ETL for nodal development areas."""
    with arcetl.ArcETL("Nodal Development Areas") as etl:
        etl.extract(dataset.NODAL_DEVELOPMENT_AREA.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.NODAL_DEVELOPMENT_AREA.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.NODAL_DEVELOPMENT_AREA.path("pub"))


def plan_designation_etl():
    """Run ETL for plan designations."""
    with arcetl.ArcETL("Plan Designations") as etl:
        etl.init_schema(dataset.PLAN_DESIGNATION.path("pub"))
        for _path in dataset.PLAN_DESIGNATION.path("inserts"):
            etl.transform(arcetl.features.insert_from_path, insert_dataset_path=_path)
        # Need a singular ID for juris-designation-overlay.
        etl.transform(arcetl.attributes.update_by_unique_id, field_name="plandes_id")
        etl.load(dataset.PLAN_DESIGNATION.path("pub"))


def plan_designation_city_etl():
    """Run ETL for city plan designations."""
    with arcetl.ArcETL("City Plan Designations") as etl:
        etl.init_schema(dataset.PLAN_DESIGNATION_CITY.path("pub"))
        for _path in dataset.PLAN_DESIGNATION_CITY.path("inserts"):
            etl.transform(arcetl.features.insert_from_path, insert_dataset_path=_path)
        etl.transform(arcetl.features.delete, dataset_where_sql="plandes is null")
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.PLAN_DESIGNATION_CITY.field_names,
            tolerance=TOLERANCE["xy"],
        )
        juris_domain = {
            "COB": "CoburgPlanDes",
            "COT": "CottageGrovePlanDes",
            "CRE": "CreswellPlanDes",
            "DUN": "DunesCityPlanDes",
            "FLO": "FlorencePlanDes",
            "JUN": "JunctionCityPlanDes",
            "LOW": "LowellPlanDes",
            "MTP": "MetroPlanDes",
            "OAK": "OakridgePlanDes",
            "VEN": "VenetaPlanDes",
            "WES": "WestfirPlanDes",
        }
        for juris_code, domain_name in juris_domain.items():
            etl.transform(
                arcetl.attributes.update_by_domain_code,
                field_name="plandesnam",
                code_field_name="plandes",
                domain_name=domain_name,
                domain_workspace_path=database.LCOGGEO.path,
                dataset_where_sql="planjuris = '{}'".format(juris_code),
            )
        etl.load(dataset.PLAN_DESIGNATION_CITY.path("pub"))


def plan_designation_county_etl():
    """Run ETL for county plan designations."""
    with arcetl.ArcETL("County Plan Designations") as etl:
        etl.extract(dataset.PLAN_DESIGNATION_COUNTY.path("maint"))
        transform.add_missing_fields(etl, dataset.PLAN_DESIGNATION_COUNTY, tags=["pub"])
        etl.transform(
            arcetl.attributes.update_by_value, field_name="planjuris", value="LC"
        )
        for new_name, old_name in [("plandes", "ZONE_"), ("plandesnam", "ZONE_NAME")]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=new_name,
                function=(lambda x: x),
                field_as_first_arg=False,
                arg_field_names=[old_name],
            )
        # Remove county designations where city ones exist.
        etl.transform(
            arcetl.features.erase,
            erase_dataset_path=dataset.PLAN_DESIGNATION_CITY.path("pub"),
        )
        transform.clean_whitespace(
            etl, field_names=["planjuris", "plandes", "plandesnam", "finalorder"]
        )
        etl.transform(arcetl.features.delete, dataset_where_sql="plandes is null")
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.PLAN_DESIGNATION_COUNTY.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.PLAN_DESIGNATION_COUNTY.path("pub"))


def taxlot_zoning_etl():
    """Run ETL for taxlot zoning cross-reference."""
    keys = {
        "taxlot": ["maptaxlot", "maptaxlot_hyphen", "map", "taxlot"],
        "zoning": [
            "zonejuris",
            "zonecode",
            "zonename",
            "subarea",
            "subareaname",
            "alloverlays",
            "overlay1",
            "overlay2",
            "overlay3",
            "overlay4",
        ],
    }
    keys["all"] = keys["taxlot"] + keys["zoning"]
    with arcetl.ArcETL("Taxlot Zoning") as etl:
        etl.init_schema(dataset.TAXLOT_ZONING.path())
        etl.transform(
            arcetl.dataset.add_field, field_name="zoning_id", field_type="long"
        )
        # To avoid memory/topoengine errors when processing, run ETL on subsets.
        subsets = taxlot_subset_temp_copies(REAL_LOT_SQL, field_names=keys["taxlot"])
        for subset in subsets:
            with subset:
                arcetl.dataset.add_field(
                    subset.path, field_name="zoning_id", field_type="long"
                )
                arcetl.geoset.identity(
                    dataset_path=subset.path,
                    field_name="zoning_id",
                    identity_dataset_path=dataset.ZONING.path("pub"),
                    identity_field_name="zoning_id",
                )
                arcetl.features.delete(
                    subset.path, dataset_where_sql="zoning_id is null"
                )
                # Must join these in the subset copy, before the dissolve: Final order
                # attribute on zoning means zoning ID can be different for matching
                # juris + zone + subarea + overlays.
                for key in keys["zoning"]:
                    arcetl.dataset.join_field(
                        dataset_path=subset.path,
                        join_dataset_path=dataset.ZONING.path("pub"),
                        join_field_name=key,
                        on_field_name="zoning_id",
                        on_join_field_name="zoning_id",
                    )
                # Dissolve on lot & overlay, for proper area representation.
                # Currently get topoengine error with tolerance on certain lot.
                for where_sql, tolerance in [
                    ("maptaxlot =  '2135093001000'", None),
                    ("maptaxlot <> '2135093001000'", TOLERANCE["xy"]),
                ]:
                    arcetl.features.dissolve(
                        subset.path,
                        dissolve_field_names=keys["all"],
                        dataset_where_sql=where_sql,
                        tolerance=tolerance,
                    )
                etl.transform(
                    arcetl.features.insert_from_path,
                    insert_dataset_path=subset.path,
                    field_names=keys["all"],
                )
        # Add descriptions/names.
        etl.transform(
            arcetl.attributes.update_by_mapping,
            field_name="zonejuris_name",
            mapping=zone_jurisdiction_name_map,
            key_field_names="zonejuris",
        )
        for i in [1, 2, 3, 4]:
            etl.transform(
                arcetl.attributes.update_by_joined_value,
                field_name="overlay{}_name".format(i),
                join_dataset_path=dataset.ZONING_OVERLAY.path(),
                join_field_name="overlay_name",
                on_field_pairs=[
                    ("zonejuris", "zoning_juris"),
                    ("overlay{}".format(i), "overlay_code"),
                ],
            )
        # Yeah, the -9999 is weird. However, trying to delete null-geometry features
        # that arise from every-so-tiny slivers in identity results.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="approx_acres",
            function=(lambda x: x / 43560.0 if x is not None else -9999),
            field_as_first_arg=False,
            arg_field_names=["shape@area"],
        )
        etl.transform(arcetl.features.delete, dataset_where_sql="approx_acres <= 0")
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="approx_taxlot_acres",
            function=(lambda lot, sqft=taxlot_area_map(): sqft[lot] / 43560.0),
            field_as_first_arg=False,
            arg_field_names=["maptaxlot"],
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="taxlot_area_ratio",
            function=(lambda acres, taxlot_acres: acres / taxlot_acres),
            field_as_first_arg=False,
            arg_field_names=["approx_acres", "approx_taxlot_acres"],
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="compactness_ratio",
            function=arcetl.geometry.compactness_ratio,
            field_as_first_arg=False,
            arg_field_names=["shape@"],
        )
        # Remove minimal overlays.
        etl.transform(
            arcetl.features.delete, dataset_where_sql="taxlot_area_ratio <= 0.01"
        )
        # Must be done after filtering minimal/bad overlays.
        etl.transform(
            arcetl.attributes.update_by_feature_match,
            field_name="taxlot_zoning_count",
            id_field_names=["maptaxlot"],
            update_type="match_count",
        )
        ##TODO: Still useful after allowing auto-splits?
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="at_taxlot_centroid",
            function=functools.partial(
                zoning_matches_taxlot_yn, taxlot_zoning_map=taxlot_centroid_zoning_map()
            ),
            field_as_first_arg=False,
            kwarg_field_names=keys["all"],
        )
        ##TODO: Remove after auto-splits.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="in_split_table",
            function=(
                lambda x, split_lots=split_zoned_taxlots(): "Y"
                if x in split_lots
                else "N"
            ),
            field_as_first_arg=False,
            arg_field_names=["maptaxlot"],
        )
        # ##TODO: Repoint output & delete test-output.
        # etl.load(
        #     os.path.join(
        #         path.CPA_WORK_SHARE, "Boundary\\Zoning\\Taxlot_Zoning.gdb\\TaxlotZoning"
        #     )
        # )
        etl.load(dataset.TAXLOT_ZONING.path())


def willamette_river_greenway_etl():
    """Run ETL for the Willamette River Greenway."""
    with arcetl.ArcETL("Willamette River Greenway") as etl:
        etl.extract(dataset.WILLAMETTE_RIVER_GREENWAY.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.WILLAMETTE_RIVER_GREENWAY.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.WILLAMETTE_RIVER_GREENWAY.path("pub"))


def zoning_etl():
    """Run ETL for zoning."""
    with arcetl.ArcETL("Zoning") as etl:
        etl.init_schema(dataset.ZONING.path("pub"))
        for _path in dataset.ZONING.path("inserts"):
            etl.transform(arcetl.features.insert_from_path, insert_dataset_path=_path)
        # Need a singular ID for overlay use.
        etl.transform(arcetl.attributes.update_by_unique_id, field_name="zoning_id")
        # Still need overlay 1-4 format for taxlot overlays.
        for i in [1, 2, 3, 4]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name="overlay" + str(i),
                ##TODO: Lambda --> helper function.
                function=(
                    lambda overlays, position=i: overlays.split(",")[position - 1]
                    if (overlays and len(overlays.split(",")) >= position)
                    else None
                ),
                field_as_first_arg=False,
                arg_field_names=["alloverlays"],
            )
        etl.load(dataset.ZONING.path("pub"))


def zoning_city_etl():
    """Run ETL for city zoning."""
    overlay_field_names = [
        name
        for name in dataset.ZONING_CITY.field_names
        if name.lower().startswith("over")
    ]
    with arcetl.ArcETL("City Zoning") as etl:
        etl.init_schema(dataset.ZONING_CITY.path("pub"))
        for _path in dataset.ZONING_CITY.path("inserts"):
            etl.transform(arcetl.features.insert_from_path, insert_dataset_path=_path)
        # Clean maintenance values.
        transform.force_uppercase(etl, overlay_field_names)
        transform.force_yn(etl, overlay_field_names, default="N")
        etl.transform(arcetl.features.delete, dataset_where_sql="zonecode is null")
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.ZONING_CITY.field_names,
            tolerance=TOLERANCE["xy"],
        )
        juris_domain = {
            "COB": "CoburgZoning",
            "COT": "CottageGroveZoning",
            "CRE": "CreswellZoning",
            "DUN": "DunesCityZoning",
            "EUG": "EugeneZoning",
            "FLO": "FlorenceZoning",
            "JUN": "JunctionCityZoning",
            "LOW": "LowellZoning",
            "OAK": "OakridgeZoning",
            "SPR": "SpringfieldZoning",
            "VEN": "VenetaZoning",
            "WES": "WestfirZoning",
        }
        for juris_code, domain_name in juris_domain.items():
            etl.transform(
                arcetl.attributes.update_by_domain_code,
                field_name="zonename",
                code_field_name="zonecode",
                domain_name=domain_name,
                domain_workspace_path=database.LCOGGEO.path,
                dataset_where_sql="zonejuris = '{}'".format(juris_code),
            )
        etl.transform(
            arcetl.attributes.update_by_domain_code,
            field_name="subareaname",
            code_field_name="subarea",
            domain_name="EugeneZoningSubarea",
            domain_workspace_path=database.LCOGGEO.path,
        )
        # Clean domain-derived values.
        transform.clean_whitespace(etl, field_names=["zonename", "subareaname"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="alloverlays",
            function=concatenate_zoning_overlays,
            field_as_first_arg=False,
            kwarg_field_names=overlay_field_names,
        )
        etl.load(dataset.ZONING_CITY.path("pub"))


def zoning_county_etl():
    """Run ETL for county zoning."""
    overlay_field_names = [
        name
        for name in dataset.ZONING_COUNTY.field_names
        if name.lower().startswith("over")
    ]
    with arcetl.ArcETL("County Zoning") as etl:
        etl.extract(dataset.ZONING_COUNTY.path("maint"))
        etl.transform(
            arcetl.features.insert_from_path,
            insert_dataset_path=dataset.ZONING_COUNTY.path("insert"),
        )
        transform.add_missing_fields(etl, dataset.ZONING_COUNTY, tags=["pub"])
        for new_name, old_name in [("zonecode", "ZONE_"), ("zonename", "ZONE_NAME")]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=new_name,
                function=(lambda x: x),
                field_as_first_arg=False,
                arg_field_names=[old_name],
            )
        # UGB zoning has slightly different names. We want to standardize on the main
        # zoning dataset names.
        etl.transform(
            arcetl.attributes.update_by_mapping,
            field_name="zonename",
            mapping=county_zone_name_map,
            key_field_names="zonecode",
        )
        # Clean maintenance values.
        transform.clean_whitespace(etl, field_names=["zonecode", "zonename"])
        etl.transform(arcetl.features.delete, dataset_where_sql="zonecode is null")
        # Remove county zoning where city ones exist.
        etl.transform(
            arcetl.features.erase, erase_dataset_path=dataset.ZONING_CITY.path("pub")
        )
        # Assign zoning overlays.
        identity_kwargs = [
            {
                "field_name": "coastalzonecode",
                "identity_field_name": "TYPE",
                "identity_dataset_path": os.path.join(
                    LANE_ZONING_STAGING_PATH, "coastal_zones.shp"
                ),
            },
            {
                "field_name": "overas",
                "identity_field_name": "AIRPORT",
                "identity_dataset_path": os.path.join(
                    LANE_ZONING_STAGING_PATH, "aszone.shp"
                ),
                "replacement_value": "Y",
            },
            {
                "field_name": "overcas",
                "identity_field_name": "AIRPORT",
                "identity_dataset_path": os.path.join(
                    LANE_ZONING_STAGING_PATH, "caszone.shp"
                ),
                "replacement_value": "Y",
            },
            {
                "field_name": "overdms",
                "identity_field_name": "TYPE",
                "identity_dataset_path": os.path.join(
                    LANE_ZONING_STAGING_PATH, "dredge_sites.shp"
                ),
                "replacement_value": "Y",
            },
            {
                "field_name": "overbd",
                "identity_field_name": "Shape_Leng",
                "identity_dataset_path": os.path.join(
                    LANE_ZONING_STAGING_PATH, "beach_dune.shp"
                ),
                "replacement_value": "Y",
            },
            {
                "field_name": "overu",
                "identity_field_name": "urban",
                "identity_dataset_path": os.path.join(
                    LANE_ZONING_STAGING_PATH, "interim_urban.shp"
                ),
                "replacement_value": "Y",
            },
        ]
        for kwargs in identity_kwargs:
            etl.transform(arcetl.geoset.identity, **kwargs)
        # Clean identity values.
        transform.clean_whitespace(etl, field_names=["coastalzonecode"])
        etl.transform(
            arcetl.attributes.update_by_value, field_name="zonejuris", value="LC"
        )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=[
                field["name"]
                for field in dataset.ZONING_COUNTY.fields
                if "pub" in field["tags"]
            ],
            tolerance=TOLERANCE["xy"],
        )
        # Assign the overlay flags dependent on coastal zone code.
        for code in ["CE", "DE", "MD", "NE", "NRC", "PW", "RD", "SN"]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name="over{}".format(code.lower()),
                function=(lambda czc, c=code: "Y" if czc == c else "N"),
                field_as_first_arg=False,
                arg_field_names=["coastalzonecode"],
            )
        transform.force_uppercase(etl, overlay_field_names)
        transform.force_yn(etl, overlay_field_names, default="N")
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="alloverlays",
            function=concatenate_zoning_overlays,
            field_as_first_arg=False,
            kwarg_field_names=overlay_field_names,
        )
        etl.load(dataset.ZONING_COUNTY.path("pub"))


##TODO: Auto-generate LCOGGeo.lcagadm.ZoningOverlay from overX fields & aliases. Also move to ETL_Load_A.


# Jobs.


BOUNDARY_DATASETS_JOB = Job(
    "Planning_Development_Boundary_Datasets",
    etls=[
        # Pass 1.
        metro_plan_boundary_etl,
        nodal_development_area_etl,
        plan_designation_city_etl,
        willamette_river_greenway_etl,
        zoning_city_etl,
        # Pass 2.
        plan_designation_county_etl,
        zoning_county_etl,
        # Pass 3.
        plan_designation_etl,
        zoning_etl,
    ],
)


TAXLOT_ZONING_JOB = Job("Taxlot_Zoning_Dataset", etls=[taxlot_zoning_etl])


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
