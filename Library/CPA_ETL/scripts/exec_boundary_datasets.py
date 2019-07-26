"""Execution code for boundary processing."""
import argparse
import logging

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import dataset
from helper.misc import TOLERANCE
from helper import transform


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# Helpers.


def precinct_ward(precinct):
    """Return ward based on precinct number."""
    precinct_ward_prefix = {"1": "E", "2": "SP"}
    if precinct and precinct[0] in precinct_ward_prefix:
        ward = precinct_ward_prefix[precinct[0]] + precinct[1]
    else:
        ward = None
    return ward


def ward_city_code(ward):
    """Return city code for ward, based on ward name."""
    prefix_city_code = {"CG": "COT", "E": "EUG", "SP": "SPR"}
    if ward:
        prefix = "".join(char for char in ward if not char.isdigit())
        city_code = prefix_city_code[prefix]
    else:
        city_code = None
    return city_code


# ETLs: City boundaries.


def annexation_history_etl():
    """Run ETL for annexation history areas."""
    with arcetl.ArcETL("Annexation History") as etl:
        etl.init_schema(dataset.ANNEXATION_HISTORY.path("pub"))
        transform.insert_features_from_paths(
            etl, dataset.ANNEXATION_HISTORY.path("inserts")
        )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.ANNEXATION_HISTORY.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="annexyearn",
            function=(lambda year: int(year) if year else None),
            field_as_first_arg=False,
            arg_field_names=["annexyear"],
        )
        etl.load(dataset.ANNEXATION_HISTORY.path("pub"))


def incorporated_city_limits_etl():
    """Run ETL for incorporated city limits."""
    with arcetl.ArcETL("Incorporated City Limits") as etl:
        etl.extract(dataset.ANNEXATION_HISTORY.path("pub"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=["annexcity"],
            tolerance=TOLERANCE["xy"],
        )
        transform.add_missing_fields(etl, dataset.INCORPORATED_CITY_LIMITS)
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="inccityabbr",
            function=(lambda x: x),
            field_as_first_arg=False,
            arg_field_names=["annexcity"],
        )
        etl.transform(
            arcetl.attributes.update_by_joined_value,
            field_name="inccityname",
            join_field_name="CityName",
            join_dataset_path=dataset.CITY.path(),
            on_field_pairs=[("inccityabbr", "CityNameAbbr")],
        )
        etl.load(dataset.INCORPORATED_CITY_LIMITS.path())


def ugb_etl():
    """Run ETL for urban growth boundaries."""
    with arcetl.ArcETL("Urban Growth Boundaries") as etl:
        etl.init_schema(dataset.UGB.path("pub"))
        transform.insert_features_from_paths(etl, dataset.UGB.path("inserts"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=["ugbcity"],
            tolerance=TOLERANCE["xy"],
        )
        etl.transform(
            arcetl.attributes.update_by_joined_value,
            field_name="ugbcityname",
            join_field_name="CityName",
            join_dataset_path=dataset.CITY.path(),
            on_field_pairs=[("ugbcity", "CityNameAbbr")],
        )
        for geom_property in ["xmin", "xmax", "ymin", "ymax"]:
            etl.transform(
                arcetl.attributes.update_by_geometry,
                field_name=geom_property,
                geometry_properties=[geom_property],
            )
        etl.load(dataset.UGB.path("pub"))


def ugb_line_etl():
    """Run ETL for UGB lines."""
    with arcetl.ArcETL("UGB Lines") as etl:
        etl.extract(dataset.UGB.path("pub"))
        etl.transform(arcetl.convert.polygons_to_lines, topological=True)
        etl.load(dataset.UGB_LINE.path())


# ETLs: Education boundaries.


def elementary_school_area_etl():
    """Run ETL for elementary school areas."""
    with arcetl.ArcETL("Elementary School Areas") as etl:
        etl.extract(dataset.ELEMENTARY_SCHOOL_AREA.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.ELEMENTARY_SCHOOL_AREA.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.ELEMENTARY_SCHOOL_AREA.path("pub"))


def elementary_school_line_etl():
    """Run ETL for elementary school lines."""
    with arcetl.ArcETL("Elementary School Lines") as etl:
        etl.extract(dataset.ELEMENTARY_SCHOOL_AREA.path("pub"))
        etl.transform(arcetl.convert.polygons_to_lines, topological=True)
        etl.load(dataset.ELEMENTARY_SCHOOL_LINE.path())


def high_school_area_etl():
    """Run ETL for high school areas."""
    with arcetl.ArcETL("High School Areas") as etl:
        etl.extract(dataset.HIGH_SCHOOL_AREA.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.HIGH_SCHOOL_AREA.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.HIGH_SCHOOL_AREA.path("pub"))


def high_school_line_etl():
    """Run ETL for high school lines."""
    with arcetl.ArcETL("High School Lines") as etl:
        etl.extract(dataset.HIGH_SCHOOL_AREA.path("pub"))
        etl.transform(arcetl.convert.polygons_to_lines, topological=True)
        etl.load(dataset.HIGH_SCHOOL_LINE.path())


def middle_school_area_etl():
    """Run ETL for middle school areas."""
    with arcetl.ArcETL("Middle School Areas") as etl:
        etl.extract(dataset.MIDDLE_SCHOOL_AREA.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.MIDDLE_SCHOOL_AREA.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.MIDDLE_SCHOOL_AREA.path("pub"))


def middle_school_line_etl():
    """Run ETL for middle school lines."""
    with arcetl.ArcETL("Middle School Lines") as etl:
        etl.extract(dataset.MIDDLE_SCHOOL_AREA.path("pub"))
        etl.transform(arcetl.convert.polygons_to_lines, topological=True)
        etl.load(dataset.MIDDLE_SCHOOL_LINE.path())


def school_district_etl():
    """Run ETL for school districts."""
    with arcetl.ArcETL("School Districts") as etl:
        etl.extract(dataset.SCHOOL_DISTRICT.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.SCHOOL_DISTRICT.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.SCHOOL_DISTRICT.path("pub"))


# ETLs: Election boundaries.


def city_ward_etl():
    """Run ETL for city wards."""
    with arcetl.ArcETL("City Wards") as etl:
        etl.extract(
            dataset.ELECTION_PRECINCT.path("maint"),
            extract_where_sql="precntnum like '1%' or precntnum like '2%'",
        )
        transform.add_missing_fields(etl, dataset.CITY_WARD)
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="ward",
            function=precinct_ward,
            field_as_first_arg=False,
            arg_field_names=["precntnum"],
        )
        # Add Cottage Grove wards.
        etl.transform(
            arcetl.features.insert_from_path,
            insert_dataset_path=dataset.COT_CITY_WARD.path(),
        )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names="ward",
            tolerance=TOLERANCE["xy"],
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="wardcity",
            function=ward_city_code,
            field_as_first_arg=False,
            arg_field_names=["ward"],
        )
        etl.transform(
            arcetl.attributes.update_by_joined_value,
            field_name="councilor",
            join_dataset_path=dataset.CITY_COUNCILOR.path(),
            join_field_name="Councilor",
            on_field_pairs=[("ward", "Ward")],
        )
        etl.load(dataset.CITY_WARD.path())


def county_commissioner_dist_etl():
    """Run ETL for county commissioner districts."""
    with arcetl.ArcETL("County Commissioner Districts") as etl:
        etl.extract(dataset.COUNTY_COMMISSIONER_DISTRICT.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.COUNTY_COMMISSIONER_DISTRICT.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.COUNTY_COMMISSIONER_DISTRICT.path("pub"))


def election_precinct_etl():
    """Run ETL for election precincts."""
    with arcetl.ArcETL("Election Precincts") as etl:
        etl.extract(dataset.ELECTION_PRECINCT.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.ELECTION_PRECINCT.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.ELECTION_PRECINCT.path("pub"))


def epud_subdistrict_etl():
    """Run ETL for EPUD subdistricts."""
    with arcetl.ArcETL("EPUD Subdistricts") as etl:
        etl.extract(dataset.EPUD_SUBDISTRICT.path("source"))
        transform.add_missing_fields(etl, dataset.EPUD_SUBDISTRICT, tags=["pub"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="boardid",
            function=(lambda x: int(x) if x.isdigit() else None),
            field_as_first_arg=False,
            arg_field_names=["boardid_"],
        )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=["boardid", "boardmbr"],
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.EPUD_SUBDISTRICT.path("pub"))


def eweb_commissioner_etl():
    """Run ETL for EWEB commissioner table."""
    with arcetl.ArcETL("EWEB Commissioners") as etl:
        etl.extract(dataset.EWEB_COMMISSIONER.path("maint"))
        etl.load(dataset.EWEB_COMMISSIONER.path("pub"))


def lcc_board_zone_etl():
    """Run ETL for LCC board zones."""
    with arcetl.ArcETL("LCC Board Zones") as etl:
        etl.extract(dataset.LCC_BOARD_ZONE.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.LCC_BOARD_ZONE.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.LCC_BOARD_ZONE.path("pub"))


def swc_district_etl():
    """Run ETL for soil & water conservation districts."""
    with arcetl.ArcETL("Soil & Water Conservation Districts") as etl:
        etl.extract(dataset.SOIL_WATER_CONSERVATION_DISTRICT.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.SOIL_WATER_CONSERVATION_DISTRICT.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.SOIL_WATER_CONSERVATION_DISTRICT.path("pub"))


def state_representative_dist_etl():
    """Run ETL for state representative districts."""
    with arcetl.ArcETL("State Representative Districts") as etl:
        etl.extract(dataset.STATE_REPRESENTATIVE_DISTRICT.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.STATE_REPRESENTATIVE_DISTRICT.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.STATE_REPRESENTATIVE_DISTRICT.path("pub"))


def state_senator_district_etl():
    """Run ETL for state senator districts."""
    with arcetl.ArcETL("State Senator Districts") as etl:
        etl.extract(dataset.STATE_SENATOR_DISTRICT.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.STATE_SENATOR_DISTRICT.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.STATE_SENATOR_DISTRICT.path("pub"))


# ETLs: Other/miscellaneous boundaries.


def zip_code_area_etl():
    """Run ETL for ZIP code areas."""
    with arcetl.ArcETL("ZIP Code Areas") as etl:
        etl.extract(dataset.ZIP_CODE_AREA.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.ZIP_CODE_AREA.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.ZIP_CODE_AREA.path("pub"))


# Jobs.


WEEKLY_JOB = Job(
    "Boundary_Datasets_Weekly",
    etls=[
        # City boundaries.
        annexation_history_etl,
        incorporated_city_limits_etl,
        ugb_etl,
        ugb_line_etl,
        # Education boundaries.
        elementary_school_area_etl,
        elementary_school_line_etl,
        high_school_area_etl,
        high_school_line_etl,
        middle_school_area_etl,
        middle_school_line_etl,
        school_district_etl,
        # Election boundaries.
        city_ward_etl,
        county_commissioner_dist_etl,
        election_precinct_etl,
        epud_subdistrict_etl,
        eweb_commissioner_etl,
        lcc_board_zone_etl,
        swc_district_etl,
        state_representative_dist_etl,
        state_senator_district_etl,
        # Other/miscellaneous boundaries.
        zip_code_area_etl,
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
