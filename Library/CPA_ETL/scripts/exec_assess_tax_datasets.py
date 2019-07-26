"""Execution code for A&T dataset processing."""
import argparse
import logging
import os

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import dataset
from helper.misc import TOLERANCE
from helper import transform
from helper.value import clean_whitespace


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

RANGES_REPR_FUNCTION = {
    (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14): "{}W".format,
    (15, 25, 35, 45, 55, 65, 75, 85): (lambda rng: "{}E".format(int(rng) // 10)),
    (16, 19, 26, 29, 36, 39, 46, 49, 56, 59, 66, 69, 76, 79, 86, 89): (
        lambda rng: "{}.5E".format(int(rng) // 10)
    ),
    (18, 28, 38, 48, 58, 68, 78, 88): (lambda rng: "{}.5W".format(int(rng) // 10)),
}
"""dict: Mapping of collection of ranges to the proper range-formatting."""


# Helpers.


def convert_lane_trs_to_common(township_number, range_number, section_number):
    """Return common township/range/section style from Lane County style."""
    # All townships in Lane County are "south".
    common = {"township": "{}S".format(township_number), "section": str(section_number)}
    for ranges, func in RANGES_REPR_FUNCTION.items():
        if int(range_number) in ranges:
            common["range"] = func(range_number)
            break

    return "{township} {range} {section}".format(**common)


def comparable_sale_taxlot():
    """Generate comparable sale taxlot features."""
    maptaxlots = set(
        maptaxlot
        for maptaxlot, in arcetl.attributes.as_iters(
            dataset.COMPARABLE_SALE_TAXLOT.path("source"), field_names=["maplot"]
        )
    )
    maptaxlot_geoms = arcetl.attributes.as_iters(
        dataset.TAXLOT.path("maint"), field_names=["maptaxlot", "shape@"]
    )
    for maptaxlot, geom in maptaxlot_geoms:
        if maptaxlot in maptaxlots:
            yield (maptaxlot, geom)


# ETLs.


def comparable_sale_taxlot_etl():
    """Run ETL for comparable sale taxlots."""
    with arcetl.ArcETL("Comparable Sale Taxlots") as etl:
        etl.init_schema(dataset.COMPARABLE_SALE_TAXLOT.path("pub"))
        etl.transform(
            arcetl.features.insert_from_iters,
            insert_features=comparable_sale_taxlot,
            field_names=["maptaxlot", "shape@"],
        )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=["maptaxlot"],
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.COMPARABLE_SALE_TAXLOT.path("pub"))


def county_boundary_etl():
    """Run ETL for the county boundary.

    DO NOT RUN REGULARLY! Automatic ETL of the taxlots means accepting
    possible slivers from accidental gaps in the taxlot fabric, plus having
    too much edge definition from border islands & coastal inlets. If running
    this to recreate the boundary, be sure to correct for these issues.
    """
    with arcetl.ArcETL("County Boundary") as etl:
        etl.extract(dataset.TAXLOT.path("maint"))
        etl.transform(
            arcetl.features.dissolve, dissolve_field_names=[], tolerance=TOLERANCE["xy"]
        )
        etl.transform(
            arcetl.attributes.update_by_value, field_name="name", value="Lane"
        )
        etl.load(dataset.COUNTY_BOUNDARY.path())


def plat_etl():
    """Run ETL for plats."""
    with arcetl.ArcETL("Plats") as etl:
        etl.extract(dataset.PLAT.path("maint"))
        transform.clean_whitespace(etl, field_names=["platname", "docnumber"])
        transform.force_uppercase(etl, field_names=["platname"])
        transform.clear_nonpositive(etl, field_names=["agencydocn"])
        pub_field_names = {
            field["name"] for field in dataset.PLAT.fields if "pub" in field["tags"]
        }
        etl.transform(
            arcetl.features.delete,
            dataset_where_sql=" and ".join(
                "{} is null".format(name) for name in pub_field_names
            ),
        )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=pub_field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.PLAT.path("pub"))


def plss_dlc_etl():
    """Run ETL for PLSS donation land claims."""
    with arcetl.ArcETL("PLSS Donation Land Claims") as etl:
        etl.extract(dataset.PLSS_DLC.path("maint"))
        transform.clean_whitespace(etl, field_names=["name", "trs"])
        transform.add_missing_fields(etl, dataset.PLSS_DLC, tags=["pub"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="dlcname",
            function=(lambda x: x),
            field_as_first_arg=False,
            arg_field_names=["NAME"],
        )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=[
                field["name"]
                for field in dataset.PLSS_DLC.fields
                if "pub" in field["tags"]
            ],
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.PLSS_DLC.path("pub"))


def plss_quarter_section_etl():
    """Run ETL for PLSS quarter sections."""
    with arcetl.ArcETL("PLSS Quarter-sections") as etl:
        etl.extract(dataset.PLSS_QUARTER_SECTION.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=[
                field["name"]
                for field in dataset.PLSS_QUARTER_SECTION.fields
                if "pub" in field["tags"]
            ],
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.PLSS_QUARTER_SECTION.path("pub"))


def plss_section_etl():
    """Run ETL for PLSS sections."""
    with arcetl.ArcETL("PLSS Sections") as etl:
        etl.extract(dataset.PLSS_SECTION.path("maint"))
        transform.add_missing_fields(etl, dataset.PLSS_SECTION, tags=["pub"])
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=[
                field["name"]
                for field in dataset.PLSS_SECTION.fields
                if "pub" in field["tags"]
            ],
            tolerance=TOLERANCE["xy"],
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="trsalt",
            function=convert_lane_trs_to_common,
            field_as_first_arg=False,
            arg_field_names=["tnum", "rnum", "sec"],
        )
        etl.load(dataset.PLSS_SECTION.path("pub"))


def plss_township_etl():
    """Run ETL for PLSS townships."""
    with arcetl.ArcETL("PLSS Townships") as etl:
        etl.extract(dataset.PLSS_TOWNSHIP.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=["tr"],
            tolerance=TOLERANCE["xy"],
        )
        transform.add_missing_fields(etl, dataset.PLSS_TOWNSHIP, tags=["pub"])
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="tnum",
            function=(lambda tr: tr // 100),
            field_as_first_arg=False,
            arg_field_names=["tr"],
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="rnum",
            function=(lambda tr: tr % 100),
            field_as_first_arg=False,
            arg_field_names=["tr"],
        )
        etl.load(dataset.PLSS_TOWNSHIP.path("pub"))


def tax_code_area_etl():
    """Run ETL for tax code areas."""
    with arcetl.ArcETL("Tax Code Areas") as etl:
        etl.extract(dataset.TAX_CODE_AREA.path("maint"))
        transform.clean_whitespace(
            etl, field_names=["taxcode", "source", "ordinance", "schooldist"]
        )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.TAX_CODE_AREA.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.TAX_CODE_AREA.path("pub"))


def tax_code_detail_etl():
    """Run ETL for tax code details.

    ##TODO:
        * Don't run unless you're sure the RLID table is picking up dormant non-levying
            tax districts.
        * Ensure duplicates are removed.
        * Probably only needs to be run right after tax certification.
        * Add levy_rate?
    """
    with arcetl.ArcETL("Tax Code Details") as etl:
        LOG.info("Start: Collect RLID tax codes & organizations.")
        rlid_code_orgs = set(
            (code, clean_whitespace(org))
            for code, org in arcetl.attributes.as_iters(
                dataset.TAX_CODE_DETAIL.path("source"),
                field_names=["tca_hyphen", "organization_name"],
            )
        )
        LOG.info("End: Collect.")
        LOG.info("Start: Collect codes from tax code areas.")
        tca_codes = set(
            code
            for code, in arcetl.attributes.as_iters(
                dataset.TAX_CODE_AREA.path("pub"), field_names=["taxcode"]
            )
        )
        LOG.info("End: Collect.")
        LOG.info("Start: Collect still-relevant RLIDGeo details.")
        rlidgeo_code_orgs = set(
            arcetl.attributes.as_iters(
                dataset.TAX_CODE_DETAIL.path("rlidgeo"),
                field_names=["taxcode", "orgname"],
            )
        )
        LOG.info("End: Collect.")
        LOG.info("Start: Filter details.")
        # Remove details not in RLID and without a current tax code area.
        rlidgeo_code_orgs = set(
            (code, org)
            for code, org in rlidgeo_code_orgs
            if (code, org) in rlid_code_orgs or code in tca_codes
        )
        code_orgs = rlidgeo_code_orgs.union(rlid_code_orgs)
        LOG.info("End: Filter.")
        etl.init_schema(dataset.TAX_CODE_DETAIL.path("pub"))
        etl.transform(
            arcetl.features.insert_from_iters,
            insert_features=code_orgs,
            field_names=["taxcode", "orgname"],
        )
        etl.load(dataset.TAX_CODE_DETAIL.path("pub"))


##TODO: Use misc helper module account # generator.
def taxlot_owner_etl():
    """Run ETL for taxlot owners (real & active)."""
    rlid_field_name = {
        "maptaxlot": "maplot",
        "acctno": "account_stripped",
        "ownname": "owner_name",
        "addr1": "addr_line1",
        "addr2": "addr_line2",
        "addr3": "addr_line3",
        "ownercity": "city",
        "ownerprvst": "prov_state",
        "ownerzip": "zip_code",
        "ownercntry": "country",
    }
    with arcetl.ArcETL("Taxlot Owners") as etl:
        LOG.info("Start: Collect accounts.")
        accounts = set(
            acct
            for acct, in arcetl.attributes.as_iters(
                os.path.join(database.RLID.path, "dbo.Account"),
                field_names=["account_stripped"],
                dataset_where_sql="""
                    (active_this_year = 'Y' or new_acct_active_next_year = 'Y')
                    and account_int < 4000000
                """,
            )
        )
        LOG.info("End: Collect.")
        LOG.info("Start: Collect owners.")
        _owners = arcetl.attributes.as_dicts(
            dataset.TAXLOT_OWNER.path("source"),
            field_names=rlid_field_name.values(),
            dataset_where_sql="account_int < 4000000",
        )
        owners = []
        for owner in _owners:
            if owner["account_stripped"] not in accounts:
                continue

            # Rename keys & clean value (many in RLID are char/varchar type).
            owner = {
                key: clean_whitespace(owner[rlid_key])
                for key, rlid_key in rlid_field_name.items()
            }
            if owner not in owners:
                owners.append(owner)
        LOG.info("End: Collect.")
        etl.init_schema(dataset.TAXLOT_OWNER.path("pub"))
        etl.transform(
            arcetl.features.insert_from_dicts,
            insert_features=owners,
            field_names=rlid_field_name.keys(),
        )
        etl.load(dataset.TAXLOT_OWNER.path("pub"))


# Jobs.


DAILY_JOB = Job("Assess_Tax_Datasets_Daily", etls=[comparable_sale_taxlot_etl])


WEEKLY_JOB = Job(
    "Assess_Tax_Datasets_Weekly",
    etls=[
        plat_etl,
        plss_dlc_etl,
        plss_quarter_section_etl,
        plss_section_etl,
        plss_township_etl,
        tax_code_area_etl,
        taxlot_owner_etl,
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
