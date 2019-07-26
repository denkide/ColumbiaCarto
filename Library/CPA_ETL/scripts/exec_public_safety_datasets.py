"""Execution code for boundary processing."""
import argparse
import logging

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
from helper import transform


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# ETLs.


def ambulance_service_area_etl():
    """Run ETL for ambulance service areas."""
    with arcetl.ArcETL("Ambulance Service Areas") as etl:
        etl.extract(dataset.AMBULANCE_SERVICE_AREA.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.AMBULANCE_SERVICE_AREA.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.AMBULANCE_SERVICE_AREA.path("pub"))


def emergency_service_zone_etl():
    """Run ETL for emergency service zones."""
    with arcetl.ArcETL("Emergency Service Zones") as etl:
        # Use county boundary, to get a blank slate for identity overlay.
        etl.extract(dataset.COUNTY_BOUNDARY.path())
        transform.add_missing_fields(etl, dataset.EMERGENCY_SERVICE_ZONE)
        identity_kwargs = [
            {
                "temporary_field": True,
                "field_name": "inccityabbr",
                "identity_dataset_path": dataset.INCORPORATED_CITY_LIMITS.path(),
                "identity_field_name": "inccityabbr",
            },
            {
                "temporary_field": True,
                "field_name": "fireprotprov",
                "identity_dataset_path": dataset.FIRE_PROTECTION_AREA.path("pub"),
                "identity_field_name": "fireprotprov",
            },
            {
                "field_name": "asa_code",
                "identity_dataset_path": dataset.AMBULANCE_SERVICE_AREA.path("pub"),
                "identity_field_name": "asacode",
            },
            {
                "field_name": "psap_code",
                "identity_dataset_path": dataset.PSAP_AREA.path("pub"),
                "identity_field_name": "psap_code",
            },
        ]
        for kwargs in identity_kwargs:
            if kwargs.get("temporary_field"):
                etl.transform(
                    arcetl.dataset.add_field,
                    field_name=kwargs["field_name"],
                    field_type="text",
                )
            etl.transform(arcetl.geoset.identity, tolerance=2.0, **kwargs)
        # Drop where feature lacks any city, fire, & ambulance.
        etl.transform(
            arcetl.features.delete,
            dataset_where_sql=(
                "inccityabbr is null and fireprotprov is null and asa_code is null"
            ),
        )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=[
                "inccityabbr",
                "fireprotprov",
                "asa_code",
                "psap_code",
            ],
            tolerance=TOLERANCE["xy"],
        )
        etl.transform(
            arcetl.attributes.update_by_joined_value,
            field_name="emergency_service_number",
            join_field_name="emergency_service_number",
            join_dataset_path=dataset.EMERGENCY_SERVICE_NUMBER.path(),
            on_field_pairs=[
                ("inccityabbr", "city_limits"),
                ("fireprotprov", "fire_district"),
                ("asa_code", "asa_code"),
                ("psap_code", "psap_code"),
            ],
        )
        join_kwargs = [
            {
                "field_name": "law_provider",
                "join_field_name": "law_provider",
                "join_dataset_path": dataset.EMERGENCY_SERVICE_NUMBER.path(),
                "on_field_pairs": [
                    ("emergency_service_number", "emergency_service_number")
                ],
            },
            {
                "field_name": "fire_coverage_description",
                "join_field_name": "fire_coverage_description",
                "join_dataset_path": dataset.EMERGENCY_SERVICE_NUMBER.path(),
                "on_field_pairs": [
                    ("emergency_service_number", "emergency_service_number")
                ],
            },
            {
                "field_name": "asa_code",
                "join_field_name": "asa_code",
                "join_dataset_path": dataset.EMERGENCY_SERVICE_NUMBER.path(),
                "on_field_pairs": [
                    ("emergency_service_number", "emergency_service_number")
                ],
            },
            {
                "field_name": "psap_code",
                "join_field_name": "psap_code",
                "join_dataset_path": dataset.EMERGENCY_SERVICE_NUMBER.path(),
                "on_field_pairs": [
                    ("emergency_service_number", "emergency_service_number")
                ],
            },
            {
                "field_name": "asa_name",
                "join_field_name": "asa",
                "join_dataset_path": dataset.AMBULANCE_SERVICE_AREA.path("pub"),
                "on_field_pairs": [("asa_code", "asacode")],
            },
            {
                "field_name": "psap_name",
                "join_field_name": "psap_name",
                "join_dataset_path": dataset.PSAP_AREA.path("pub"),
                "on_field_pairs": [("psap_code", "psap_code")],
            },
        ]
        for kwargs in join_kwargs:
            etl.transform(arcetl.attributes.update_by_joined_value, **kwargs)
        etl.load(dataset.EMERGENCY_SERVICE_ZONE.path())


def fire_protection_area_etl():
    """Run ETL for fire protection areas."""
    city_provider_code = {"EUG": "EGF", "OAK": "OKF", "SPR": "SPF", "WEF": "WEF"}
    city_fire_protection_sql = "inccityabbr in ({})".format(
        ", ".join(repr(city) for city in city_provider_code)
    )
    with arcetl.ArcETL("Fire Protection Areas") as etl:
        etl.extract(dataset.FIRE_PROTECTION_AREA.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=[
                "fireprotprov",
                "fireprottype",
                "dateformed",
                "taxdist",
            ],
            tolerance=TOLERANCE["xy"],
        )
        transform.add_missing_fields(etl, dataset.FIRE_PROTECTION_AREA, tags=["pub"])
        # Erase fire protection that overlaps with city limits with fire protection.
        # City incorporation often leads removal of a property from an FPA after a
        # variable amount of time, so we assume precedence of the city fire protection.
        etl.transform(
            arcetl.features.erase,
            erase_dataset_path=dataset.INCORPORATED_CITY_LIMITS.path(),
            erase_where_sql=city_fire_protection_sql,
        )
        # Temporarily add inccityabbr field, so we can transfer city name.
        etl.transform(
            arcetl.dataset.add_field, field_name="inccityabbr", field_type="text"
        )
        # Append cities with fire protection.
        etl.transform(
            arcetl.features.insert_from_path,
            insert_dataset_path=dataset.INCORPORATED_CITY_LIMITS.path(),
            insert_where_sql=city_fire_protection_sql,
        )
        # Set the values for in-city fire protection areas.
        etl.transform(
            arcetl.attributes.update_by_value,
            field_name="fireprottype",
            value="CITY",
            dataset_where_sql="inccityabbr is not null",
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="fireprotprov",
            function=city_provider_code.get,
            field_as_first_arg=False,
            arg_field_names=["inccityabbr"],
            dataset_where_sql="inccityabbr is not null",
        )
        etl.transform(
            arcetl.attributes.update_by_value,
            field_name="taxdist",
            value="Y",
            dataset_where_sql="inccityabbr is not null",
        )
        etl.transform(
            arcetl.attributes.update_by_domain_code,
            field_name="fptypename",
            code_field_name="fireprottype",
            domain_name="FireProtectionType",
            domain_workspace_path=database.LCOGGEO.path,
        )
        # Assign joinable field values city overlays/additions.
        attr_join_key = {
            "fpprovname": "provider_name",
            "contact_phone": "contact_phone",
            "contact_email": "contact_email",
            "contact_mailing_address": "contact_mailing_address",
            "website_link": "website_link",
        }
        for key, join_key in attr_join_key.items():
            etl.transform(
                arcetl.attributes.update_by_joined_value,
                field_name=key,
                join_dataset_path=dataset.FIRE_PROTECTION_PROVIDER.path(),
                join_field_name=join_key,
                on_field_pairs=[("fireprotprov", "provider_code")],
            )
        etl.load(dataset.FIRE_PROTECTION_AREA.path("pub"))


def psap_area_etl():
    """Run ETL for PSAP areas."""
    with arcetl.ArcETL("PSAP Areas") as etl:
        etl.extract(dataset.PSAP_AREA.path("maint"))
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.PSAP_AREA.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.load(dataset.PSAP_AREA.path("pub"))


def taxlot_fire_protection_etl():
    """Run ETL for taxlot fire protection cross-reference."""
    keys = {"taxlot": ["maptaxlot", "maptaxlot_hyphen", "map", "taxlot"]}
    with arcetl.ArcETL("Taxlot Fire Protection") as etl:
        etl.init_schema(dataset.TAXLOT_FIRE_PROTECTION.path())
        # To avoid memory/topoengine errors when processing, run ETL on subsets.
        subsets = taxlot_subset_temp_copies(REAL_LOT_SQL, field_names=keys["taxlot"])
        for subset in subsets:
            with subset:
                arcetl.dataset.add_field(
                    subset.path, field_name="provider_code", field_type="text"
                )
                arcetl.geoset.identity(
                    dataset_path=subset.path,
                    field_name="provider_code",
                    identity_dataset_path=dataset.FIRE_PROTECTION_AREA.path("pub"),
                    identity_field_name="fireprotprov",
                )
                # DO NOT remove features without overlay; unprotected areas important.
                # Dissolve on lot & overlay, for proper area representation.
                arcetl.features.dissolve(
                    dataset_path=subset.path,
                    dissolve_field_names=keys["taxlot"] + ["provider_code"],
                    tolerance=TOLERANCE["xy"],
                )
                etl.transform(
                    arcetl.features.insert_from_path,
                    insert_dataset_path=subset.path,
                    field_names=keys["taxlot"] + ["provider_code"],
                )
        # Join fire protection attributes.
        attr_join_key = {
            "provider_name": "fpprovname",
            "protection_type_code": "fireprottype",
            "protection_type_description": "fptypename",
            "tax_district": "taxdist",
            "contact_phone": "contact_phone",
            "contact_email": "contact_email",
            "contact_mailing_address": "contact_mailing_address",
            "website_link": "website_link",
        }
        for key, join_key in attr_join_key.items():
            etl.transform(
                arcetl.attributes.update_by_joined_value,
                field_name=key,
                join_dataset_path=dataset.FIRE_PROTECTION_AREA.path("pub"),
                join_field_name=join_key,
                on_field_pairs=[("provider_code", "fireprotprov")],
            )
        # Yeah, the -9999 is weird. However, trying to delete null-geometry features
        # that arise from every-so-tiny slivers in identity results, this was best way.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="approx_acres",
            function=(lambda x: x / 43560.0 if x is not None else -9999),
            field_as_first_arg=False,
            arg_field_names=["shape@area"],
        )
        etl.transform(arcetl.features.delete, dataset_where_sql="approx_acres < 0")
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
        # Remove minimal overlays.
        etl.transform(
            arcetl.features.delete, dataset_where_sql="taxlot_area_ratio <= 0.001"
        )
        etl.load(dataset.TAXLOT_FIRE_PROTECTION.path())


# Jobs.


BOUNDARY_DATASETS_JOB = Job(
    "Public_Safety_Boundary_Datasets",
    etls=[
        # Pass 1.
        ambulance_service_area_etl,
        fire_protection_area_etl,
        psap_area_etl,
        # Pass 2.
        emergency_service_zone_etl,
    ],
)


TAXLOT_FIRE_PROTECTION_JOB = Job(
    "Taxlot_Fire_Protection_Dataset", etls=[taxlot_fire_protection_etl]
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
