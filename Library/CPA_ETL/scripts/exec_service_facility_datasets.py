"""Execution code for service area processing."""
import argparse
import logging

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import dataset
from helper import transform


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

MAX_COST_FEET = 8 * 5280  # Eight miles in feet.
PROVIDER_CLOSEST_FIRE_STATION_KWARGS = {
    "Coburg RFPD": {
        "dataset_where_sql": "firedist = 'CBF'",
        "facility_where_sql": "type = 'fire' and label_full like 'Coburg Fire %'",
    },
    "Dexter RFPD": {
        "dataset_where_sql": "firedist = 'DEF'",
        "facility_where_sql": "type = 'fire' and label_full like 'Dexter Fire %'",
    },
    "Eugene Airport": {
        "dataset_where_sql": "firedist = 'AIR'",
        "facility_where_sql": (
            "type = 'fire' and label_full = 'Eugene Fire Station 12 (Airport)'"
        ),
    },
    # This one takes forever.
    "Eugene/Springfield Fire": {
        "dataset_where_sql": "firedist in ('EGF', 'SPF')",
        "facility_where_sql": """
            type = 'fire' and (
                label_full like 'Eugene Fire %'
                or label_full like 'Springfield Fire %'
            )
            and label_full <> 'Eugene Fire Station 12 (Airport)'
        """,
    },
    "Eugene/Springfield Fire Contracts": {
        "dataset_where_sql": (
            "firedist in ('BAS', 'EU1', 'GLW', 'RAB', 'RIR', 'WLE', 'WLS', 'ZUM')"
        ),
        "facility_where_sql": """
            type = 'fire' and (
                label_full like 'Eugene Fire %'
                or label_full like 'Springfield Fire %'
            )
            and label_full <> 'Eugene Fire Station 12 (Airport)'
        """,
    },
    "Goshen RFPD": {
        "dataset_where_sql": "firedist = 'GOF'",
        "facility_where_sql": """
            type = 'fire' and (
                label_full like 'Goshen Fire %'
                or label_full like 'Pleasant Hill Fire %'
            )
        """,
    },
    "Hazeldell RFD": {
        "dataset_where_sql": "firedist = 'HDF'",
        # Hazeldell, Oakridge, & Westfir have support IGA.
        "facility_where_sql": """
            type = 'fire' and (
                label_full like 'High Prairie Fire %'
                or label_full like 'Oakridge Fire %'
                or label_full like 'Westfir Fire %'
            )
        """,
    },
    "Junction City RFPD": {
        "dataset_where_sql": "firedist = 'JCF'",
        "facility_where_sql": (
            "type = 'fire' and label_full like 'Junction City Fire %'"
        ),
    },
    "Lake Creek RFPD": {
        "dataset_where_sql": "firedist = 'LCF'",
        "facility_where_sql": """
            type = 'fire' and (
                label_full like 'Horton Road Fire %'
                or label_full like 'Lake Creek Fire %'
            )
        """,
    },
    "Lane Fire Authority": {
        "dataset_where_sql": "firedist = 'LFA'",
        "facility_where_sql": "type = 'fire' and label_full like 'Lane Fire %'",
    },
    "Lorane RFPD": {
        "dataset_where_sql": "firedist = 'LOF'",
        "facility_where_sql": "type = 'fire' and label_full like 'Lorane Fire %'",
    },
    "Lowell RFPD": {
        "dataset_where_sql": "firedist = 'LWF'",
        "facility_where_sql": """
            type = 'fire' and (
                label_full like 'Fall Creek Fire %' or label_full like 'Lowell Fire %'
            )
        """,
    },
    "Mapleton FD": {
        "dataset_where_sql": "firedist = 'MPF'",
        "facility_where_sql": "type = 'fire' and label_full like 'Mapleton Fire %'",
    },
    "McKenzie Fire & Rescue": {
        "dataset_where_sql": "firedist = 'MKF'",
        "facility_where_sql": "type = 'fire' and label_full like 'McKenzie Fire %'",
    },
    "Mohawk Valley FD": {
        "dataset_where_sql": "firedist = 'MVF'",
        "facility_where_sql": (
            "type = 'fire' and label_full like 'Mohawk Valley Fire %'"
        ),
    },
    "Monroe RFPD": {
        "dataset_where_sql": "firedist = 'MRF'",
        "facility_where_sql": "type = 'fire' and label_full like 'Monroe Fire %'",
    },
    "Oakridge FD": {
        "dataset_where_sql": "firedist = 'OKF'",
        # Hazeldell, Oakridge, & Westfir have support IGA.
        "facility_where_sql": """
            type = 'fire' and (
                label_full like 'High Prairie Fire %'
                or label_full like 'Oakridge Fire %'
                or label_full like 'Westfir Fire %'
            )
        """,
    },
    "Pleasant Hill RFPD": {
        "dataset_where_sql": "firedist = 'PHF'",
        "facility_where_sql": """
            type = 'fire' and (
                label_full like 'Goshen Fire %'
                or label_full like 'Pleasant Hill Fire %'
            )
        """,
    },
    "Santa Clara RFPD": {
        "dataset_where_sql": "firedist = 'SCF'",
        "facility_where_sql": "type = 'fire' and label_full like 'Santa Clara Fire %'",
    },
    "Siuslaw Valley Fire & Rescue": {
        "dataset_where_sql": "firedist = 'SVF'",
        "facility_where_sql": (
            "type = 'fire' and label_full like 'Siuslaw Valley Fire %'"
        ),
    },
    "South Lane County Fire & Rescue": {
        "dataset_where_sql": "firedist = 'SOL'",
        "facility_where_sql": (
            "type = 'fire' and label_full like 'South Lane County Fire %'"
        ),
    },
    "Swisshome-Deadwood RFPD": {
        "dataset_where_sql": "firedist = 'SDF'",
        "facility_where_sql": (
            "type = 'fire' and label_full like 'Swisshome-Deadwood Fire %'"
        ),
    },
    "Upper McKenzie RFPD": {
        "dataset_where_sql": "firedist = 'UMF'",
        "facility_where_sql": (
            "type = 'fire'and label_full like 'Upper McKenzie Fire %'"
        ),
    },
    "Westfir FD": {
        "dataset_where_sql": "firedist = 'WEF'",
        # Hazeldell, Oakridge, & Westfir have support IGA.
        "facility_where_sql": """
            type = 'fire' and (
                label_full like 'High Prairie Fire %'
                or label_full like 'Oakridge Fire %'
                or label_full like 'Westfir Fire %'
            )
        """,
    },
}


# Helpers.


def closest_facility_route_wrapper(provider_key, dataset_where_sql, facility_where_sql):
    """Generate closest facility by route with correct keynames.

    Args:
        provider_key (str): Key or name for provider.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        facility_where_sql (str): SQL where-clause for facilities dataset subselection.

    Yields:
        dict: Mapping of attribute name to value for a single route.
    """
    routes = arcetl.network.closest_facility_route(
        dataset_path=dataset.SITE_ADDRESS.path("pub"),
        # Route layer won"t transfer UUID type, so use integer ID.
        id_field_name="geofeat_id",
        dataset_where_sql=dataset_where_sql,
        facility_path=dataset.FACILITY.path("pub"),
        facility_id_field_name="label_full",
        facility_where_sql=facility_where_sql,
        network_path=dataset.SIMPLE_NETWORK.path(),
        cost_attribute="Length",
        max_cost=MAX_COST_FEET,
        restriction_attributes=["Oneway"],
        travel_from_facility=True,
    )
    for route in routes:
        if route["facility_id"] is not None:
            yield {
                "site_address_intid": route["dataset_id"],
                "provider_name": provider_key,
                "facility_name": route["facility_id"],
                "facility_distance_feet": route["cost"],
                "shape@": route["geometry"],
            }


# ETLs.


def closest_fire_station_etl():
    """Run ETL for closest fire station to each address.

    Only stations for the provider to the address will be considered.
    """
    with arcetl.ArcETL("Closest fire stations for addresses") as etl:
        etl.init_schema(dataset.SITE_ADDRESS_CLOSEST_FIRE_STATION.path())
        for provider_name, kwargs in PROVIDER_CLOSEST_FIRE_STATION_KWARGS.items():
            LOG.info("Start: Add stations for addresses in %s.", provider_name)
            etl.transform(
                arcetl.features.insert_from_dicts,
                insert_features=closest_facility_route_wrapper(provider_name, **kwargs),
                field_names=[
                    "site_address_intid",
                    "provider_name",
                    "facility_name",
                    "facility_distance_feet",
                    "shape@",
                ],
            )
        LOG.info("End: Add stations.")
        join_kwargs = [
            {
                "field_name": "site_address_uuid",
                "join_field_name": "site_address_gfid",
                "join_dataset_path": dataset.SITE_ADDRESS.path("pub"),
                "on_field_pairs": [("site_address_intid", "geofeat_id")],
            },
            {
                "field_name": "facility_intid",
                "join_field_name": "address_intid",
                "join_dataset_path": dataset.FACILITY.path("pub"),
                "on_field_pairs": [("facility_name", "label_full")],
            },
            {
                "field_name": "facility_uuid",
                "join_field_name": "address_uuid",
                "join_dataset_path": dataset.FACILITY.path("pub"),
                "on_field_pairs": [("facility_name", "label_full")],
            },
            {
                "field_name": "facility_x_coordinate",
                "join_field_name": "x_coordinate",
                "join_dataset_path": dataset.FACILITY.path("pub"),
                "on_field_pairs": [("facility_name", "label_full")],
            },
            {
                "field_name": "facility_y_coordinate",
                "join_field_name": "y_coordinate",
                "join_dataset_path": dataset.FACILITY.path("pub"),
                "on_field_pairs": [("facility_name", "label_full")],
            },
            {
                "field_name": "facility_longitude",
                "join_field_name": "longitude",
                "join_dataset_path": dataset.FACILITY.path("pub"),
                "on_field_pairs": [("facility_name", "label_full")],
            },
            {
                "field_name": "facility_latitude",
                "join_field_name": "latitude",
                "join_dataset_path": dataset.FACILITY.path("pub"),
                "on_field_pairs": [("facility_name", "label_full")],
            },
        ]
        for kwargs in join_kwargs:
            etl.transform(arcetl.attributes.update_by_joined_value, **kwargs)
        etl.load(dataset.SITE_ADDRESS_CLOSEST_FIRE_STATION.path())


def closest_hydrant_etl():
    """Run ETL for closest hydrant for each site address."""
    etl = arcetl.ArcETL("Closest hydrants for addresses")
    hydrants_copy = arcetl.TempDatasetCopy(dataset.HYDRANT.path(), field_names=[])
    with etl, hydrants_copy:
        # Add unique ID, longitude, latitude to temp hydrants.
        arcetl.dataset.add_field(
            hydrants_copy.path, field_name="hydrant_id", field_type="long"
        )
        arcetl.attributes.update_by_unique_id(hydrants_copy.path, "hydrant_id")
        for field_name, axis in [("longitude", "x"), ("latitude", "y")]:
            arcetl.dataset.add_field(
                hydrants_copy.path, field_name, field_type="double"
            )
            arcetl.attributes.update_by_geometry(
                hydrants_copy.path,
                field_name,
                spatial_reference_item=4326,
                geometry_properties=["centroid", axis],
            )
        etl.extract(dataset.SITE_ADDRESS.path("pub"))
        field_name_change = {
            "site_address_gfid": "site_address_uuid",
            "geofeat_id": "site_address_intid",
        }
        transform.rename_fields(etl, field_name_change)
        transform.add_missing_fields(etl, dataset.SITE_ADDRESS_CLOSEST_HYDRANT)
        id_near_info = arcetl.proximity.id_near_info_map(
            dataset_path=etl.transform_path,
            dataset_id_field_name="site_address_intid",
            near_dataset_path=hydrants_copy.path,
            near_id_field_name="hydrant_id",
            near_rank=1,
        )
        near_key_field_name = {
            "near_id": "facility_intid",
            "distance": "facility_distance_feet",
            "near_x": "facility_x_coordinate",
            "near_y": "facility_y_coordinate",
        }
        for near_key, field_name in near_key_field_name.items():
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=field_name,
                function=(lambda id_, key=near_key: id_near_info[id_][key]),
                field_as_first_arg=False,
                arg_field_names=["site_address_intid"],
            )
        # Add longitude/latitude.
        for name in ["longitude", "latitude"]:
            etl.transform(
                arcetl.attributes.update_by_joined_value,
                field_name="facility_" + name,
                join_dataset_path=hydrants_copy.path,
                join_field_name=name,
                on_field_pairs=[("facility_intid", "hydrant_id")],
            )
        # Remove features without a near-hydrant (should not happen).
        etl.transform(
            arcetl.features.delete, dataset_where_sql="facility_intid is null"
        )
        etl.load(dataset.SITE_ADDRESS_CLOSEST_HYDRANT.path())


# Jobs.


WEEKLY_JOB = Job(
    "Service_Facility_Datasets_Weekly",
    etls=[closest_hydrant_etl, closest_fire_station_etl]
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
