"""Execution code for Master Street Address Guide processing."""
import argparse
import datetime
import logging
import uuid

import arcetl
import arcpy
from etlassist.pipeline import Job, execute_pipeline


from helper import dataset
from helper.misc import parity


##TODO: Migrate `msag_ranges_current_etl` to `exec_address_datasets.py`, then deprecate this.

LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# Helpers.


def init_range(init_address):
    """Initialize info for MSAG range dictionary.

    Args:
        init_address (dict): Mapping of address attribute name to value.

    Returns:
        dict: Initial mapping of range attribute name to value.
    """
    new_range = init_address.copy()
    new_range["house_numbers"] = [new_range.pop("house_nbr")]
    new_range["points"] = [new_range.pop("shape@").firstPoint]
    return new_range


def finish_range(msag_range, geometry_style, spatial_reference):
    """Finish info for MSAG range dictionary.

    Args:
        msag_range (dict): Mapping of range attribute name to value.
        geometry_style (str): Style of geometry for range feature. Can be "convex-hull",
            "hull-rectangle", or "multipoint".
        spatial_reference (arcpy.SpatialReference): ArcPy spatial reference object.

    Returns:
        dict: Finalized mapping of range attribute name to value.
    """
    final_range = msag_range.copy()
    final_range["parity"] = parity(final_range["house_numbers"])
    final_range["parity_code"] = final_range["parity"][0].upper()
    final_range["from_house_number"] = min(final_range["house_numbers"])
    final_range["to_house_number"] = max(final_range["house_numbers"])
    if geometry_style == "convex-hull":
        final_range["shape@"] = (
            arcpy.Multipoint(arcpy.Array(final_range["points"]), spatial_reference)
            .convexHull()
            .buffer(50)
        )
    elif geometry_style == "hull-rectangle":
        LOG.warning(
            "Hull rectangle style currently cannot create polygons for 2-point ranges."
        )
        if len(final_range["points"]) == 1:
            centroid = final_range["points"][0]
            final_range["shape@"] = arcpy.Polygon(
                arcpy.Array(
                    [
                        arcpy.Point(centroid.X - 50, centroid.Y + 50),
                        arcpy.Point(centroid.X + 50, centroid.Y + 50),
                        arcpy.Point(centroid.X + 50, centroid.Y - 50),
                        arcpy.Point(centroid.X - 50, centroid.Y - 50),
                    ]
                ),
                spatial_reference,
            )
        else:
            points = arcpy.Multipoint(arcpy.Array(final_range["points"]))
            nums = [float(coord) for coord in points.hullRectangle.split()]
            final_range["shape@"] = arcpy.Polygon(
                arcpy.Array(
                    arcpy.Point(*nums[i : i + 2]) for i in range(0, len(nums), 2)
                ),
                spatial_reference,
            ).buffer(50)
    elif geometry_style == "multipoint":
        final_range["shape@"] = arcpy.Multipoint(arcpy.Array(final_range["points"]))
    return final_range


def msag_ranges(geometry_style="convex-hull"):
    """Generate Master Street Address Guide (MSAG) reference ranges.

    The order of the cursor rows is critically important! The rows must be order-grouped
    by a full street name + city combo, then ascending order of the house numbers. It is
    best to then sort by ESN, so that if an address straddles an ESZ boundary (e.g.
    multiple units), it will assign the same every time.

    Args:
        geometry_style (str): Style of geometry for range feature. Can be "convex-hull",
            "hull-rectangle", or "multipoint".

    Yields:
        dict: Mapping of range attribute name to value.
    """
    keys = {
        "street": ["pre_direction_code", "street_name", "street_type_code", "city_name"]
    }
    keys["range"] = keys["street"] + ["emergency_service_number"]
    keys["sort_order"] = keys["street"] + ["house_nbr", "emergency_service_number"]
    keys["address"] = keys["sort_order"] + ["shape@"]
    spatial_reference = arcetl.arcobj.spatial_reference(
        dataset.SITE_ADDRESS.path("pub")
    )
    msag_address_sql = """
        -- Omit archived addresses (but allow not-valid).
        archived != 'Y'
        -- Omit addresses where required values are missing.
        and house_nbr > 0 and house_nbr is not null
        and street_name is not null
        and city_name is not null
        and shape is not null and shape.STIsEmpty() != 1
    """
    # Need to extract as tuples, then zip into a dict to use the tuple-sorting.
    address_rows = arcetl.attributes.as_iters(
        dataset.SITE_ADDRESS.path("pub"),
        field_names=keys["address"],
        dataset_where_sql=msag_address_sql,
    )
    addresses = (dict(zip(keys["address"], row)) for row in sorted(address_rows))
    for i, address in enumerate(addresses):
        # Create range for first address.
        if i == 0:
            msag_range = init_range(address)
        # Address still on same city-street combo *and* same ESN: extend range.
        elif all(address[key] == msag_range[key] for key in keys["range"]):
            msag_range["house_numbers"].append(address["house_nbr"])
            msag_range["points"].append(address["shape@"].firstPoint)
        # Address changed street, city, or ESN.
        else:
            # Finish & yield range info before starting anew.
            yield finish_range(msag_range, geometry_style, spatial_reference)

            # Reset current attributes with current address.
            msag_range = init_range(address)
    # Outside loop, finish & append last range.
    yield finish_range(msag_range, geometry_style, spatial_reference)


# ETLs.


def msag_ranges_current_etl():
    """Run ETL for current model of Master Street Address Guide (MSAG)."""
    core_keys = [
        "emergency_service_number",
        "parity_code",
        "parity",
        "from_house_number",
        "to_house_number",
        "pre_direction_code",
        "street_name",
        "street_type_code",
        "city_name",
    ]
    with arcetl.ArcETL("MSAG Ranges - Current") as etl:
        etl.init_schema(dataset.MSAG_RANGE.path("current"))
        etl.transform(
            arcetl.features.insert_from_dicts,
            insert_features=msag_ranges,
            field_names=core_keys + ["shape@"],
        )
        old_msag_id = {
            row[:-1]: row[-1]
            for row in arcetl.attributes.as_iters(
                dataset.MSAG_RANGE.path("master"),
                field_names=core_keys + ["msag_id"],
                dataset_where_sql="expiration_date is null",
            )
        }

        def assign_msag_id(*core_values):
            """Assign MSAG ID for the given range core attributes."""
            if core_values in old_msag_id:
                result = old_msag_id[core_values]
            else:
                result = "{" + str(uuid.uuid4()) + "}"
            return result

        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="msag_id",
            function=assign_msag_id,
            field_as_first_arg=False,
            arg_field_names=core_keys,
        )
        etl.transform(
            arcetl.attributes.update_by_joined_value,
            field_name="city_code",
            join_field_name="CityNameAbbr",
            join_dataset_path=dataset.CITY.path(),
            on_field_pairs=[("city_name", "CityName")],
        )
        # Get effective date for ranges already in master.
        etl.transform(
            arcetl.attributes.update_by_joined_value,
            field_name="effective_date",
            join_field_name="effective_date",
            join_dataset_path=dataset.MSAG_RANGE.path("master"),
            on_field_pairs=[("msag_id", "msag_id")],
        )
        # Assign effective date for new ranges.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="effective_date",
            function=datetime.date.today,
            field_as_first_arg=False,
            dataset_where_sql="effective_date is null",
        )
        etl.load(dataset.MSAG_RANGE.path("current"))


# Jobs.


WEEKLY_JOB = Job("MSAG_Weekly", etls=[msag_ranges_current_etl])


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
