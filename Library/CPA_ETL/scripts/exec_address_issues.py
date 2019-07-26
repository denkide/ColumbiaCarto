"""Execution code for production address issues."""
import argparse
from collections import defaultdict
import datetime
import logging
from operator import itemgetter

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import dataset
from helper.value import concatenate_arguments


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# Helpers.


def concatenated_address(address):
    """Concatenate full address using given address parts.

    Args:
        address (dict): Mapping of attribute names to values.

    Returns:
        str: Full address as a single string.
    """
    return concatenate_arguments(
        address["house_nbr"],
        address["house_suffix_code"],
        address["pre_direction_code"],
        address["street_name"],
        address["street_type_code"],
        address["unit_type_code"],
        address["unit_id"],
    )


def init_issue(address, description="Init.", update_publication=True):
    """Initialize issue dictionary for address.

    Args:
        address (dict): Mapping of attribute names to values.
        description (str): Description of issue. Default is "Init.".
        update_publication (bool): True if address with issue may still be published
            as-is, False otherwise. Default is True.

    Returns:
        dict: Information about issue.
    """
    issue = {
        "concat_address": concatenated_address(address),
        "description": description,
        "update_publication": update_publication,
    }
    for key in ["site_address_gfid", "geofeat_id", "city_name", "shape@"]:
        issue[key] = address[key]
    return issue


def issue_maint_info_map():
    """Create map of issues (address ID + description) to maintenance notes.

    Returns:
        dict: Mapping of address IDs to maintenance info-dicts.
    """
    issues = arcetl.attributes.as_dicts(
        dataset.ADDRESS_ISSUES.path(),
        field_names=[
            "site_address_gfid",
            "description",
            "maint_notes",
            "maint_init_date",
        ],
    )
    return {
        (issue["site_address_gfid"], issue["description"]): issue for issue in issues
    }


def last_update_days_ago(address):
    """Return days since last update for address.

    Args:
        address (dict): Mapping of attribute names to values.

    Returns:
        int: Number of days.
    """
    if address["last_update_date"]:
        date = address["last_update_date"]
    else:
        date = address["initial_create_date"]
    return (datetime.datetime.now() - date).total_seconds() / 86400


# Issues generators.


def assess_tax_issues(addresses):
    """Generate issue dictionary regarding address A&T attributes.

    Args:
        addresses (iter of dicts): Collection of address atttributes to evaluate.

    Yields:
        dict: Information about issue.
    """
    exemptions = [
        {
            "house_nbr": 26629,
            "pre_direction_code": None,
            "street_name": "BENNETT",
            "street_type_code": "BLVD",
            "city_name": "Monroe",
        },
        {
            "house_nbr": 97089,
            "pre_direction_code": None,
            "street_name": "FIVE RIVERS",
            "street_type_code": "RD",
            "city_name": "Tidewater",
        },
    ]
    for address in addresses:
        if address["archived"] == "Y":
            continue

        if any(
            all(address[key] == val for key, val in exemption.items())
            for exemption in exemptions
        ):
            continue

        if all([address["maptaxlot"] is None, last_update_days_ago(address) > 10]):
            description = (
                "`maptaxlot` must not be `null` (10-day delay from last edit)."
            )
            yield init_issue(address, description, update_publication=True)

        if all(
            [
                address["maptaxlot"] is not None,
                address["account"] is None,
                last_update_days_ago(address) > 45,
            ]
        ):
            description = (
                "`account` must not be `null` when `maptaxlot` is not `null`"
                " (45-day delay from last edit)."
            )
            yield init_issue(address, description, update_publication=True)


def city_issues(addresses):
    """Generate issue dictionary regarding address city attributes.

    Args:
        addresses (iter of dicts): Collection of address atttributes to evaluate.

    Yields:
        dict: Information about issue.
    """
    valid = {
        "city_name": {
            city
            for city, in arcetl.attributes.as_iters(
                dataset.CITY.path(),
                field_names=["cityname"],
                dataset_where_sql="ismailcity = 'Y'",
            )
        }
    }
    for address in addresses:
        if address["city_name"] is None:
            description = "`city_name` must not be `null`."
            yield init_issue(address, description, update_publication=False)

        elif address["city_name"] not in valid["city_name"]:
            description = (
                "`city_name` must be in `cityname` field of (LCOGGeo) City table"
                " where `ismailcity`='Y.'"
            )
            yield init_issue(address, description, update_publication=False)


def duplicate_issues(addresses):
    """Generate issue dictionary regarding address core duplicates.

    Core address is defined as `concat_address` & `city_name`. The address with the
    earliest `init_create_date` will not be flagged for not publishing.

    Args:
        addresses (iter of dicts): Collection of address atttributes to evaluate.

    Yields:
        dict: Information about issue.
    """
    core_address_map = defaultdict(list)
    for address in addresses:
        if address["archived"] == "Y":
            continue

        core_address = (concatenated_address(address), address["city_name"])
        core_address_map[core_address].append(address)
    for addrs in core_address_map.values():
        # Ignore non-duplicates.
        if len(addrs) == 1:
            continue

        addrs = sorted(addrs, key=itemgetter("initial_create_date"))
        for i, address in enumerate(addrs):
            description = "Must have unique core address."
            # Allow first to publish, others to not.
            yield init_issue(
                address, description, update_publication=(True if i == 0 else False)
            )


def extended_issues(addresses):
    """Generate issue dictionary regarding address extended attributes.

    Args:
        addresses (iter of dicts): Collection of address atttributes to evaluate.

    Yields:
        dict: Information about issue.
    """
    valid = {
        "infill": {"0", "1", "2", "3", "4"},
        "land_use": {
            use
            for use, in arcetl.attributes.as_iters(
                dataset.LAND_USE_CODES_DETAILED.path("maint"), field_names=["landusec"]
            )
        },
        "structure": {
            structure
            for structure, in arcetl.attributes.as_iters(
                dataset.STRUCTURE_TYPE.path(), field_names=["code"]
            )
        },
        "zip_code": {
            code
            for code, in arcetl.attributes.as_iters(
                dataset.ZIP_CODE_AREA.path("maint"), field_names=["zipcode"]
            )
        },
    }
    for address in addresses:
        if address["archived"] == "Y":
            continue

        if address["five_digit_zip_code"] not in valid["zip_code"]:
            # Postal sorting facility in Springfield has its own ZIP code.
            if all(
                [
                    address["five_digit_zip_code"] == "97475",
                    address["house_nbr"] == 3148,
                    address["pre_direction_code"] is None,
                    address["street_name"] == "GATEWAY",
                    address["street_type_code"] == "ST",
                    address["city_name"] == "Springfield",
                ]
            ):
                pass

            else:
                description = (
                    "`five_digit_zip_code` must be in `zipcode` field"
                    " of (RLIDGeo) ZIPCode feature class."
                )
                yield init_issue(address, description, update_publication=True)

        if address["landuse"] not in valid["land_use"]:
            description = (
                "`landuse` must be in `landusec` field"
                " of (RLIDGeo) LanduseCodesDetailed table."
            )
            yield init_issue(address, description, update_publication=True)

        if address["infill"] not in valid["infill"]:
            description = """`infill` must be "0", "1", "2", "3", or "4"."""
            yield init_issue(address, description, update_publication=True)

        if address["structure"] not in valid["structure"]:
            description = "`structure` must be in `code` field of StructureType table."
            yield init_issue(address, description, update_publication=True)

        if address["drive_id"] is not None:
            parts = address["drive_id"].split("-")
            if any([len(parts) != 2, parts[0] != "DRVWY", not parts[-1].isdigit()]):
                description = (
                    """`drive_id` must be `null` or formatted "DRVWY-{number}"."""
                )
                yield init_issue(address, description, update_publication=True)


##TODO: geometry_issues: not None, no empty, outside county (envelope?). Then can remove from prod-proc.


def house_number_issues(addresses):
    """Generate issue dictionary regarding address house numbers.

    Args:
        addresses (iter of dicts): Collection of address atttributes to evaluate.

    Yields:
        dict: Information about issue.
    """
    valid = {
        "suffix": {
            suffix
            for suffix, in arcetl.attributes.as_iters(
                dataset.HOUSE_SUFFIX.path(), field_names=["code"]
            )
        }
    }
    for address in addresses:
        if address["archived"] == "Y":
            continue

        if address["house_nbr"] is None:
            description = "`house_nbr` must not be `null`."
            yield init_issue(address, description, update_publication=False)

        elif address["house_nbr"] <= 0:
            description = "`house_nbr` must not be zero or negative."
            yield init_issue(address, description, update_publication=False)

        elif address["house_nbr"] > 97999:
            description = (
                "`house_nbr` must not be greater than 97999"
                " (highest number in the county grid)."
            )
            yield init_issue(address, description, update_publication=False)

        if address["house_suffix_code"] not in valid["suffix"]:
            description = (
                "`house_suffix_code` must be in `code` field of HouseSuffix table."
            )
            yield init_issue(address, description, update_publication=False)


def maintenance_issues(addresses):
    """Generate issue dictionary regarding address maintenance attributes.

    Args:
        addresses (iter of dicts): Collection of address atttributes to evaluate.

    Yields:
        dict: Information about issue.
    """
    valid = {
        "lmh": {"L", "M", "H"},
        "lmh_none": {"L", "M", "H", None},
        "location": {"APPROXIMATE", "VERIFIED", None},
        "point_review": {"NEW", "OK", None},
        "problem_address": {"D", "EO", "M", "OTH", "R", "S", "SC", "WS", None},
        "x_none": {"X", None},
        "yn": {"N", "Y"},
        "yn_none": {"N", "Y", None},
    }
    for address in addresses:
        if address["valid"] not in valid["yn"]:
            description = """`valid` must be "N" or "Y"."""
            yield init_issue(address, description, update_publication=False)

        if address["archived"] not in valid["yn"]:
            description = """`archived` must be "N" or "Y"."""
            yield init_issue(address, description, update_publication=False)

        # if address["structure"] is not None:
        #     if any(["DELETED" in address["structure"], "DEMO" in address["structure"]]):
        #         if address["archived"] == "N":
        #             description = (
        #                 """`archived` should be "Y" if `structure`"""
        #                 " indicates demolished or deleted."
        #             )
        #             yield init_issue(address, description, update_publication=True)
        #
        #     elif address["archived"] == "Y":
        #         description = (
        #             """`archived` should by "N" if `structure`"""
        #             " does not indicate demolished or deleted."
        #         )
        #         yield init_issue(address, description, update_publication=True)

        if address["address_confidence"] not in valid["lmh_none"]:
            description = (
                """`address_confidence` must be `null` (unknown), "L", "M", or "H"."""
            )
            yield init_issue(address, description, update_publication=True)

        if address["location"] not in valid["location"]:
            description = (
                """`location` must be `null` (unknown), "APPROXIMATE" or "VERIFIED"."""
            )
            yield init_issue(address, description, update_publication=True)

        if address["outofseq"] not in valid["x_none"]:
            description = """`outofseq` must be `null` or "X"."""
            yield init_issue(address, description, update_publication=True)

        if address["source_from_field_ind"] not in valid["yn"]:
            description = """`source_from_field_ind` must be "N" or "Y"."""
            yield init_issue(address, description, update_publication=True)

        if address["renumbering_ind"] not in valid["yn"]:
            description = """`renumbering_ind` must be "N" or "Y"."""
            yield init_issue(address, description, update_publication=True)

        if address["point_review"] not in valid["point_review"]:
            description = """`point_review` must be `null` (unknown), "NEW" or "OK"."""
            yield init_issue(address, description, update_publication=True)

        if address["point_review"] in (None, "NEW"):
            if address["point_review_date"] is not None:
                description = (
                    "`point_review_date` must be `null`"
                    """ if `point_review` is `null` (unknown), or "NEW"."""
                )
                yield init_issue(address, description, update_publication=True)

        if address["problem_address"] not in valid["problem_address"]:
            description = (
                "`problem_address` must be"
                """ `null`, "D", "EO", "M", "OTH", "R", "S", "SC", or "WS"."""
            )
            yield init_issue(address, description, update_publication=True)

        if address["sent_to_juris"] not in valid["yn"]:
            description = """`sent_to_juris` must be "N" or "Y"."""
            yield init_issue(address, description, update_publication=True)

        if address["conflict_w_juris"] not in valid["yn"]:
            description = """`conflict_w_juris` must be "N" or "Y"."""
            yield init_issue(address, description, update_publication=True)


def overlay_issues(addresses):
    """Generate issue dictionary regarding critical address overlays.

    Args:
        addresses (iter of dicts): Collection of address atttributes to evaluate.

    Yields:
        dict: Information about issue.
    """
    field_overlay_kwargs = {
        "firedist": {
            "overlay_field_name": "fireprotprov",
            "overlay_dataset_path": dataset.FIRE_PROTECTION_AREA.path("pub"),
        }
    }
    keys = {
        "overlay": list(field_overlay_kwargs),
        "extract": ["site_address_gfid"] + list(field_overlay_kwargs),
    }
    overlay = {
        "old": {
            addr["site_address_gfid"]: addr
            for addr in arcetl.attributes.as_dicts(
                dataset.SITE_ADDRESS.path("pub"), keys["extract"]
            )
        }
    }
    addr_copy = arcetl.TempDatasetCopy(
        dataset.SITE_ADDRESS.path("maint"), field_names=["site_address_gfid"]
    )
    with addr_copy:
        for key, kwargs in field_overlay_kwargs.items():
            field_meta = next(
                field for field in dataset.SITE_ADDRESS.fields if field["name"] == key
            )
            arcetl.dataset.add_field_from_metadata(
                addr_copy.path, field_meta, log_level="debug"
            )
            arcetl.attributes.update_by_overlay(
                addr_copy.path,
                field_name=key,
                overlay_central_coincident=True,
                log_level="debug",
                **kwargs
            )
        overlay["new"] = {
            addr["site_address_gfid"]: addr
            for addr in arcetl.attributes.as_dicts(addr_copy.path, keys["extract"])
        }
    for address in addresses:
        if address["archived"] == "Y":
            continue

        if address["site_address_gfid"] not in overlay["old"]:
            continue

        for key in keys["overlay"]:
            val = {
                tag: overlay[tag][address["site_address_gfid"]][key]
                for tag in ["old", "new"]
            }
            if all([val["old"] != val["new"], last_update_days_ago(address) < 15]):
                description = (
                    "Maintenance changes must not change `{}` overlay"
                    " (temporary: 15-day hold from last edit)."
                ).format(key)
                yield init_issue(address, description, update_publication=False)


def street_issues(addresses):
    """Generate issue dictionary regarding address streets.

    Args:
        addresses (iter of dicts): Collection of address atttributes to evaluate.

    Yields:
        dict: Information about issue.
    """
    street_city_keys = [
        "pre_direction_code",
        "street_name",
        "street_type_code",
        "city_name",
    ]
    valid = {
        "direction_code": {
            _dir
            for _dir, in arcetl.attributes.as_iters(
                dataset.STREET_DIRECTION.path(), field_names=["code"]
            )
        },
        "type_code": {
            _type
            for _type, in arcetl.attributes.as_iters(
                dataset.STREET_TYPE.path(), field_names=["code"]
            )
        },
        "street_city": {
            vals
            for vals in arcetl.attributes.as_iters(
                dataset.STREET_NAME_CITY.path(), field_names=street_city_keys
            )
        },
    }
    for address in addresses:
        if address["archived"] == "Y":
            continue

        if address["pre_direction_code"] not in valid["direction_code"]:
            description = (
                "`pre_direction_code` must be in `code` field of StreetDirection table."
            )
            yield init_issue(address, description, update_publication=False)

        if address["street_name"] is None:
            description = "`street_name` must not be `null`"
            yield init_issue(address, description, update_publication=False)

        if address["street_type_code"] not in valid["type_code"]:
            description = (
                "`street_type_code` must be in `code` field of StreetType table."
            )
            yield init_issue(address, description, update_publication=False)

        street_city = tuple(address[key] for key in street_city_keys)
        if street_city not in valid["street_city"]:
            description = (
                "Street-city combination must match all fields of StreetNameCity table."
            )
            yield init_issue(address, description, update_publication=False)


def unit_issues(addresses):
    """Generate issue dictionary regarding address units.

    Args:
        addresses (iter of dicts): Collection of address atttributes to evaluate.

    Yields:
        dict: Information about issue.
    """
    valid = {
        "type": {
            _type
            for _type, in arcetl.attributes.as_iters(
                dataset.UNIT_TYPE.path(), field_names=["code"]
            )
        },
        "id": {
            _id
            for _id, in arcetl.attributes.as_iters(
                dataset.UNIT_ID.path(), field_names=["unit_id"]
            )
        },
    }
    for address in addresses:
        if address["archived"] == "Y":
            continue

        if address["unit_type_code"] not in valid["type"]:
            description = "`unit_type_code` must be in `code` field of UnitType table."
            yield init_issue(address, description, update_publication=False)

        if address["unit_id"] not in valid["id"]:
            description = "`unit_id` must be in `unit_id` field of UnitID table."
            yield init_issue(address, description, update_publication=False)

        if all([address["unit_id"] is None, address["unit_type_code"] is not None]):
            description = (
                "`unit_id` must not be `null` if `unit_type_code` is not `null`."
            )
            yield init_issue(address, description, update_publication=False)


# ETLs.


def issues_update():
    """Run procedures to find invalid address maintenance attributes."""
    LOG.info("Start: Collect address attributes.")
    address_field_names = [
        field["name"]
        for field in dataset.SITE_ADDRESS.fields
        if "maint" in field["tags"]
    ] + ["shape@"]
    addresses = list(
        arcetl.attributes.as_dicts(
            dataset.SITE_ADDRESS.path("maint"),
            field_names=address_field_names,
            dataset_where_sql="site_address_gfid is not null",
        )
    )
    LOG.info("End: Collect.")
    LOG.info("Start: Validate address attributes.")
    issues = []
    issues_functions = [
        house_number_issues,
        street_issues,
        unit_issues,
        city_issues,
        extended_issues,
        maintenance_issues,
        assess_tax_issues,
        duplicate_issues,
        overlay_issues,
    ]
    for function in issues_functions:
        LOG.info(function.__name__)
        issues.extend(function(addresses))
    LOG.info("End: Validate.")
    with arcetl.ArcETL("Address Issues") as etl:
        etl.init_schema(dataset.ADDRESS_ISSUES.path())
        if issues:
            maint_info = issue_maint_info_map()
            for issue in issues:
                maint_id = (issue["site_address_gfid"], issue["description"])
                issue.update(maint_info.get(maint_id, dict()))
                issue.setdefault("maint_notes")
                issue.setdefault("maint_init_date", datetime.datetime.now())
            etl.transform(
                arcetl.features.insert_from_dicts,
                insert_features=issues,
                field_names=issues[0].keys(),
            )
        etl.update(
            dataset.ADDRESS_ISSUES.path(),
            id_field_names=["site_address_gfid", "description"],
        )


# Jobs.


NIGHTLY_JOB = Job("Address_Issues_Nightly", etls=[issues_update])


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
