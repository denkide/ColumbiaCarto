"""Execution code for address assesment & taxation info processing."""
import argparse
from collections import defaultdict
import logging

import arcetl
import arcpy
from etlassist.pipeline import Job, execute_pipeline

from helper import dataset
from helper.misc import CURRENT_TAX_YEAR, rlid_accounts, rlid_tax_year_accounts


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

ADDRESS_WHERE_SQL = {"base": "site_address_gfid is not null and geofeat_id is not null"}
"""dict: Mapping of a string key to a SQL where-clause for address filtering."""
ADDRESS_WHERE_SQL["unarchived"] = ADDRESS_WHERE_SQL["base"] + " and archived = 'N'"
ADDRESS_WHERE_SQL["valid"] = ADDRESS_WHERE_SQL["unarchived"] + " and valid = 'Y'"


# Helpers.


def address_accounts():
    """Generate address ID & tax account tuple."""
    taxlot_accounts = taxlot_accounts_map()
    taxlot_code_accounts = taxlot_code_accounts_map()
    addr_tax_code = arcetl.attributes.id_map(
        dataset.ADDRESS_ASSESS_TAX_INFO.path(),
        id_field_names=["geofeat_id"],
        field_names=["tax_code_overlay"],
    )
    g_addr_attrs = arcetl.attributes.as_dicts(
        dataset.SITE_ADDRESS.path("maint"),
        field_names=["geofeat_id", "maptaxlot", "account", "valid", "archived"],
        dataset_where_sql=ADDRESS_WHERE_SQL["valid"],
    )
    for addr in g_addr_attrs:
        addr["tax_code"] = addr_tax_code[addr["geofeat_id"]]
        addr["maptaxlot_code"] = (addr["maptaxlot"], addr["tax_code"])
        # Order of checks matter!
        # Manually-set no-account flag: Preserve.
        if addr["account"] == "NO ACCT":
            pass

        # Archived & invalid address: No-account flag.
        elif addr["archived"] == "Y" or addr["valid"] == "N":
            addr["account"] = "NO ACCT"
        # Missing taxlot: Clear account.
        elif not addr["maptaxlot"]:
            addr["account"] = None
        # Address in right-of-way: ROW exception flag.
        elif int(addr["maptaxlot"][-5:]) < 100:
            addr["account"] = "ROAD"
        # Taxlot not (yet) in RLID A&T data: Preserve set account.
        elif addr["maptaxlot"] not in taxlot_accounts:
            pass

        # Only one account associated with taxlot: Assign.
        elif len(taxlot_accounts[addr["maptaxlot"]]) == 1:
            addr["account"] = min(taxlot_accounts[addr["maptaxlot"]])
        # Past here means multiple accounts on taxlot.
        # Taxlot-code pair not (yet) in RLID A&T data: Preserve set account.
        elif addr["maptaxlot_code"] not in taxlot_code_accounts:
            pass

        # Only one account associated with taxlot/tax code combo: Assign.
        elif len(taxlot_code_accounts[addr["maptaxlot_code"]]) == 1:
            addr["account"] = min(taxlot_code_accounts[addr["maptaxlot_code"]])
        # Past here means multiple accounts on taxlot-code combo.
        # Current account is valid for taxlot: Preserve set account.
        elif addr["account"] and addr["account"] in taxlot_accounts[addr["maptaxlot"]]:
            pass

        # Didn not satisify any of the above criteria: Clear account.
        else:
            addr["account"] = None
        yield (addr["geofeat_id"], addr["account"])


def address_maptaxlots():
    """Generate address ID & maptaxlot tuple."""
    address_copy = arcetl.TempDatasetCopy(
        dataset.SITE_ADDRESS.path("maint"),
        dataset_where_sql=ADDRESS_WHERE_SQL["unarchived"],
        field_names=["geofeat_id", "maptaxlot"],
    )
    taxlot_copy = arcetl.TempDatasetCopy(
        dataset.TAXLOT.path("maint"), field_names=["maptaxlot"]
    )
    with address_copy, taxlot_copy:
        arcetl.dataset.rename_field(
            dataset_path=taxlot_copy.path,
            field_name="maptaxlot",
            new_field_name="overlay_maptaxlot",
            log_level=None,
        )
        output_path = arcetl.unique_path()
        arcpy.analysis.SpatialJoin(
            target_features=address_copy.path,
            join_features=taxlot_copy.path,
            out_feature_class=output_path,
            join_operation="join_one_to_many",
            join_type="keep_all",
            match_option="within",
        )
    addr_maptaxlot = {}
    g_addr_maptaxlots = arcetl.attributes.as_iters(
        output_path, field_names=["geofeat_id", "maptaxlot", "overlay_maptaxlot"]
    )
    for addr_id, maptaxlot, overlay_maptaxlot in g_addr_maptaxlots:
        if addr_id not in addr_maptaxlot:
            addr_maptaxlot[addr_id] = {"current": maptaxlot, "overlays": set()}
        if overlay_maptaxlot:
            addr_maptaxlot[addr_id]["overlays"].add(overlay_maptaxlot)
    arcetl.dataset.delete(output_path)
    for addr_id, maptaxlot in addr_maptaxlot.items():
        # OK if assigned taxlot in overlaid taxlots.
        if maptaxlot["current"] in maptaxlot["overlays"]:
            yield (addr_id, maptaxlot["current"])

        # Can assign if only one overlaid taxlot.
        elif len(maptaxlot["overlays"]) == 1:
            yield (addr_id, maptaxlot["overlays"].pop())

        # Cannot assign if there are multiple: needs manual override.
        else:
            yield (addr_id, None)


def address_tax_codes():
    """Generate address ID & tax code tuple."""
    address_copy = arcetl.TempDatasetCopy(
        dataset.SITE_ADDRESS.path("maint"),
        dataset_where_sql=ADDRESS_WHERE_SQL["valid"],
        field_names=["geofeat_id"],
    )
    with address_copy:
        arcetl.dataset.add_field(
            address_copy.path,
            field_name="tax_code",
            field_type="text",
            field_length=6,
            log_level=None,
        )
        arcetl.attributes.update_by_overlay(
            address_copy.path,
            field_name="tax_code",
            overlay_dataset_path=dataset.TAX_CODE_AREA_CERTIFIED.path(CURRENT_TAX_YEAR),
            overlay_field_name="taxcode",
            log_level=None,
        )
        g_addr_tax_code = arcetl.attributes.as_iters(
            address_copy.path, field_names=["geofeat_id", "tax_code"]
        )
        for addr_id, tax_code in g_addr_tax_code:
            yield (addr_id, tax_code)


def taxlot_accounts_map():
    """Return mapping of maptaxlot to set of accounts."""
    g_account_taxlot = rlid_accounts(
        include_attributes=["maptaxlot"], exclude_inactive=True, exclude_personal=True
    )
    taxlot_accounts = defaultdict(set)
    for account, maptaxlot in g_account_taxlot:
        if account and maptaxlot:
            taxlot_accounts[maptaxlot].add(str(account))
    return taxlot_accounts


def taxlot_code_accounts_map():
    """Return mapping of maptaxlot & tax code to set of accounts."""
    g_account_taxlot_code = rlid_tax_year_accounts(
        include_attributes=["maptaxlot", "tca"]
    )
    taxlot_code_accounts = defaultdict(set)
    for account, maptaxlot, code in g_account_taxlot_code:
        taxlot_code = (maptaxlot, code[:3] + "-" + code[3:] if code else None)
        taxlot_code_accounts[taxlot_code].add(str(account))
    return taxlot_code_accounts


# ETLs & updates.


def assess_tax_info_update():
    """Run update for A&T info dataset."""
    dataset_path = {
        "address": dataset.SITE_ADDRESS.path("maint"),
        "info": dataset.ADDRESS_ASSESS_TAX_INFO.path(),
    }
    LOG.info("Clear info table rows with defunct IDs.")
    ids = {}
    for _dataset, _path in dataset_path.items():
        ids[_dataset] = {
            addr_id
            for addr_id in arcetl.attributes.as_iters(_path, field_names=["geofeat_id"])
        }
    arcetl.features.delete_by_id(
        dataset.ADDRESS_ASSESS_TAX_INFO.path(),
        delete_ids=(ids["info"] - ids["address"]),
        id_field_names=["geofeat_id"],
        use_edit_session=True,
    )
    # Update attributes in info table & addresses.
    # Order matters: attribute order, dataset update order.
    update_kwargs = [
        {
            "update_features": address_tax_codes,
            "field_names": ["geofeat_id", "tax_code_overlay"],
            "datasets": ["info"],
        },
        {
            "update_features": address_maptaxlots,
            "field_names": ["geofeat_id", "maptaxlot"],
            "datasets": ["info", "address"],
        },
        {
            "update_features": address_accounts,
            "field_names": ["geofeat_id", "account"],
            "datasets": ["info", "address"],
        },
    ]
    for kwargs in update_kwargs:
        for key in kwargs["datasets"]:
            LOG.info("Update %s in %s.", kwargs["field_names"][1], dataset_path[key])
            arcetl.features.update_from_iters(
                dataset_path[key],
                id_field_names=["geofeat_id"],
                delete_missing_features=False,
                **kwargs
            )


# Jobs.


NIGHTLY_JOB = Job("Address_Assess_Tax_Info_Nightly", etls=[assess_tax_info_update])


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
