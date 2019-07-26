"""Execution code for taxlot cross-reference processing."""
import argparse
from collections import defaultdict
from copy import copy
import logging
import os

import pyodbc

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import credential
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
from helper import url
from helper.value import clean_whitespace, datetime_from_string


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

PATH = {
    "flood_hazard_area": os.path.join(
        path.REGIONAL_DATA, "natural\\flood\\Flood.gdb", "FloodHazardArea"
    ),
    "flood_zone_code": os.path.join(
        path.REGIONAL_DATA, "natural\\flood\\Flood.gdb", "FloodZoneCode"
    ),
    # "petition": os.path.join(path.REGIONAL_DATA, "parcel\\eug\\pollpet", "lots.shp"),
    "petition": os.path.join(
        path.REGIONAL_DATA, "parcel\\eug\\EugParcel.gdb\\Poll_Petition_FD", "Lots_FC"
    ),
    "petition_database": os.path.join(
        path.EUGENE_DATABASE_SHARE, "pollpet", "Pollpet_data.mdb"
    ),
    # "plat": os.path.join(path.REGIONAL_DATA, "parcel\\eug", "Platbndy.shp"),
    "plat": os.path.join(
        path.REGIONAL_DATA, "parcel\\eug\\EugParcel.gdb\\Plats_FD", "PlatBndy_FC"
    ),
    "plat_database": os.path.join(
        path.EUGENE_DATABASE_SHARE, "PLAT", "Plat_prg_2015.accdb"
    ),
    "soil": os.path.join(path.REGIONAL_DATA, "natural\\soils\\Soils.gdb", "Soil"),
    "soil_component": os.path.join(
        path.REGIONAL_DATA, "natural\\soils\\Soils.gdb", "Component"
    ),
    "soil_agriculture_ratings": os.path.join(
        path.REGIONAL_DATA, "natural\\soils\\Soils.gdb", "LaneSoilRatingsForAgriculture"
    ),
    "soil_map_unit_aggregate": os.path.join(
        path.REGIONAL_DATA, "natural\\soils\\Soils.gdb", "MUAggAtt"
    ),
}
"""dict: Map of reference key to data path."""


# Helpers.


def soil_key_components_map():
    """Return mapping of soil key to tuple of ordered components.

    Returns:
        dict
    """
    mukey_components = defaultdict(set)
    mukey_component_names = arcetl.attributes.as_iters(
        dataset_path=PATH["soil_component"], field_names=["mukey", "compname"]
    )
    for mukey, component in mukey_component_names:
        mukey_components[mukey].add(component)
    return {mukey: sorted(components) for mukey, components in mukey_components.items()}


def petition_documents_map():
    """Return mapping of petition ID to list of document attribute dictionaries.

    The Access database has infuriatingly-named objects, so we need to munge that
    rather than just joining values from the database table directly.

    Returns:
        dict
    """
    sql = """
        select
            ltrim(rtrim([id_num])) as petition_id,
            replace(
                replace(ucase(ltrim(rtrim([tiffname]))), '.PDF', ''), '.TIF', ''
            ) as document_name,
            ltrim(rtrim([image type])) as document_type
        from [image path]
        where [id_num] is not null and [tiffname] is not null
        group by [id_num], [tiffname], [image type];
    """
    odbc_conn = pyodbc.connect(database.access_odbc_string(PATH["petition_database"]))
    unc_conn = credential.UNCPathCredential(
        path.EUGENE_DATABASE_SHARE, **credential.CEDOM100
    )
    with odbc_conn, unc_conn:
        documents = defaultdict(list)
        for petition_id, document_name, document_type in odbc_conn.execute(sql):
            documents[petition_id].append(
                {"document_name": document_name, "document_type": document_type}
            )
    return documents


def plat_documents_map():
    """Return mapping of plat document ID to list of document attribute dictionaries.

    The Access database has infuriatingly-named objects, so we need to munge that rather
    than just joining values from the database table directly.

    Returns:
        dict
    """
    sql = """
        select
            ltrim(rtrim([plat number])) as document_number,
            ltrim(rtrim([subdivision name])) as plat_name,
            replace(
                replace(ucase(ltrim(rtrim([image name]))), '.PDF', ''), '.TIF', ''
            ) as document_name,
            ucase(ltrim(rtrim([Description]))) as document_type
        from [image path]
        where
            [plat number] is not null
            and [plat number] <> 0
            and [image name] is not null
        group by
            [subdivision name], [plat number], [image name], [description];
    """
    odbc_conn = pyodbc.connect(database.access_odbc_string(PATH["plat_database"]))
    unc_conn = credential.UNCPathCredential(
        path.EUGENE_DATABASE_SHARE, **credential.CEDOM100
    )
    with odbc_conn, unc_conn:
        documents = defaultdict(list)
        for (
            document_number,
            plat_name,
            document_name,
            document_type,
        ) in odbc_conn.execute(sql):
            documents[int(document_number)].append(
                {
                    "plat_name": plat_name,
                    "document_name": document_name,
                    "document_type": document_type,
                }
            )
    return documents


# ETLs.


def taxlot_flood_hazard_etl():
    """Run ETL for taxlot/flood hazard cross-reference."""
    keys = {"taxlot": ["maptaxlot", "maptaxlot_hyphen", "map", "taxlot"]}
    with arcetl.ArcETL("Taxlot Flood Hazard") as etl:
        etl.init_schema(dataset.TAXLOT_FLOOD_HAZARD.path())
        # To avoid memory/topoengine errors when processing, run ETL on subsets.
        subsets = taxlot_subset_temp_copies(REAL_LOT_SQL, field_names=keys["taxlot"])
        for subset in subsets:
            with subset:
                arcetl.dataset.add_field(
                    subset.path, field_name="flood_area_id", field_type="text"
                )
                arcetl.geoset.identity(
                    dataset_path=subset.path,
                    field_name="flood_area_id",
                    identity_dataset_path=PATH["flood_hazard_area"],
                    identity_field_name="fld_ar_id",
                )
                # Remove features without overlay.
                arcetl.features.delete(
                    dataset_path=subset.path, dataset_where_sql="flood_area_id is null"
                )
                # Dissolve on lot & overlay, for proper area representation.
                arcetl.features.dissolve(
                    dataset_path=subset.path,
                    dissolve_field_names=keys["taxlot"] + ["flood_area_id"],
                    tolerance=TOLERANCE["xy"],
                )
                etl.transform(
                    arcetl.features.insert_from_path,
                    insert_dataset_path=subset.path,
                    field_names=keys["taxlot"] + ["flood_area_id"],
                )
        # Assign joinable attributes.
        join_kwargs = [
            {
                "field_name": "flood_zone_code",
                "join_dataset_path": PATH["flood_hazard_area"],
                "join_field_name": "fld_zone",
                "on_field_pairs": [("flood_area_id", "fld_ar_id")],
            },
            {
                "field_name": "flood_zone_subtype",
                "join_dataset_path": PATH["flood_hazard_area"],
                "join_field_name": "zone_subty",
                "on_field_pairs": [("flood_area_id", "fld_ar_id")],
            },
            {
                "field_name": "old_flood_zone_code",
                "join_dataset_path": PATH["flood_zone_code"],
                "join_field_name": "old_flood_zone_code",
                "on_field_pairs": [
                    ("flood_zone_code", "flood_zone_code"),
                    ("flood_zone_subtype", "flood_zone_subtype"),
                ],
            },
            {
                "field_name": "flood_zone_description",
                "join_dataset_path": PATH["flood_zone_code"],
                "join_field_name": "flood_zone_description",
                "on_field_pairs": [
                    ("flood_zone_code", "flood_zone_code"),
                    ("flood_zone_subtype", "flood_zone_subtype"),
                ],
            },
        ]
        transform.update_attributes_by_joined_values(etl, join_kwargs)
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="approx_acres",
            function=(lambda x: x / 43560.0),
            arg_field_names=["shape@area"],
            field_as_first_arg=False,
        )
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
        # KEEP minimal overlays.
        etl.load(dataset.TAXLOT_FLOOD_HAZARD.path())


##TODO: Constrain subsets to just Eugene area?
def taxlot_petition_document_etl():
    """Run ETL for taxlot/petition document cross-reference."""
    keys = {"taxlot": ["maptaxlot", "maptaxlot_hyphen", "map", "taxlot"]}
    with arcetl.ArcETL("Taxlot Petition Documents") as etl:
        etl.init_schema(dataset.TAXLOT_PETITION_DOCUMENT.path())
        # To avoid memory/topoengine errors when processing, run ETL on subsets.
        subsets = taxlot_subset_temp_copies(REAL_LOT_SQL, field_names=keys["taxlot"])
        petition_documents = petition_documents_map()
        for subset in subsets:
            with subset:
                arcetl.dataset.add_field(
                    subset.path, field_name="petition_id", field_type="text"
                )
                arcetl.geoset.overlay(
                    dataset_path=subset.path,
                    field_name="petition_id",
                    overlay_dataset_path=PATH["petition"],
                    overlay_field_name="ID_NUM",
                    overlay_central_coincident=True,
                )
                arcetl.attributes.update_by_function(
                    dataset_path=subset.path,
                    field_name="petition_id",
                    function=clean_whitespace,
                )
                # Remove features without overlay.
                arcetl.features.delete(
                    dataset_path=subset.path, dataset_where_sql="petition_id is null"
                )
                petition_document_rows = []
                for petition in arcetl.attributes.as_dicts(
                    dataset_path=subset.path,
                    field_names=keys["taxlot"] + ["petition_id"],
                ):
                    petition.update({"document_name": None, "document_type": None})
                    # If petition has no documents, add a document-less row.
                    if petition["petition_id"] not in petition_documents:
                        petition_document_rows.append(petition)
                        continue

                    for document in petition_documents[petition["petition_id"]]:
                        row = copy(petition)
                        row.update(document)
                        petition_document_rows.append(row)
                if petition_document_rows:
                    etl.transform(
                        arcetl.features.insert_from_dicts,
                        insert_features=petition_document_rows,
                        field_names=petition_document_rows[0].keys(),
                    )
        # Set petition jurisdiction (only Eugene petitions at the moment).
        etl.transform(
            arcetl.attributes.update_by_value,
            field_name="petition_jurisdiction_code",
            value="EUG",
        )
        # Add temp field for convertable string values from petition lots.
        etl.transform(
            arcetl.dataset.add_field,
            field_name="petition_date_string",
            field_type="text",
            field_length=32,
        )
        # Assign joinable attributes.
        join_kwargs = [
            {"field_name": "petition_number", "join_field_name": "PETNUM"},
            {"field_name": "petition_type_code", "join_field_name": "PET_TYPE"},
            {"field_name": "petition_date_string", "join_field_name": "DATE"},
            {"field_name": "is_active", "join_field_name": "ACTIVE"},
            {"field_name": "alley_petition", "join_field_name": "ALY"},
            {"field_name": "bikepath_petition", "join_field_name": "BP"},
            {"field_name": "paving_petition", "join_field_name": "PAV"},
            {"field_name": "pedway_petition", "join_field_name": "PED"},
            {"field_name": "rehab_petition", "join_field_name": "RHB"},
            {"field_name": "sanitary_petition", "join_field_name": "SAN"},
            {"field_name": "sidewalk_petition", "join_field_name": "CW"},
            {"field_name": "storm_petition", "join_field_name": "STM"},
            {"field_name": "streetlight_petition", "join_field_name": "SL"},
        ]
        for kwargs in join_kwargs:
            etl.transform(
                arcetl.attributes.update_by_joined_value,
                join_dataset_path=PATH["petition"],
                on_field_pairs=[("petition_id", "ID_NUM")],
                **kwargs
            )
        petition_fields = [
            "alley_petition",
            "bikepath_petition",
            "paving_petition",
            "pedway_petition",
            "rehab_petition",
            "sanitary_petition",
            "sidewalk_petition",
            "storm_petition",
            "streetlight_petition",
        ]
        # Clean added values from sources of unknown maintenance.
        transform.clean_whitespace(
            etl,
            field_names=petition_fields
            + [
                "petition_number",
                "petition_type_code",
                "petition_date_string",
                "is_active",
            ],
        )
        # RLID uses Y/N flags, convert these Yes/No ones.
        for field_name in petition_fields + ["is_active"]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=field_name,
                function=(lambda x: "Y" if x and x.upper() in ["Y", "YES"] else "N"),
            )
        # Update petition_date from the string value version.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="petition_date",
            function=datetime_from_string,
            field_as_first_arg=False,
            arg_field_names=["petition_date_string"],
        )
        etl.transform(arcetl.dataset.delete_field, field_name="petition_date_string")
        # Add values derived from other values.
        petition_type = {
            "A": "Prepaid Assessment",
            "I": "Irrevocable Petition",
            "P": "Poll",
            "S": "Survey",
            "V": "Voluntary Petition",
            "X": "Adjacent to Unimproved Street or Alley",
        }
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="petition_type_description",
            function=petition_type.get,
            field_as_first_arg=False,
            arg_field_names=["petition_type_code"],
        )
        # Build URL values.
        for field_name, ext in [
            ("rlid_document_url", ".pdf"),
            ("rlid_image_url", ".tif"),
        ]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=field_name,
                function=(url.RLID_IMAGE_SHARE + "/petitions/" + "{}" + ext).format,
                field_as_first_arg=False,
                arg_field_names=["document_name"],
                dataset_where_sql="document_name is not null",
            )
        etl.load(dataset.TAXLOT_PETITION_DOCUMENT.path())


##TODO: Constrain subsets to just Eugene area?
def taxlot_plat_document_etl():
    """Run ETL for taxlot/plat document cross-reference."""
    keys = {"taxlot": ["maptaxlot", "maptaxlot_hyphen", "map", "taxlot"]}
    with arcetl.ArcETL("Taxlot Plat Documents") as etl:
        etl.init_schema(dataset.TAXLOT_PLAT_DOCUMENT.path())
        # To avoid memory/topoengine errors when processing, run ETL on subsets.
        subsets = taxlot_subset_temp_copies(REAL_LOT_SQL, field_names=keys["taxlot"])
        plat_documents = plat_documents_map()
        for subset in subsets:
            with subset:
                arcetl.dataset.add_field(
                    subset.path, field_name="document_number", field_type="long"
                )
                arcetl.geoset.overlay(
                    dataset_path=subset.path,
                    field_name="document_number",
                    overlay_dataset_path=PATH["plat"],
                    overlay_field_name="AGENCYDOCN",
                    overlay_central_coincident=True,
                )
                # Remove features without overlay.
                arcetl.features.delete(
                    dataset_path=subset.path,
                    dataset_where_sql="document_number is null or document_number = 0",
                )
                plat_document_rows = []
                for plat in arcetl.attributes.as_dicts(
                    dataset_path=subset.path,
                    field_names=keys["taxlot"] + ["document_number"],
                ):
                    plat.update(
                        {
                            "plat_name": None,
                            "document_name": None,
                            "document_type": None,
                        }
                    )
                    # If plat has no documents, add a document-less row.
                    if plat["document_number"] not in plat_documents:
                        plat_document_rows.append(plat)
                        continue

                    for document in plat_documents[plat["document_number"]]:
                        row = copy(plat)
                        row.update(document)
                        plat_document_rows.append(row)
                if plat_document_rows:
                    etl.transform(
                        arcetl.features.insert_from_dicts,
                        insert_features=plat_document_rows,
                        field_names=plat_document_rows[0].keys(),
                    )
        # Build URL values.
        for field_name, ext in [
            ("rlid_document_url", ".pdf"),
            ("rlid_image_url", ".tif"),
        ]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name=field_name,
                function=(url.RLID_IMAGE_SHARE + "/plats/" + "{}" + ext).format,
                field_as_first_arg=False,
                arg_field_names=["document_name"],
                dataset_where_sql="document_name is not null",
            )
        etl.load(dataset.TAXLOT_PLAT_DOCUMENT.path())


def taxlot_soil_etl():
    """Run ETL for taxlot soil cross-reference."""
    keys = {"taxlot": ["maptaxlot", "maptaxlot_hyphen", "map", "taxlot"]}
    with arcetl.ArcETL("Taxlot Soil") as etl:
        etl.init_schema(dataset.TAXLOT_SOIL.path())
        # To avoid memory/topoengine errors when processing, run ETL on subsets.
        subsets = taxlot_subset_temp_copies(REAL_LOT_SQL, field_names=keys["taxlot"])
        for subset in subsets:
            with subset:
                arcetl.dataset.add_field(
                    subset.path, field_name="mukey", field_type="text"
                )
                arcetl.geoset.identity(
                    dataset_path=subset.path,
                    field_name="mukey",
                    identity_dataset_path=PATH["soil"],
                    identity_field_name="mukey",
                )
                # Remove features without overlay.
                arcetl.features.delete(
                    dataset_path=subset.path, dataset_where_sql="mukey is null"
                )
                # Dissolve on lot & overlay, for proper area representation.
                arcetl.features.dissolve(
                    dataset_path=subset.path,
                    dissolve_field_names=keys["taxlot"] + ["mukey"],
                    tolerance=TOLERANCE["xy"],
                )
                etl.transform(
                    arcetl.features.insert_from_path,
                    insert_dataset_path=subset.path,
                    field_names=keys["taxlot"] + ["mukey"],
                )
        key_components = soil_key_components_map()
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="all_components",
            function=(
                lambda mukey, components=key_components: ",".join(components[mukey])
            ),
            field_as_first_arg=False,
            arg_field_names=["mukey"],
        )
        for i in [1, 2, 3]:
            etl.transform(
                arcetl.attributes.update_by_function,
                field_name="compname{}".format(i),
                function=(
                    lambda mukey, components=key_components, i=i: components[mukey][
                        i - 1
                    ]
                    if len(components[mukey]) >= i
                    else None
                ),
                field_as_first_arg=False,
                arg_field_names=["mukey"],
            )
        # Assign joinable attributes.
        join_kwargs = [
            {
                "field_name": "musym",
                "join_dataset_path": PATH["soil"],
                "join_field_name": "musym",
                "on_field_pairs": [("mukey", "mukey")],
            },
            {
                "field_name": "muname",
                "join_dataset_path": PATH["soil_map_unit_aggregate"],
                "join_field_name": "muname",
                "on_field_pairs": [("mukey", "mukey")],
            },
            {
                "field_name": "land_capability_class",
                "join_dataset_path": PATH["soil_map_unit_aggregate"],
                "join_field_name": "niccdcd",
                "on_field_pairs": [("mukey", "mukey")],
            },
            {
                "field_name": "hydric_presence",
                "join_dataset_path": PATH["soil_map_unit_aggregate"],
                "join_field_name": "hydclprs",
                "on_field_pairs": [("mukey", "mukey")],
            },
            {
                "field_name": "farm_high_value",
                "join_dataset_path": PATH["soil_agriculture_ratings"],
                "join_field_name": "farm_high_value",
                "on_field_pairs": [("musym", "musym")],
            },
            {
                "field_name": "farm_high_value_if_drained",
                "join_dataset_path": PATH["soil_agriculture_ratings"],
                "join_field_name": "farm_high_value_if_drained",
                "on_field_pairs": [("musym", "musym")],
            },
            {
                "field_name": "farm_high_value_if_protected",
                "join_dataset_path": PATH["soil_agriculture_ratings"],
                "join_field_name": "farm_high_value_if_protected",
                "on_field_pairs": [("musym", "musym")],
            },
            {
                "field_name": "farm_potential_high_value",
                "join_dataset_path": PATH["soil_agriculture_ratings"],
                "join_field_name": "farm_potential_high_value",
                "on_field_pairs": [("musym", "musym")],
            },
        ]
        join_kwargs += [
            {
                "field_name": "nirrcapcl" + str(i),
                "join_dataset_path": PATH["soil_component"],
                "join_field_name": "nirrcapcl",
                "on_field_pairs": [
                    ("mukey", "mukey"),
                    ("compname{}".format(i), "compname"),
                ],
            }
            for i in [1, 2, 3]
        ]
        transform.update_attributes_by_joined_values(etl, join_kwargs)
        # Assign the legacy hydric flag using hydric_presence.
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="hydric",
            function=(lambda x: "Y" if x and int(x) >= 50 else "N"),
            field_as_first_arg=False,
            arg_field_names=["hydric_presence"],
        )
        # Convert Lane soil rating nulls to "N".
        transform.force_yn(
            etl,
            field_names=[
                "farm_high_value",
                "farm_high_value_if_drained",
                "farm_high_value_if_protected",
                "farm_potential_high_value",
            ],
            default="N",
        )
        etl.transform(
            arcetl.attributes.update_by_function,
            field_name="approx_acres",
            function=(lambda x: x / 43560.0),
            field_as_first_arg=False,
            arg_field_names=["shape@area"],
        )
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
        # KEEP minimal overlays.
        etl.load(dataset.TAXLOT_SOIL.path())


# Jobs.


FLOOD_HAZARD_JOB = Job("Taxlot_Flood_Hazard_Dataset", etls=[taxlot_flood_hazard_etl])


PETITION_JOB = Job(
    "Taxlot_Petition_Document_Dataset", etls=[taxlot_petition_document_etl]
)


PLAT_JOB = Job("Taxlot_Plat_Document_Dataset", etls=[taxlot_plat_document_etl])


SOIL_JOB = Job("Taxlot_Soil_Dataset", etls=[taxlot_soil_etl])


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
