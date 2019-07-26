"""Execution code for production dataset processing."""
import argparse
import logging

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import dataset
from helper.misc import TOLERANCE
from helper import update


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# Helpers.


def city_name_case(value):
    """Return city name converted to title case.

    Args:
        value (str, None): City name value.

    Returns:
        str: title-cased name.
    """
    if value:
        if value.lower() == "mckenzie bridge":
            value = "McKenzie Bridge"
        else:
            value = value.lower()
    return value


# ETLs.


def mailing_city_area_etl():
    """Run ETL for mailing city areas."""
    with arcetl.ArcETL("Mailing City Areas") as etl:
        etl.extract(
            dataset.ZIP_CODE_AREA.path("pub"),
            extract_where_sql="mailcitycode is not null",
        )
        for old, new in {"mailcitycode": "city_code", "mailcity": "city_name"}.items():
            etl.transform(
                arcetl.dataset.rename_field, field_name=old, new_field_name=new
            )
        etl.transform(
            arcetl.features.erase,
            erase_dataset_path=dataset.INCORPORATED_CITY_LIMITS.path(),
            erase_where_sql="inccityabbr = 'COB'",
        )
        etl.transform(
            arcetl.features.insert_from_path,
            insert_dataset_path=dataset.INCORPORATED_CITY_LIMITS.path(),
            insert_where_sql="inccityabbr = 'COB'",
        )
        for attr, value in {"city_code": "COB", "city_name": "Coburg"}.items():
            etl.transform(
                arcetl.attributes.update_by_value,
                field_name=attr,
                value=value,
                dataset_where_sql=attr + " is null",
            )
        etl.transform(
            arcetl.features.dissolve,
            dissolve_field_names=dataset.MAILING_CITY_AREA.field_names,
            tolerance=TOLERANCE["xy"],
        )
        etl.update(
            dataset.MAILING_CITY_AREA.path(),
            id_field_names=dataset.MAILING_CITY_AREA.id_field_names,
            field_names=dataset.MAILING_CITY_AREA.field_names,
            use_edit_session=False,
        )


##TODO: Refactor away from SQL, "make" an update.
def production_update_etl():
    """Run cleaning/assigning update for miscellaneous production datasets."""
    sql_statements = [
        # Building: IDs.
        """
            -- Replace non-unique global IDs that are not the "first-set"
            -- (e.g. lowest object ID).
            update feature
            set geofeature_id = newid()
            from LCOGGeo.lcogadm.Building_evw as feature
            where
                geofeature_id is not null
                and objectid not in (
                    select min(objectid)
                    from LCOGGeo.lcogadm.Building_evw
                    where geofeature_id is not null
                    group by geofeature_id
                );
            -- Set global IDs where missing.
            update feature
            set geofeature_id = newid()
            from LCOGGeo.lcogadm.Building_evw as feature
            where geofeature_id is null;
        """,
        # Street: IDs.
        """
            -- Replace non-unique global IDs that are not the "first-set"
            -- (e.g. lowest object ID).
            update feature
            set road_segment_gfid = newid()
            from LCOGGeo.lcogadm.Street_evw as feature
            where
                road_segment_gfid is not null
                and objectid not in (
                    select min(objectid)
                    from LCOGGeo.lcogadm.Street_evw
                    where road_segment_gfid is not null
                    group by road_segment_gfid
                );
            -- Set global IDs where missing.
            update feature
            set road_segment_gfid = newid()
            from LCOGGeo.lcogadm.Street_evw as feature
            where road_segment_gfid is null;
        """,
        "exec LCOGGeo.dbo.proc_Update_Production_Datasets;",
        ##TODO: Enable.
        "exec Regional.dbo.proc_Update_Production_Datasets;",
        # Site address: Cleaning & assigning.
        "exec Addressing.dbo.proc_Update_SiteAddress_ProdData;",
    ]
    for sql in sql_statements:
        arcetl.workspace.execute_sql(sql, database.ETL_LOAD_A.path)


def proposed_street_name_update():
    """Run update for proposed street names dataset."""
    kwargs = {
        "dataset_path": dataset.PROPOSED_STREET_NAME.path(),
        "use_edit_session": True,
    }
    update.clean_whitespace(
        field_names=[
            "pre_direction_code",
            "street_name",
            "street_type_code",
            "subdivision_name",
            "proposal_type",
            "requestor_name",
            "proposal_status",
            "denied_reason",
            "memo",
        ],
        **kwargs
    )
    update.clean_whitespace_without_clear(
        field_names=["city_name", "jurisdiction_name"], **kwargs
    )
    update.force_uppercase(
        field_names=[
            "pre_direction_code",
            "street_name",
            "street_type_code",
            "display_in_rlid_flag",
        ],
        **kwargs
    )
    arcetl.attributes.update_by_function(
        field_name="city_name", function=city_name_case, **kwargs
    )
    update.force_yn(field_names=["display_in_rlid_flag"], default="Y", **kwargs)


##TODO: Finish & test.
# def street_update():
#     """Run update for streets dataset."""
#     kwargs = {
#         ##TODO: Remove path monkey-patch.
#         "dataset_path": dataset.ROAD.path("maint") + "_TEST",
#         "use_edit_session": True,
#     }
#     update.update_attributes_by_unique_ids(field_names=["road_segment_gfid"], **kwargs)
#     update.clear_nonpositive(
#         field_names=[
#             "seg_id",
#             "lcpwid",
#             "eugid",
#             "sprid",
#             "lcogid",
#             "speed",
#             "speedfrwrd",
#             "speedback",
#             "snowroute",
#             "f_zlev",
#             "t_zlev",
#         ],
#         **kwargs
#     )
#     for key in ["l_ladd", "l_hadd", "r_ladd", "r_hadd"]:
#         arcetl.attributes.update_by_function(
#             field_name=key, function=(lambda x: x if x >= 0 else 0), **kwargs
#         )
#     update.clean_whitespace(
#         field_names=[
#             "sgname",
#             "mailcity",
#             "cityl",
#             "cityr",
#             "county",
#             "fclass",
#             "fed_class",
#             "paved",
#             "owner",
#             "maint",
#             "source",
#             "method",
#             "contributor",
#             "one_way",
#             "flow",
#         ],
#         **kwargs
#     )
#     update.clean_whitespace_without_clear(
#         field_names=["airsname", "dir", "name", "type", "airsclass", "ugbcity"],
#         **kwargs
#     )
#     update.force_uppercase(
#         field_names=[
#             "airsname",
#             "dir",
#             "name",
#             "type",
#             "sgname",
#             "cityl",
#             "cityr",
#             "fclass",
#             "airsclass",
#             "paved",
#             "owner",
#             "maint",
#             "source",
#             "method",
#             "contributor",
#             "ugbcity",
#             "one_way",
#             "flow",
#         ],
#         **kwargs
#     )
#     arcetl.attributes.update_by_function(
#         field_name="mailcity", function=city_name_case, **kwargs
#     )
#     update.force_title_case(field_names=["county"], **kwargs)


# Jobs.


NIGHTLY_JOB = Job(
    "Production_Datasets_Nightly",
    etls=[proposed_street_name_update, mailing_city_area_etl, production_update_etl],
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
