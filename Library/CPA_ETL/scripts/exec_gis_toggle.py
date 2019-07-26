"""Execution code for GIS toggle to RLID."""
import argparse
import logging

import pyodbc

from etlassist.pipeline import Job, execute_pipeline

from helper import database


##TODO: Migrate `rlid_gis_toggle` to `exec_rlid_gis_load.py`.
##TODO: Delete this exec.

LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# ETLs.


def rlid_gis_toggle():
    """Run toggle for GIS-derived data in RLID.

    `proc_toggle_GIS` seems to not run quickly (perhaps completely) without using the
    cursor to execute it (i.e. conn.execute instead of cursor.execute. This might be
    because of internal transactions within the procedure.
    """
    LOG.info("Start: Toggle GIS to RLID warehouse.")
    sql_strings = [
        "exec dbo.proc_push_changes_to_EWEB;",
        "exec dbo.proc_toggle_GIS @as_return_msg=null, @ai_return_code=null;",
    ]
    with pyodbc.connect(database.RLID.odbc_string) as conn:
        for sql in sql_strings:
            LOG.info("Start: Execute %s.", sql)
            with conn.cursor() as cursor:
                cursor.execute(sql)
            LOG.info("End: Execute.")
    LOG.info("End: Toggle.")


# Jobs.


RLID_GIS_JOB = Job("RLID_GIS_Toggle", etls=[rlid_gis_toggle])


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
