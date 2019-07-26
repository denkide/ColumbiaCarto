"""Execution code for RLID GIS loading."""
import argparse
import logging
import subprocess

from etlassist.pipeline import Job, execute_pipeline


##TODO: Rename exec_rlid_gis.py.
##TODO: Migrate `rlid_gis_toggle` in `exec_gis_toggle.py` to here.


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# ETLs.


def load_rlid_gis():
    """Execute stored procedure to load GIS to the offline RLID warehouse.

    Using command-line tool to avoid issues with procedure processes suspending in
    `arcetl.workspace.execute_sql()` & `pyodbc.connect().execute()`.

    SQLCMD does not like check_call putting arguments in other list items.
    Use a single-string instead.
    SQLCMD options:
        -S {name}: server instance name.
        -E: trusted connection.
        -d {name}: database name.
        -b: terminate batch job if errors.
        -Q "{string}": query string.
    """
    call_string = " ".join(
        [
            "sqlcmd.exe -S {} -E -d RLID -b -Q",
            '"exec dbo.proc_load_GIS @as_return_msg = null, @ai_return_code = null;"',
        ]
    )
    subprocess.check_call(call_string.format("gisql113"))


# Jobs.


WEEKLY_JOB = Job("RLID_GIS_Load_Weekly", etls=[load_rlid_gis])


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
