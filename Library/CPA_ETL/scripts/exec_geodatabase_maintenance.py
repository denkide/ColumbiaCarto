"""Execution code for geodatabase maintenance."""
import argparse
import logging
import os
import subprocess

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper.misc import IGNORE_PATTERNS_DATA_BACKUP, randomized, timestamp
from helper import path


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

IGNORE_DATASETS_DATA_BACKUP = [
    # These hang the backup.
    "LCOGGeo.dbo.LU_Site_Address_QA",
    "Regional.lcadm.Anno0100scale",
]
##TODO: Move this to wiki or other well-regarded document.
GISRV106_SQL_SERVER_MAINTENANCE_INFO = """
    Internal maintenance for the SQL Server instance on GISRV106 is currently
    based on the SQL Server Maintenance Solution devised by Ola Hallengren,
    found at https://ola.hallengren.com/.
    It is recommended that the server/database admins sign up for the Ola Hallengren
    newsletter, so they can update the solution when changes are made.

    Maintenance jobs:

    * Jobs email database admins when fails.
    * Cleanup jobs/job parts clear items older than 30 days.
    * Index maintenance should be followed by full backup (keeps diffs small).
    * Integrity check right before full backup (know backup is okay).

    * Sunday 12:01 AM
        + DatabaseIntegrityCheck - SYSTEM_DATABASES
        + DatabaseIntegrityCheck - USER_DATABASES
        + IndexOptimize - USER_DATABASES
    * Sunday 1:01 AM
        + DatabaseBackup - SYSTEM_DATABASES - FULL
        + DatabaseBackup - USER_DATABASES - FULL
    * Weekday 1:01 AM
        + DatabaseBackup - SYSTEM_DATABASES - FULL
        + DatabaseBackup - USER_DATABASES - DIFF
    * Saturday 12:01 AM
        + CommandLog Cleanup
        + Output File Cleanup
        + sp_delete_backuphistory
        + sp_purge_jobhistory
    * Saturday 1:01 AM
        + DatabaseBackup - SYSTEM_DATABASES - FULL
        + DatabaseBackup - USER_DATABASES - DIFF
    * Unscheduled.
        + DatabaseBackup - USER_DATABASES - LOG
    """
"""str: Backup/maintenance procedures in place for GISRV106 database instance."""


# ETLs.


def geodatabase_backup_build_etl():
    """Run ETL for geodatabase build SQL backup."""
    LOG.info("Starting backup of geodatabase SQL build scripts.")
    for gdb in randomized(database.GISRV106_DATABASES):
        if not gdb.back_up_build_sql:
            continue

        # Ensure backup directory is present.
        backup_path = os.path.join(path.SDE_DATA_BACKUP_SHARE, gdb.name, "Build_SQL")
        path.create_directory(backup_path, exist_ok=True, create_parents=True)
        sql_path = os.path.join(backup_path, "{}_{}.sql".format(gdb.name, timestamp()))
        LOG.info("Start: Generate build SQL for %s", gdb.name)
        subprocess.check_call(
            "powershell.exe -ExecutionPolicy Bypass"
            " -File {} -instance {} -database {} -output {}".format(
                path.GENERATE_BUILD_SQL, "gisrv106", gdb.name, sql_path
            )
        )
        LOG.info("Generated at %s", sql_path)
        LOG.info("End: Generate")
    LOG.info("Geodatabase SQL build scripts backup complete.")


def geodatabase_backup_datasets_etl():
    """Run ETL for geodatabase datasets backup."""
    LOG.info("Starting backup of geodatabase datasets.")
    ignore_dataset_names = [name.lower() for name in IGNORE_DATASETS_DATA_BACKUP]
    for gdb in randomized(database.GISRV106_DATABASES):
        if not gdb.back_up_gdb_data:
            continue

        # Ensure backup directory is present.
        backup_path = os.path.join(path.SDE_DATA_BACKUP_SHARE, gdb.name, "Data")
        path.create_directory(backup_path, exist_ok=True, create_parents=True)
        gdb_path = os.path.join(backup_path, "{}_{}.gdb".format(gdb.name, timestamp()))
        arcetl.workspace.create_file_geodatabase(gdb_path)
        for name in arcetl.workspace.dataset_names(gdb.path):
            # Specific database/datasets to not back up. These cause problems, and
            # generally are not important/can be restored from SQL backup.
            if name.lower() in ignore_dataset_names:
                LOG.warning("%s listed in ignore-datasets: skipping", name)
                continue

            # Certain patterns indicate datasets that don't need to be backed up (e.g.
            # views of other data).
            if any(
                pattern.lower() in name.lower()
                for pattern in IGNORE_PATTERNS_DATA_BACKUP
            ):
                LOG.warning("%s matches ignore-pattern: skipping.", name)
                continue

            source_path = os.path.join(gdb.path, name)
            copy_path = os.path.join(gdb_path, name.split(".")[-1])
            arcetl.dataset.copy(source_path, copy_path)
        arcetl.workspace.compress(gdb_path)
    LOG.info("Geodatabase datasets backup complete.")


def geodatabase_backup_schema_etl():
    """Run ETL for geodatabase schema backup."""
    LOG.info("Starting backup of geodatabase schema.")
    for gdb in randomized(database.GISRV106_DATABASES):
        if not gdb.back_up_gdb_schema:
            continue

        # Ensure backup directory is present.
        backup_path = os.path.join(path.SDE_DATA_BACKUP_SHARE, gdb.name, "Schema")
        path.create_directory(backup_path, exist_ok=True, create_parents=True)
        xml_path = os.path.join(backup_path, "{}_{}.xml".format(gdb.name, timestamp()))
        arcetl.workspace.create_geodatabase_xml_backup(
            geodatabase_path=gdb.path,
            output_path=xml_path,
            include_data=False,
            include_metadata=True,
        )
    LOG.info("Geodatabase schema backup complete.")


def geodatabase_compress_etl():
    """Run ETL for geodatabase compress."""
    LOG.info("Starting compression of geodatabases.")
    for geodatabase in randomized(database.GISRV106_DATABASES):
        if not geodatabase.compress:
            continue

        arcetl.workspace.compress(geodatabase.path)
    LOG.info("Geodatabases compression complete.")


# Jobs.


NIGHTLY_JOB = Job(
    "Geodatabase_Maintenance_Nightly",
    etls=[
        geodatabase_compress_etl,
        geodatabase_backup_schema_etl,
        geodatabase_backup_datasets_etl,
        geodatabase_backup_build_etl,
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
