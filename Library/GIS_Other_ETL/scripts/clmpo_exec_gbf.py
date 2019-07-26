"""Execution code for General Bikeshare Feed-related operations."""
import argparse
import datetime
import logging
import os
import sqlite3

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import dataset
from helper import path


##TODO: Eventually add the every-five-minute gbfsnap call to here as an ETL & job.

LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

DATASETS = [
    dataset.GBF_FREE_BIKE_STATUS,
    dataset.GBF_GBFS,
    dataset.GBF_STATION_INFORMATION,
    dataset.GBF_STATION_STATUS,
    dataset.GBF_SYSTEM_ALERTS,
    dataset.GBF_SYSTEM_CALENDAR,
    dataset.GBF_SYSTEM_HOURS,
    dataset.GBF_SYSTEM_INFORMATION,
    dataset.GBF_SYSTEM_PRICING_PLANS,
    dataset.GBF_SYSTEM_REGIONS,
]
"""list: Collection of datasets."""
SNAPSHOT_DB_PATH = os.path.join(
    path.REGIONAL_FILE_SHARE,
     "staging\\Bikeshare_Feed\\PeaceHealth_Rides_Feed_{}.sqlite3",
)


# Helpers.


def source_rows(gbf_database_path, dataset_name):
    """Generate row-dictionaries from GBF dataset source."""
    conn = sqlite3.connect(gbf_database_path)
    cursor = conn.cursor()
    sql = "select * from {}".format(dataset_name)
    cursor.execute(sql)
    column_names = [column[0] for column in cursor.description]
    for row in cursor:
        yield dict(zip(column_names, row))


# ETLs.


def gbf_pub_update():
    """Run update for GBF publication datasets."""
    LOG.info("Start: Update datasets in RLIDGeo warehouse.")
    month_stamps = [
        datetime.date.today().strftime("%Y_%m"),
        (
            datetime.date.today().replace(day=1)
            - datetime.timedelta(days=1)
        ).strftime("%Y_%m"),
    ]
    for month_stamp in month_stamps:
        snapshot_db_path = SNAPSHOT_DB_PATH.format(month_stamp)
        if not os.path.exists(snapshot_db_path):
            LOG.warning("Snapshot database %s does not exist.", snapshot_db_path)
            continue

        for _dataset in DATASETS:
            arcetl.features.update_from_dicts(
                dataset_path=_dataset.path("pub"),
                update_features=source_rows(snapshot_db_path, _dataset.path("source")),
                id_field_names=_dataset.id_field_names,
                field_names=_dataset.field_names,
                delete_missing_features=False,
                use_edit_session=False,
            )
    LOG.info("End: Update.")


# Jobs.


NIGHTLY_JOB = Job("CLMPO_GBF_Nightly", etls=[gbf_pub_update])


# Execution.


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument("pipelines", nargs="*", help="Pipeline(s) to run")
    # Collect pipeline objects.
    if args.parse_args().pipelines:
        pipelines = [globals()[arg] for arg in args.parse_args().pipelines]
    else:
        pipelines = []
    # Execute.
    for pipeline in pipelines:
        execute_pipeline(pipeline)


if __name__ == "__main__":
    main()
