"""Execution code for {##TODO} processing."""
import argparse
import logging
import sys 

 

#sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data')
#sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data\\ArcETL')
#sys.path.insert(0, 'C:\\ColumbiaCarto\\work\\TillamookPy\\Tillamook_data\\ETLAssist')
#sys.path.insert(0, 'C:\ColumbiaCarto\work\TillamookPy\Tillamook_data\helper')

sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA')
sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA\\Library')
sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA\\Library\\ArcETL')
sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA\\Library\\ETLAssist')
sys.path.insert(0, 'D:\\Tillamook_911\\Code\\TillamookQA\\Library\\helper')


import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper.communicate import ##TOD
from helper import credential
from helper import database
from helper import dataset
from helper import document
from helper.misc import ##TODO
from helper.model import ##TODO
from helper import path
from helper import transform
from helper import update
from helper import url
from helper.value import ##TODO


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

##TODO: Place any global variables specific to these jobs/ETLs here.


# Helpers.


##TODO: Place any helper functions specific to these jobs/ETLs here.


# ETLs & updates.


def template_etl():
    """Run ETL for {##TODO}."""
    ##TODO: Execute with UNC connection?
    # conn = credential.UNCPathCredential(path.XX_SHARE, **credential.XX_SHARE)
    # with conn, arcetl.ArcETL("##TODO: ETL Name") as etl:
    with arcetl.ArcETL("##TODO: ETL Name") as etl:
        ##TODO: Add extract keyword arguments (if necessary).
        etl.extract("##TODO: dataset_path")
        ##TODO: Add transform keyword arguments (if necessary).
        etl.transform("##TODO: transformation (e.g. arcetl.features.dissolve)")
        ##TODO: Add load keyword arguments (if necessary).
        etl.load("##TODO: dataset_path")


def template_chained_etl():
    """Run ETL for {##TODO}."""
    ##TODO: Execute with UNC connection?
    # conn = credential.UNCPathCredential(path.XX_SHARE, **credential.XX_SHARE)
    # with conn, arcetl.ArcETL("##TODO: ETL Name") as etl:
    with arcetl.ArcETL("##TODO: ETL Name") as etl:
        ##TODO: Add extract keyword arguments (if necessary).
        (
            etl.extract("##TODO: dataset_path")
            ##TODO: Add transform keyword arguments (if necessary).
            .transform("##TODO: transformation (e.g. arcetl.features.dissolve)")
            ##TODO: Add load keyword arguments (if necessary).
            .load("##TODO: dataset_path")
        )


def template_update():
    """Run update for {##TODO}."""
    LOG.info("Start: {##TODO}.")
    ##TODO: Execute with UNC connection?
    # conn = credential.UNCPathCredential(path.XX_SHARE, **credential.XX_SHARE)
    # with conn, arcetl.ArcETL("##TODO: Update Name") as etl:
    with arcetl.ArcETL("##TODO: Update Name") as etl:
        ##TODO: Add extract keyword arguments (if necessary).
        etl.extract("##TODO: dataset_path")
        ##TODO: Add transform keyword arguments (if necessary).
        etl.transform("##TODO: transformation (e.g. arcetl.features.dissolve)")
        ##TODO: Add load keyword arguments (if necessary).
        etl.update("##TODO: dataset_path", "##TODO: id_field_names")


# Jobs.


# Match name to ETL-Job metadata table (case-insensitive). etls must be an iterable.
TEMPLATE_JOB = Job("Job_Name", etls=[template_etl])


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
