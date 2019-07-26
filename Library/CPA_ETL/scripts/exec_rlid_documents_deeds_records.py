"""Execution code for RLID deeds & records document processing."""
from __future__ import unicode_literals
import argparse
from collections import Counter
import csv
import datetime
import logging
import os
import shutil
import stat

import sqlalchemy as sql

from etlassist.pipeline import Job, execute_pipeline

from helper import credential
from helper import database
from helper import document
from helper.misc import elapsed
from helper.model import RLIDDeedRecDocImage
from helper import path


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""
LOGFILE_FORMATTER = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
)

PATH = {
    "drop": os.path.join(path.REGIONAL_STAGING, "DeedsRecords"),
    "repository": os.path.join(path.RLID_DATA_SHARE, "DeedsRecords"),
    "staging": os.path.join(path.RLID_DATA_STAGING_SHARE, "DeedsRecords"),
}


# Helpers.


def convert_image(image_path, delete_original=False):
    """Convert image to PDF."""
    convert_path = "{}.pdf".format(os.path.splitext(image_path)[0])
    try:
        converted = document.convert_image_to_pdf(image_path, convert_path)
    except IOError:
        converted = False
    LOG.log(
        (logging.INFO if converted else logging.WARNING),
        ("%r converted to %r." if converted else "%r failed to convert to %r."),
        os.path.basename(image_path),
        os.path.basename(convert_path),
    )
    if converted and delete_original:
        os.remove(image_path)
    return "converted" if converted else "failed to convert"


def extract_records(archive_path, archive_original=False):
    """Extract deeds & records archives."""
    extracted = path.extract_archive(archive_path, PATH["staging"])
    LOG.log(
        (logging.INFO if extracted else logging.WARNING),
        ("%r extracted." if extracted else "%r failed to extract."),
        os.path.basename(archive_path),
    )
    if archive_original:
        move_path = os.path.join(
            PATH["staging"],
            ("Extracted_Archives" if extracted else "Invalid_Archives"),
            os.path.basename(archive_path),
        )
        path.create_directory(
            os.path.dirname(move_path), exist_ok=True, create_parents=True
        )
        shutil.move(archive_path, move_path)
        LOG.log(
            (logging.INFO if extracted else logging.WARNING),
            ("%r archived at %r." if extracted else "%r failed to archive."),
            os.path.basename(move_path),
        )
    return "extracted" if extracted else "failed to extract"


def place_record(document_path, delete_original=False):
    """Place deed or record document in RLID repository."""
    doc_id, ext = os.path.splitext(os.path.basename(document_path).replace("-", "_"))
    place_path = rlid_record_path(doc_id, ext)
    if place_path:
        # Create bin (if necessary).
        path.create_directory(
            os.path.dirname(place_path), exist_ok=True, create_parents=True
        )
        try:
            shutil.copy2(document_path, place_path)
        except IOError:
            placed = False
        else:
            os.chmod(place_path, stat.S_IWRITE)
            placed = True
    else:
        place_path = "{unknown path}"
        placed = False
    LOG.log(
        (logging.INFO if placed else logging.WARNING),
        ("%r placed at %r." if placed else "%r failed to place at %r."),
        os.path.basename(document_path),
        place_path,
    )
    if placed and delete_original:
        os.remove(document_path)
    return "placed" if placed else "failed to place"


def place_record_old(document_path):
    """Place deed or record document in RLID repository (old-style)."""
    doc_id, ext = os.path.splitext(os.path.basename(document_path).replace("-", "_"))
    place_path = rlid_record_path_old(doc_id, ext)
    if place_path:
        # Create bin (if necessary).
        path.create_directory(
            os.path.dirname(place_path), exist_ok=True, create_parents=True
        )
        try:
            shutil.copy2(document_path, place_path)
        except IOError:
            placed = False
        else:
            os.chmod(place_path, stat.S_IWRITE)
            placed = True
    else:
        place_path = "{unknown path}"
        placed = False
    LOG.log(
        (logging.INFO if placed else logging.WARNING),
        ("%r placed at %r." if placed else "%r failed to place at %r."),
        os.path.basename(document_path),
        place_path,
    )
    return "placed" if placed else "failed to place"


def rlid_record_path(doc_id, extension):
    """Return the RLID file path for a deed or record."""
    try:
        doc_year, doc_num = [int(i) for i in doc_id.split("_")]
    except ValueError:
        record_path = None
    else:
        # D&R repository, year-bin, 1000s-bin, file name.
        record_path = os.path.join(
            PATH["repository"],
            str(doc_year),
            str(doc_num // 1000 * 1000),
            (doc_id + extension),
        )
    return record_path


def rlid_record_path_old(doc_id, extension):
    """Return the old-style RLID file path for a deed or record."""
    try:
        doc_year, doc_num = (int(i) for i in doc_id.split("_"))
    except ValueError:
        image_file_num = None
    else:
        session = database.RLID_DEEDREC.create_session()
        try:
            query = session.query(RLIDDeedRecDocImage.image_file_name).filter(
                RLIDDeedRecDocImage.doc_year == doc_year,
                RLIDDeedRecDocImage.doc_num == doc_num,
            )
            # If no result, will return None & index will trigger TypeError.
            try:
                image_file_num = query.first()[0]
            except TypeError:
                image_file_num = None
        finally:
            session.close()
    if image_file_num:
        # D&R repository, 1000s-bin, file name.
        record_path = os.path.join(
            PATH["repository"],
            str(image_file_num // 1000 * 1000),
            str(image_file_num) + extension,
        )
    else:
        record_path = None
    return record_path


def rlid_record_paths():
    """Generate deed & record document file paths."""
    session = database.RLID_DEEDREC.create_session()
    try:
        query = (
            session.query(RLIDDeedRecDocImage.doc_year, RLIDDeedRecDocImage.doc_num)
            .group_by(RLIDDeedRecDocImage.doc_year, RLIDDeedRecDocImage.doc_num)
            .order_by(
                sql.desc(RLIDDeedRecDocImage.doc_year), RLIDDeedRecDocImage.doc_num
            )
        )
        for doc_year, doc_num in query:
            doc_id = "{}_{:06}".format(doc_year, doc_num)
            yield rlid_record_path(doc_id, extension=".pdf")

    finally:
        session.close()


# ETLs.


def deeds_records_update():
    """Run update for deeds & records documents RLID repository."""
    start_time = datetime.datetime.now()
    PATH["logfile"] = os.path.join(
        PATH["staging"], "Deeds_Records_Update_{}.log".format(start_time.year)
    )
    conn = credential.UNCPathCredential(PATH["staging"], **credential.RLID_DATA_SHARE)
    with conn:
        # Attach logfile handler for staging logfile.
        logfile = logging.FileHandler(PATH["logfile"])
        logfile.setLevel(logging.INFO)
        logfile.setFormatter(LOGFILE_FORMATTER)
        LOG.addHandler(logfile)
        LOG.info("START SCRIPT: Update RLID deeds & records repository.")
        LOG.info("Start: Move deeds & records drop-files to staging directory.")
        drop_extensions = [".exe", ".pdf", ".zip"] + document.IMAGE_FILE_EXTENSIONS
        for file_name in os.listdir(PATH["drop"]):
            file_path = os.path.join(PATH["drop"], file_name)
            file_extension = os.path.splitext(file_name)[-1].lower()
            if all([os.path.isfile(file_path), file_extension in drop_extensions]):
                move_path = os.path.join(PATH["staging"], file_name)
                shutil.move(file_path, move_path)
                LOG.info("Moved %r to %r.", file_path, move_path)
        LOG.info("End: Move.")
        LOG.info("Start: Extract record archives.")
        count = Counter()
        for file_path in path.folder_file_paths(PATH["staging"]):
            if os.path.splitext(file_path)[-1].lower() in [".exe", ".zip"]:
                count[extract_records(file_path, archive_original=True)] += 1
        document.log_state_counts(count, documents_type="archives")
        # D&R archives include a few log & reference files; delete if present.
        for file_path in path.folder_file_paths(PATH["staging"]):
            for pattern in ["_logfile", "_xreffile"]:
                if pattern.lower() in file_path.lower():
                    os.remove(file_path)
        LOG.info("Start: Replace record images with PDFs.")
        count = Counter()
        for file_path in path.folder_file_paths(PATH["staging"]):
            if (
                os.path.splitext(file_path)[-1].lower()
                in document.IMAGE_FILE_EXTENSIONS
            ):
                count[convert_image(file_path, delete_original=True)] += 1
        document.log_state_counts(count, documents_type="images")
        LOG.info("Start: Place record PDFs in RLID repository.")
        count = Counter()
        for file_path in path.folder_file_paths(PATH["staging"]):
            if os.path.splitext(file_path)[-1].lower() == ".pdf":
                old_state = place_record_old(file_path)
                new_state = place_record(
                    file_path, delete_original=(old_state == "placed")
                )
                count.update([old_state, new_state])
        document.log_state_counts(count, documents_type="records")
    elapsed(start_time, LOG)
    LOG.info("END SCRIPT")


def mirror_old_style_records_etl():
    """Ensure the old-style records exist for all present new-style ones."""
    start_time = datetime.datetime.now()
    LOG.info("Start: Mirror record documents in old-style naming.")
    conn = credential.UNCPathCredential(
        path.RLID_DATA_SHARE, **credential.RLID_DATA_SHARE
    )
    count = Counter()
    with conn:
        for doc_path in rlid_record_paths():
            doc_name = os.path.basename(doc_path)
            doc_id, ext = os.path.splitext(doc_name)
            old_style_path = rlid_record_path_old(doc_id, ext)
            if not old_style_path:
                count["not in database"] += 1
            elif os.path.exists(old_style_path):
                count["already mirrored"] += 1
            elif place_record_old(doc_path):
                count["mirrored"] += 1
            else:
                count["failed to mirror"] += 1
                LOG.warning("%r failed to mirror to %r.", doc_name, old_style_path)
    document.log_state_counts(count, documents_type="records")
    LOG.info("End: Mirror.")
    elapsed(start_time, LOG)


##TODO: Monthly email?
def missing_in_rlid_etl():
    """Run ETL for log of deeds & records documents missing in RLID."""
    start_time = datetime.datetime.now()
    LOG.info(
        "Start: Compile table of deeds & records listed in Lane County records system,"
        + " but not present in RLID repository."
    )
    conn = credential.UNCPathCredential(PATH["staging"], **credential.RLID_DATA_SHARE)
    csv_path = os.path.join(PATH["staging"], "Missing_in_RLID.csv")
    check_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    missing_count = 0
    with conn:
        csvfile = open(csv_path, "wb")
        with csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["document_id", "document_path", "check_time"])
            for doc_path in rlid_record_paths():
                if not os.path.exists(doc_path):
                    doc_id = os.path.splitext(os.path.basename(doc_path))[0]
                    csvwriter.writerow((doc_id, doc_path, check_time))
                    missing_count += 1
    LOG.info("Found %s missing documents.", missing_count)
    LOG.info("End: Compile.")
    elapsed(start_time, LOG)


# Jobs.


HOURLY_JOB = Job("RLID_Documents_Deeds_Records_Hourly", etls=[deeds_records_update])


WEEKLY_JOB = Job("RLID_Documents_Deeds_Records_Weekly", etls=[missing_in_rlid_etl])


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
