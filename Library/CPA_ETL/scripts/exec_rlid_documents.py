"""Execution code for RLID document processing."""
import argparse
from collections import Counter
import csv
import datetime
import io
from itertools import permutations
import logging
import os
import stat

import PyPDF2
from reportlab.lib import colors
from reportlab.lib import pagesizes
import reportlab.pdfgen.canvas
import sqlalchemy as sql

from etlassist.pipeline import Job, execute_pipeline

from helper import credential
from helper import database
from helper import document
from helper.misc import elapsed, rlid_data_currency, rlid_data_currency_setter
from helper.model import LaneTaxMapReleaseLog, RLIDTaxMapArchive, RLIDTaxMapImage
from helper import path


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""
LOGFILE_FORMATTER = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
)

REPO_PATH = {
    "petition": os.path.join(path.RLID_DATA_SHARE, "Petitions"),
    "plat": os.path.join(path.RLID_DATA_SHARE, "Plats"),
    "property-card": os.path.join(path.RLID_DATA_SHARE, "AssessorPropertyCards"),
    "property-card-staging": os.path.join(
        path.RLID_DATA_STAGING_SHARE, "AssessorPropertyCards"
    ),
    "tax-map": os.path.join(path.RLID_DATA_SHARE, "TaxMap"),
    "tax-map-staging": os.path.join(path.RLID_DATA_STAGING_SHARE, "TaxMap"),
}


# Helpers.


def archive_tax_map(tax_map_path, archive_date, is_replaced):
    """Archive copy of tax map with watermark in RLID repository.

    Props to https://stackoverflow.com/a/17538003 for ideas.

    Args:
        tax_map_path (str): Path for tax map document.
        archive_date (datetime.datetime): Date/time of when archiving occured.
        is_replaced (bool): True if archived map is being replaced, False otherwise.

    Returns:
        str: Result key.
    """
    archive = PyPDF2.PdfFileWriter()
    for i, page in enumerate(PyPDF2.PdfFileReader(tax_map_path).pages, start=1):
        try:
            page.mergePage(archive_watermark_page(archive_date))
        except ValueError:
            LOG.warning("POSSIBLY FAILED TO WATERMARK PAGE %s.", i)
        archive.addPage(page)
    map_root, ext = os.path.splitext(os.path.basename(tax_map_path))
    archive_path = os.path.join(
        REPO_PATH["tax-map"],
        "Archive",
        map_root + archive_date.strftime("_%Y%m%d") + ext,
    )
    if os.path.exists(archive_path):
        os.chmod(archive_path, stat.S_IWRITE)
    with open(archive_path, "wb") as archive_file:
        archive.write(archive_file)
        archived = True
    session = database.RLID.create_session()
    try:
        existing = (
            session.query(RLIDTaxMapArchive.date_archived)
            .filter(
                RLIDTaxMapArchive.taxmap_archive_filename
                == os.path.basename(archive_path)
            )
            .one_or_none()
        )
        if existing:
            row = (
                session.query(RLIDTaxMapArchive)
                .filter(
                    RLIDTaxMapArchive.taxmap_archive_filename
                    == os.path.basename(archive_path)
                )
                .first()
            )
            row.date_archived = archive_date
        else:
            row = RLIDTaxMapArchive(
                taxmap_filename=os.path.basename(tax_map_path),
                taxmap_archive_filename=os.path.basename(archive_path),
                date_archived=archive_date,
                archived_no_replacement="N" if is_replaced else "Y",
            )
            session.add(row)
        session.commit()
    finally:
        session.close()
    result_key = "archived" if archived else "failed to archive"
    LOG.log(
        logging.INFO if archived else logging.WARNING,
        "`%s` %s at `%s`.",
        os.path.basename(tax_map_path),
        result_key,
        archive_path,
    )
    return result_key


def archive_watermark_page(archive_date):
    """Return PDF object with archiving watermark.

    Args:
        archive_date (datetime.datetime): Date of archiving.

    Returns:
        PyPDF2.pdf.PageObject: Page with just the archive watermark.
    """
    size = {"width": 20 * pagesizes.inch, "height": 18 * pagesizes.inch}
    font = {
        "name": "Helvetica",
        "size": 256,
        "color": colors.red.clone(alpha=0.20),
        "start_x": size["width"] * 0.50,
        "start_y": size["height"] * 0.45,
    }
    text_lines = ["ARCHIVED", archive_date.strftime("%Y-%m-%d")]
    packet = io.BytesIO()
    canvas = reportlab.pdfgen.canvas.Canvas(
        filename=packet, pagesize=(size["width"], size["height"]), bottomup=0
    )
    canvas.setFillColor(font["color"])
    canvas.setFont(psfontname=font["name"], size=font["size"])
    for i, line in enumerate(text_lines):
        canvas.drawCentredString(
            x=font["start_x"], y=(font["start_y"] + canvas._leading * i), text=line
        )
    canvas.save()
    packet.seek(0)
    watermark_page = PyPDF2.PdfFileReader(packet).getPage(0)
    return watermark_page


def fixed_file_name(file_name_or_path):
    """Return document file name as RLID standard.

    Args:
        str: File name or path to fix.

    Returns:
        str: File name or path corrected to RLID standard.
    """
    dir_path, file_name = os.path.split(file_name_or_path)
    root, ext = os.path.splitext(file_name)
    return os.path.join(dir_path, root.upper() + ext.lower())


def rlid_document_path(file_name, document_type):
    """Return RLID repository path for the given document.

    Args:
        file_name (str): Name of the file/document.
        document_type (str): Type of document.

    Returns:
        str: Path of document in RLID repository.
    """
    rlid_path = {
        # Most populous card series are divided into four-digit bins.
        # Rest are divided into two-digit bins.
        "property-card": os.path.join(
            REPO_PATH["property-card"],
            file_name[:4] if file_name[:2] in ["17", "18"] else file_name[:2],
            fixed_file_name(file_name),
        ),
        "tax-map": os.path.join(REPO_PATH["tax-map"], fixed_file_name(file_name)),
        # Tax map source repo has a one-deep bin, first four digits.
        "tax-map-staging": os.path.join(
            REPO_PATH["tax-map-staging"], file_name[:4], fixed_file_name(file_name)
        ),
    }
    if document_type not in rlid_path:
        raise NotImplementedError(
            "document type {!r} not implemented.".format(document_type)
        )

    return rlid_path[document_type]


def tax_map_file_name_release_map(start_datetime):
    """Generate mapping of tax map relative path to its most recent release date.

    Args:
        start_datetime (datetime.datetime): Timestamp of when to start release query.

    Returns:
        dict: Mapping of map name to release date/time.
    """
    session = database.TAX_MAP_DISTRIBUTION.create_session()
    try:
        query = (
            session.query(
                LaneTaxMapReleaseLog.map_name,
                sql.func.max(LaneTaxMapReleaseLog.release_datetime),
            )
            .filter(LaneTaxMapReleaseLog.release_datetime > start_datetime)
            .group_by(LaneTaxMapReleaseLog.map_name)
        )
        return dict(query)

    finally:
        session.close()


def update_tax_map(source_path, update_path, release_date, archive_previous):
    """Place tax map document from source into repository."""
    if not os.path.exists(source_path):
        raise IOError("Source file not found {}.".format(source_path))

    if os.path.exists(update_path) and archive_previous:
        archive_tax_map(update_path, release_date, is_replaced=True)
    result_key = document.update_document(source_path, update_path)
    if result_key == "updated":
        session = database.RLID.create_session()
        file_name = os.path.basename(update_path)
        try:
            existing = (
                session.query(RLIDTaxMapImage.date_modified)
                .filter(RLIDTaxMapImage.image_filename == file_name)
                .one_or_none()
            )
            if existing:
                row = (
                    session.query(RLIDTaxMapImage)
                    .filter(RLIDTaxMapImage.image_filename == file_name)
                    .first()
                )
                row.date_modified = release_date
            else:
                row = RLIDTaxMapImage(
                    image_filename=file_name, date_modified=release_date
                )
                session.add(row)
            session.commit()
        finally:
            session.close()
    return result_key


# ETLs.


def petition_documents_update():
    """Run update for RLID petition document repository."""
    root_path = REPO_PATH["petition"]
    source_root_path = os.path.join(path.EUGENE_IMAGE2_SHARE, "PollPet")
    conn = credential.UNCPathCredential(
        path.RLID_DATA_SHARE, **credential.RLID_DATA_SHARE
    )
    source_conn = credential.UNCPathCredential(source_root_path, **credential.CEDOM100)
    with conn, source_conn:
        # Currently only Eugene provides petition documents for RLID.
        document.update_repository(
            root_path,
            source_root_path,
            file_extensions=[".jpg", "jpeg", ".tif", ".tiff"],
            flatten_tree=True,
            create_pdf_copies=True,
        )


def plat_maps_update():
    """Run update for RLID plat map repository."""
    root_path = REPO_PATH["plat"]
    source_root_path = os.path.join(path.EUGENE_IMAGES_SHARE, "PLAT")
    conn = credential.UNCPathCredential(
        path.RLID_DATA_SHARE, **credential.RLID_DATA_SHARE
    )
    source_conn = credential.UNCPathCredential(source_root_path, **credential.CEDOM100)
    with conn, source_conn:
        # Currently only Eugene provides plat maps for RLID.
        document.update_repository(
            root_path,
            source_root_path,
            file_extensions=[".jpg", "jpeg", ".pdf", ".tif", ".tiff"],
            flatten_tree=True,
            create_pdf_copies=True,
        )


def property_cards_staging_update():
    """Run update for RLID assessor property card staging repository."""
    LOG.info("Start: Update assessor property card staging repository.")
    start_time = datetime.datetime.now()
    source_paths = document.repository_file_paths(path.LANE_PROPERTY_CARDS)
    conn = credential.UNCPathCredential(
        path.RLID_DATA_STAGING_SHARE, **credential.RLID_DATA_SHARE
    )
    with conn:
        count = Counter()
        for source_path in source_paths:
            staging_path = os.path.join(
                REPO_PATH["property-card-staging"], os.path.basename(source_path)
            )
            if document.changed(staging_path, source_path):
                result_key = document.update_document(source_path, staging_path)
                count[result_key] += 1
    LOG.info("End: Update.")
    document.log_state_counts(count, documents_type="property cards (staging)")
    elapsed(start_time, LOG)


def property_cards_update():
    """Run update for assessor property card RLID production repository."""
    LOG.info("Start: Update RLID assessor property card repository.")
    start_time = datetime.datetime.now()
    staging_paths = document.repository_file_paths(
        REPO_PATH["property-card-staging"], file_extensions=[".pdf"]
    )
    conn = credential.UNCPathCredential(
        path.RLID_DATA_SHARE, **credential.RLID_DATA_SHARE
    )
    with conn:
        count = Counter()
        for staging_path in staging_paths:
            rlid_path = rlid_document_path(
                os.path.basename(staging_path), document_type="property-card"
            )
            if document.changed(rlid_path, staging_path):
                result_key = document.update_document(staging_path, rlid_path)
                count[result_key] += 1
    LOG.info("End: Update.")
    document.log_state_counts(count, documents_type="property cards")
    elapsed(start_time, LOG)


##TODO: Monthly? Email?
def tax_maps_not_in_source_etl():
    """Run ETL for log of tax map documents in RLID but not source repository.

    We used to have an automatic check & retire for RLID tax maps that were no longer in
    the source repository. This pretty much retired the entire taxmap repository the
    night of 2015-05-07. This was because there appear to be times when the source
    repository is not reachable, and/or reports nothing in the source. For now, we will
    just log potential orphans.

    If you do need to "retire" a tax map no longer in use:
    1. Make an archive copy of the document with this function call:
        ```
        archive_tax_map(
            tax_map_path, archive_date=datetime.datetime.now(), is_replaced=False
        )
        ```
    2. Move the document file to the `RetiredNoReplacement` subfolder.
    3. Execute the following SQL statement:
        ```
        if exists (
            select 1 from RLID.dbo.Taxmap_Retired where image_filename = {file-name}
        ) begin;
            update RLID.dbo.Taxmap_Retired
            set date_retired = {same-date-as-archive-above}
            where image_filename = {file-name};
        end;
        else begin;
            insert into RLID.dbo.Taxmap_Retired(image_filename, date_retired)
            values ({file-name}, {same-date-as-archive-above});
        end;
        delete from RLID.dbo.Taxmap_Image where image_filename = {file-name};`
    """
    start_time = datetime.datetime.now()
    LOG.info(
        "Start: Compile table of tax maps not mirrored between the Lane County & RLID"
        " repositories.\nAny tax maps in RLID not mirrored in the county repositoryare"
        " likely tax maps that no longer exist, and should be researched (and perhaps"
        " retired)."
    )
    conn = credential.UNCPathCredential(
        path.RLID_DATA_SHARE, **credential.RLID_DATA_SHARE
    )
    with conn:
        check_time = start_time.strftime("%Y-%m-%d %H:%M")
        file_names = {
            "County": {
                fixed_file_name(name)
                for _, _, filenames in os.walk(REPO_PATH["tax-map-staging"])
                for name in filenames
                if name.lower().endswith(".pdf")
            },
            "RLID": {
                fixed_file_name(name)
                for name in os.listdir(REPO_PATH["tax-map"])
                if name.lower().endswith(".pdf")
            },
        }
        for repo, other in permutations(["County", "RLID"]):
            LOG.info("Checking %s repository for tax maps not mirrored.", repo)
            unmirrored_file_names = sorted(file_names[repo] - file_names[other])
            csv_path = os.path.join(
                REPO_PATH["tax-map"], "In_{}_Not_{}.csv".format(repo, other)
            )
            csv_file = open(csv_path, "wb")
            with csv_file:
                csv_ = csv.writer(csv_file)
                csv_.writerow(("file_name", "check_time"))
                for file_name in unmirrored_file_names:
                    csv_.writerow((file_name, check_time))
            LOG.info(
                "Found %s tax maps in %s repository not mirrored in %s.",
                len(unmirrored_file_names),
                repo,
                other,
            )
    LOG.info("End: Compile.")
    elapsed(start_time, LOG)


def tax_maps_staging_update():
    """Run update for RLID tax map staging repository."""
    LOG.info("Start: Update tax map staging repository.")
    start_time = datetime.datetime.now()
    conn = credential.UNCPathCredential(
        path.RLID_DATA_STAGING_SHARE, **credential.RLID_DATA_SHARE
    )
    with conn:
        count = Counter()
        for source_path in document.repository_file_paths(path.LANE_TAX_MAP_IMAGES):
            staging_path = os.path.join(
                REPO_PATH["tax-map-staging"],
                # Tax maps have a one-deep bin.
                os.path.split(os.path.dirname(source_path))[-1],
                os.path.basename(source_path),
            )
            if document.changed(staging_path, source_path):
                result_key = document.update_document(source_path, staging_path)
                count[result_key] += 1
    document.log_state_counts(count, documents_type="tax maps (staging)")
    elapsed(start_time, LOG)
    LOG.info("End: Update.")


def tax_maps_update():
    """Run update for RLID tax map repository."""
    start_time = datetime.datetime.now()
    conn = credential.UNCPathCredential(
        path.RLID_DATA_SHARE, **credential.RLID_DATA_SHARE
    )
    with conn:
        # Attach logfile handler for repository update logfile.
        logfile = logging.FileHandler(
            os.path.join(
                REPO_PATH["tax-map"], "Tax_Map_Update_{}.log".format(start_time.year)
            )
        )
        logfile.setLevel(logging.INFO)
        logfile.setFormatter(LOGFILE_FORMATTER)
        LOG.addHandler(logfile)
        LOG.info("START SCRIPT: Update RLID tax map repository from staging.")
        file_name_release_date = tax_map_file_name_release_map(
            start_datetime=rlid_data_currency("Tax Maps")
        )
        count = Counter()
        # Iterate through path/date map, adding, archiving & updating.
        for file_name, release_date in file_name_release_date.items():
            rlid_path = rlid_document_path(file_name, document_type="tax-map")
            staging_path = rlid_document_path(
                file_name, document_type="tax-map-staging"
            )
            result_key = update_tax_map(
                staging_path, rlid_path, release_date, archive_previous=True
            )
            count[result_key] += 1
    document.log_state_counts(count, documents_type="tax maps")
    # Finally, update tax map repository currency date (if we placed any).
    if count["updated"]:
        rlid_data_currency_setter("Tax Maps", max(file_name_release_date.values()))
    elapsed(start_time, LOG)
    LOG.info("END SCRIPT: Update")


# Jobs.


DAILY_JOB = Job("RLID_Documents_Daily", etls=[tax_maps_staging_update, tax_maps_update])


NIGHTLY_JOB = Job(
    "RLID_Documents_Nightly",
    etls=[property_cards_staging_update, property_cards_update],
)


WEEKLY_JOB = Job(
    "RLID_Documents_Weekly",
    etls=[petition_documents_update, plat_maps_update, tax_maps_not_in_source_etl],
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
