# -*- coding: utf-8 -*-
"""Execution code for metadata processing."""
from __future__ import unicode_literals
import argparse
import copy
import csv
import datetime
import io
import logging
import os
import re
import shutil
from collections import defaultdict
from tempfile import NamedTemporaryFile, gettempdir
import time
import xml.etree.ElementTree as ET

import pyodbc

import arcpy
from etlassist.pipeline import Job, execute_pipeline

from helper.communicate import send_email
from helper import database
from helper import path


##TODO:
"""
* Routine that checks & emails invalid titles & paths in CSVs.
* Is this really CPA_ETL, or is this GIS_Other_ETL?
* Fix the weirdness in the email report.

"""

LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

CHAR_ENTITY = {
    "'": {"description": "Apostrophe (single-quote)", "name": "&apos;"},
    # `&lsquo;` is undefined entity in XML 1.0.
    "‘": {"description": "Left single-quote", "name": "&apos;"},
    # `&rsquo;` is undefined entity in XML 1.0.
    "’": {"description": "Right single-quote", "name": "&apos;"},
    # `&ldquo;` is undefined entity in XML 1.0.
    "“": {"description": "Left double-quote", "name": "&quot;"},
    # `&rdquo;` is undefined entity in XML 1.0.
    "”": {"description": "Right double-quote", "name": "&quot;"},
}
"""dict: Mapping of characters to HTML character entities."""
KWARGS_PROBLEMS_REPORT_MESSAGE = {
    "subject": "Eugene Geoportal Metadata Record Syncing Problems",
    "recipients": ["mike.v.miller@ci.eugene.or.us"],
    "copy_recipients": ["jblair@lcog.org"],
    "reply_to": "jblair@lcog.org",
    "body_format": "HTML",
}
"""dict: Mapping of keyword arguments to values for report send function."""
PATH = {
    "metadata_translator": os.path.join(
        arcpy.GetInstallInfo()["InstallDir"], "Metadata\\Translator\\ARCGIS2FGDC.xml"
    ),
    "staging": "\\\\gisrv100.ris5.net\\regional\\staging\\Metadata",
}
"""dict: Mapping of tag to path."""
PATH["parent_info"] = os.path.join(PATH["staging"], "Eug_Parent_Metadata_Info.csv")
PATH["xslt"] = os.path.join(PATH["staging"], "OrderCodedValuesAndFields.xslt")
XML_COMMON_MISMATCHED_TAGS = ["srccite"]
"""list: Collections of XML tags commonly-mismatched (no opening or closing tag)."""
XML_DECLARATION_PATTERN_ERRORS = {
    '<?xml version="1.0"': {"<?xml version='1.0';", "<?xml version=&apos;1.0&apos;"},
    'encoding="UTF-8"?>': {
        "encoding='utf-8'?>",
        "encoding='UTF-8'?>",
        "encoding=&apos;utf-8&apos;?>",
        "encoding=&apos;UTF-8&apos;?>",
    },
}
"""dict: Mapping of XML declaration patterns to set of common error-versions."""


# Helpers.


class Geoportal(object):
    """Interface for a given Geoportal.

    Attributes:
        database (etlassist.database.Database): Object instance for Geoportal's
            database.
        base_url (str): Geoportal base URL.

    """

    def __init__(self, database_instance, base_url):
        """Initialize instance.

        Args:
            database_instance (etlassist.database.Database): Object instance for
                Geoportal's database.
            base_url (str): Geoportal base URL.

        """
        self.database = database_instance
        self.base_url = base_url

    def __gt__(self, other):
        return self.database.name > other.database.name

    def __lt__(self, other):
        return self.database.name < other.database.name

    def __repr__(self):
        return "{}(database.name={!r})".format(
            self.__class__.__name__, self.database.name
        )

    def records(self, descending_date=True):
        """Generate metadata records.

        Args:
            descending_date (bool): True if metadata is generated in descending-date
                order, False otherwise.

        Yields:
            Metadata: Instance of a metadata record.

        """
        sql = """
            select
                doc_uuid = resource.docuuid,
                id = resource.id,
                title = resource.title,
                input_date = resource.inputdate,
                update_date = resource.updatedate,
                approval_status = resource.approvalstatus,
                xml = data.xml
            from {database}.dbo.GPT_Resource as resource
                left join {database}.dbo.GPT_User as usr
                    on resource.owner = usr.userid
                left join {database}.dbo.GPT_Resource_Data as data
                    on resource.docuuid = data.docuuid
            order by
                resource.updatedate {order}, resource.inputdate {order}, resource.title;
        """.format(
            database=self.database.name, order="desc" if descending_date else "asc"
        )
        for row in self.sql_query(sql, result_type=dict):
            yield MetadataRecord(geoportal=self, record=row)

    def record_by_title(self, title):
        """Return first approved metadata record that matches the title.

        Args:
            title (str): Title of the metadata record.

        Returns:
            MetadataRecord: Object instance for metadata record.

        """
        sql = """
            select top 1
                doc_uuid = resource.docuuid,
                id = resource.id,
                title = resource.title,
                input_date = resource.inputdate,
                update_date = resource.updatedate,
                approval_status = resource.approvalstatus,
                xml = data.xml
            from {database}.dbo.GPT_Resource as resource
                left join {database}.dbo.GPT_User as usr
                    on resource.owner = usr.userid
                left join {database}.dbo.GPT_Resource_Data as data
                    on resource.docuuid = data.docuuid
            where
                resource.title = '{title}'
                and resource.approvalstatus = 'approved'
            order by resource.updatedate desc, resource.inputdate desc;
        """.format(
            database=self.database.name, title=title.replace("'", "''")
        )
        try:
            row = next(self.sql_query(sql, result_type=dict))
        except StopIteration:
            record = None
        else:
            record = MetadataRecord(geoportal=self, record=row)
        return record

    def sql_exec(self, statement, retry_count=4, retry_wait=32):
        """Execute SQL statement in geoportal database.

        Args:
            statement (str): SQL statement to execute.
            retry_count (int): Number of times to retry on connection failure.
            retry_wait (int): Seconds to wait before next try.

        """
        conn = pyodbc.connect(
            self.database.odbc_string
            + "App=CPA_ETL.exec_metatadata.Geoportal.exec_sql;"
        )
        cursor = conn.cursor()
        try:
            cursor.execute(statement)
        except pyodbc.OperationalError:
            # [08S01] Communication link failure.
            if retry_count <= 0:
                raise

            LOG.warning("Connection failure: trying again in %s seconds.", retry_wait)
            conn.close()
            time.sleep(retry_wait)
            self.sql_exec(statement, (retry_count - 1), retry_wait)
        else:
            conn.close()

    def sql_query(self, statement, result_type=tuple, retry_count=4, retry_wait=32):
        """Generate SQL query results from geoportal database.

        Args:
            statement (str): SQL statement to query.
            result_type: Type of container result should be yielded as.
            retry_count (int): Number of times to retry on connection failure.
            retry_wait (int): Seconds to wait before next try.

        Yields:
            Representation of query result row in the chosen result_type.

        """
        conn = pyodbc.connect(
            self.database.odbc_string
            + "App=CPA_ETL.exec_metatadata.Geoportal.exec_sql;"
        )
        with conn:
            cursor = conn.cursor()
            try:
                cursor.execute(statement)
            except pyodbc.OperationalError as error:
                # [08S01] Communication link failure.
                if retry_count <= 0:
                    raise

                LOG.warning(
                    "%s - trying again in %s seconds.", error.message, retry_wait
                )
                time.sleep(retry_wait)
                for result in self.sql_query(
                    statement, result_type, (retry_count - 1), retry_wait  # pylint: disable=bad-continuation
                ):
                    yield result

            else:
                column_names = [column[0] for column in cursor.description]
                try:
                    for row in cursor:
                        if result_type == dict:
                            result = dict(zip(column_names, row))
                        else:
                            result = result_type(row)
                        yield result

                except pyodbc.Error as error:
                    # [HY000] Protocol error in TDS stream.
                    if retry_count <= 0:
                        raise

                    LOG.warning(
                        "%s - trying again in %s seconds.", error.message, retry_wait
                    )
                    time.sleep(retry_wait)
                    for result in self.sql_query(
                        statement, result_type, (retry_count - 1), retry_wait  # pylint: disable=bad-continuation
                    ):
                        yield result


class MetadataRecord(object):
    """Representation of a metadata record.

    Attributes:
        geoportal (Geoportal): Object instance for metadata record's origin geoportal.

    """

    def __init__(self, geoportal, record):
        """Instance initialization.


        Args:
            geoportal (Geoportal): Object instance for metadata record's geoportal.
            record (dict): Mapping of record attribute names to values.

        """
        self.geoportal = geoportal
        self._record = record

    def __gt__(self, other):
        return self.title > other.title

    def __lt__(self, other):
        return self.title < other.title

    def __repr__(self):
        return ", ".join(
            [
                "{}(title={!r}".format(self.__class__.__name__, self.title),
                "id={!r}".format(self.id),
                "approval_status={!r}".format(self.approval_status),
                "dataset_path={!r})".format(self.dataset_path),
            ]
        )

    @property
    def approval_status(self):
        """str: Approval status for the record."""
        return self._record["approval_status"]

    @approval_status.setter
    def approval_status(self, value):
        if value != self.approval_status:
            sql = (
                "update {database}.dbo.GPT_Resource"
                + " set approvalstatus = '{status}' where id = {id};"
            )
            with pyodbc.connect(self.geoportal.database.odbc_string) as conn:
                conn.execute(
                    sql.format(
                        database=self.geoportal.database.name, status=value, id=self.id
                    )
                )
            self._record["approval_status"] = value

    @property
    def dataset_path(self):
        """str: Path to dataset the record describes."""
        return re.search(r"<othercit>([\s\S]*?)</othercit>", self.xml).group(1)

    @dataset_path.setter
    def dataset_path(self, value):
        if value != self.dataset_path:
            self.xml = re.sub(
                pattern="<othercit>(.*)</othercit>",
                repl="<othercit>{}</othercit>".format(value),
                string=self.xml,
                count=1,
                flags=re.DOTALL,
            )

    @property
    def doc_uuid(self):
        """str: Record unique identifier as UUID."""
        return self._record["doc_uuid"]

    @property
    def id(self):  # pylint: disable=invalid-name
        """int: Record unique identifier."""
        return self._record["id"]

    @property
    def input_date(self):
        """datetime.datetime: Record input date."""
        return self._record["input_date"]

    @property
    def title(self):
        """str: Record title."""
        return self._record["title"]

    @property
    def update_date(self):
        """datetime.datetime: Record update date."""
        return self._record["update_date"]

    @property
    def url(self):
        """str: URL for record in geoportal web app."""
        return (
            self.geoportal.base_url
            + "/catalog/search/resource/details.page?uuid="
            + self.doc_uuid.replace("{", "%7B").replace("}", "%7D")
        )

    @property
    def xml(self):
        """str: Record metadata XML."""
        return self._record["xml"]

    @xml.setter
    def xml(self, value):
        if value == self.xml:
            return

        sql = """
            update {database}.dbo.GPT_Resource_Data
            set xml = '{xml}'
            where id = {_id};
        """.format(
            database=self.geoportal.database.name,
            _id=self.id,
            xml=value.replace("'", "''")
        )
        self.geoportal.sql_exec(sql)
        self._record["xml"] = value


def adjust_record_xml(record):
    """Make adjustments to record's XML.

    Args:
        record (MetadataRecord): Metadata record to alter.

    Returns:
        str: Message describing any problems that arose. NoneType if no problems.

    """
    message = None
    out_xml = unfuck_xml_declaration(record.xml)
    # Skipping for now. Not sure this is a problem anymore.
    # out_xml = clear_mismatched_tags(out_xml)
    out_xml = apply_character_entities(out_xml)
    if message is None:
        record.xml = out_xml
    try:
        out_xml = fix_secinfo(out_xml)
    except ET.ParseError as error:
        message = "Bad XML: {}".format(error.message)
        LOG.warning("%s.", message)
    if message is None:
        try:
            record.xml = out_xml
        except pyodbc.Error:
            xml_path = os.path.join(gettempdir(), "metadata_pyodbc_error.xml")
            with io.open(xml_path, mode="w", encoding="utf-8-sig") as xmlfile:
                xmlfile.write(out_xml)
            LOG.error("PyODBC error: XML file at %s.", xml_path)
            raise

    return message


def apply_character_entities(xml):
    """Return metadata XML with reserved characters replaced by their entities.

    Args:
        xml (str): XML to alter (if necessary).

    Returns:
        str: XML with changes (if any).

    """
    out_xml = copy.copy(xml)
    for char, entity in CHAR_ENTITY.items():
        if char in out_xml:
            out_xml = out_xml.replace(char, entity["name"])
    return out_xml


def assign_record_parent(record, parent_info, parent_geoportal):
    """Assign record a parent via the dataset path.

    Args:
        record (MetadataRecord): Metadata record to adjust.
        parent_info (dict): Mapping of record parent's details.
        parent_geoportal (Geoportal): Geoportal instance parent record resides in.

    Returns:
        str: Message describing any problems that arose. NoneType if no problems.

    """
    message = None
    parent = parent_info[record.title]
    parent["record"] = parent_geoportal.record_by_title(parent["parent_geo_title"])
    if not parent["record"]:
        message = "Parent record title does not exist in geoportal."
        LOG.warning("%s.", message)
    if message is None:
        record.dataset_path = parent["record_note"] + ": " + parent["record"].url
        LOG.info("Has parent record: Dataset path set to link message.")
    return message


def clear_mismatched_tags(xml):
    """Return metadata XML with mismatched tags removed.

    Args:
        xml (str): XML to alter (if necessary).

    Returns:
        str: XML with changes (if any).

    """
    out_xml = copy.copy(xml)
    for tag in XML_COMMON_MISMATCHED_TAGS:
        tag_open = "<{}>".format(tag)
        tag_close = "</{}>".format(tag)
        if tag_open in out_xml and tag_close not in out_xml:
            out_xml = out_xml.replace(tag_open, "")
        elif tag_close in out_xml and tag_open not in out_xml:
            out_xml = out_xml.replace(tag_close, "")
    return out_xml


def fix_secinfo(xml):
    """Return metadata XML with corrected secinfo element.

    Args:
        xml (str): XML to alter (if necessary).

    Returns:
        str: XML with changes (if any).

    """
    if "<secinfo>" not in xml:
        return xml

    tree = ET.ElementTree(ET.fromstring(xml.encode("utf-8")))
    secinfo = tree.getroot().find("idinfo").find("secinfo")
    secsys = secinfo.find("secsys")
    secclass = secinfo.find("secclass")
    if secclass is None:
        return xml

    secsys.text = " - ".join([secsys.text, secclass.text])
    secinfo.remove(secclass)
    with NamedTemporaryFile() as xmlfile:
        tree.write(xmlfile, encoding="utf-8", xml_declaration=True)
        xmlfile.seek(0)
        out_xml = xmlfile.read()
    return out_xml.decode("utf-8")


def parent_tag_record_info_map(parent_csv_path):
    """Mapping of parent info tags to the parent record information.

    Args:
        parent_csv_path (str): Path to CSV file with parent details.

    Returns:
        dict: Mapping of parent tags to dictionaries of parent details.

    """
    with open(parent_csv_path) as csv_file:
        reader = csv.DictReader(csv_file)
        _map = defaultdict(dict)
        for row in reader:
            # Make column keys lowercase, to ensure consistent.
            row = {key.lower(): val.strip() for key, val in row.items()}
            _map[row["parent_tag"]][row["geo_title"]] = row
    return _map


def return_metadata_to_geoportal(record):
    """Return metadata record to geoportal from related dataset.

    Args:
        record (MetadataRecord): Metadata record to sync.

    Returns:
        str: Message describing any problems that arose. NoneType if no problems.

    """
    message = None
    # Create metadata XML file.
    xml_path = staging_xml_path(record)
    path.create_directory(os.path.dirname(xml_path), exist_ok=True, create_parents=True)
    arcpy.conversion.ExportMetadata(
        Source_Metadata=record.dataset_path,
        Output_File=xml_path,
        Translator=PATH["metadata_translator"],
    )
    # Prep XML for posting to Geoportal.
    with io.open(xml_path, mode="r", encoding="utf-8-sig") as xmlfile:
        # Dataset strips XML declaration  & reverts character entities: Fix.
        post_xml = apply_character_entities(
            """<?xml version="1.0" encoding="UTF-8"?>\n""" + xmlfile.read()
        )
    # Update return-file for posterity.
    with io.open(xml_path, mode="w", encoding="utf-8-sig") as xmlfile:
        xmlfile.write(post_xml)
    LOG.info("Created staging-return XML file.")
    # Post to record on Geoportal.
    try:
        record.xml = post_xml
    except pyodbc.OperationalError as error:
        message = "ODBC operational error: {}".format(error.message)
        LOG.warning("%s.", message)
    LOG.info("Metadata record synced back to %s.", record.geoportal.database.name)
    return message


def send_metadata_to_dataset(record):
    """Send metadata record to the related dataset.

    Args:
        record (MetadataRecord): Metadata record to sync.

    Returns:
        str: Message describing any problems that arose. NoneType if no problems.

    """
    message = None
    # Create metadata XML file.
    xml_path = staging_xml_path(record)
    path.create_directory(os.path.dirname(xml_path), exist_ok=True, create_parents=True)
    with NamedTemporaryFile(suffix=".xml", delete=False) as xmlfile:
        try:
            tree = ET.ElementTree(ET.fromstring(record.xml.encode("utf-8")))
        except ET.ParseError as error:
            message = "Bad XML: {}".format(error.message)
            LOG.warning("%s.", message)
        else:
            tree.write(xmlfile, encoding="utf-8", xml_declaration=True)
    if message is None:
        if os.path.exists(xml_path):
            os.remove(xml_path)
        arcpy.conversion.XSLTransform(
            source=xmlfile.name, xslt=PATH["xslt"], output=xml_path
        )
        os.remove(xmlfile.name)
        LOG.info("Created staging-send XML file.")
        # DBF files (likely an old-style image catalog) cannot receive metadata
        # applied via Arc. Just copy an adjacent XML file.
        if os.path.splitext(record.dataset_path)[-1].lower() == ".dbf":
            shutil.copyfile(xml_path, os.path.splitext(record.dataset_path)[0] + ".xml")
            LOG.info("Dataset type not syncable: Placed copy of XML file adjacent.")
        else:
            # Push metadata onto dataset.
            try:
                arcpy.conversion.MetadataImporter(
                    source=xml_path, target=record.dataset_path
                )
            except arcpy.ExecuteError:
                message = (
                    "Failed to write metadata to dataset"
                    + " (likely process user has no write-access)"
                )
                LOG.warning("%s. Dataset path: %s", message, record.dataset_path)
    if message is None:
        try:
            arcpy.conversion.UpgradeMetadata(
                record.dataset_path, Upgrade_Type="fgdc_to_arcgis"
            )
        except arcpy.ExecuteError:
            message = "Failed to upgrade metadata on record"
            LOG.warning("%s.", message)
    return message


def send_problems_report(problem_message_records):
    """Send report of problems that affect metadata syncing.

    Args:
        problem_message_records (dict): Mapping of provlem messages to collection of
            records where that problem occurred during sync.

    """
    body_template = """
        <p>Problems were found that prevent certain metadata records from syncing
            between Geoportal and the datasets.<br />
        </p>
        <table style="width:100%">
            {}{}
        </table>
    """
    table_header = (
        "<tr><th>Count</th><th>ID</th><th>Title</th>"
        + "<th>Problem</th><th>Dataset Path</th></tr>"
    )
    row_template = "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>"
    if problem_message_records:
        LOG.warning("Found syncing problem records: sending report.")
        table_rows = []
        i = 0
        for message, records in sorted(problem_message_records.items()):
            for record in sorted(records):
                i += 1
                table_rows.append(
                    row_template.format(
                        i, record.id, record.title, message, record.dataset_path
                    )
                )
        KWARGS_PROBLEMS_REPORT_MESSAGE["body"] = body_template.format(
            table_header, table_rows
        )
        send_email(**KWARGS_PROBLEMS_REPORT_MESSAGE)
    else:
        LOG.info("No syncing problem records found. Not sending report.")


def staging_xml_path(record):
    """Return XML path for staging purposes.

    Args:
        record (MetadataRecord): Metadata record to create staging path for.

    Returns:
        str: Path for an XML file.

    """
    xml_basename = path.flattened(record.dataset_path)
    xml_name = xml_basename + datetime.datetime.now().strftime("_%Y_%m_%d_%H%M-%S.xml")
    xml_path = os.path.join(PATH["staging"], record.geoportal.database.name, xml_name)
    return xml_path


def unfuck_xml_declaration(xml):
    """Return metadata XML with corrected declaration.

    Args:
        xml (str): XML to alter (if necessary).

    Returns:
        str: XML with changes (if any).

    """
    out_xml = copy.copy(xml)
    for good_pattern, bad_patterns in XML_DECLARATION_PATTERN_ERRORS.items():
        for bad_pattern in bad_patterns:
            if bad_pattern in out_xml:
                out_xml = out_xml.replace(bad_pattern, good_pattern)
    return out_xml


# ETLs & updates.


def eug_metadata_sync():
    """Run sync for Eugene Geoportal & dataset-local metadata."""
    geoportal = {
        "cpa": Geoportal(
            database.GEOPORTAL_RLID, "http://open.maps.rlid.org:8080/geoportal_RLID"
        ),
        "eug": Geoportal(
            database.GEOPORTAL_EUGENE, "http://open.maps.rlid.org:8080/geoportal_CE"
        ),
    }
    parent_info = parent_tag_record_info_map(PATH["parent_info"])
    datasets_synced = set()
    problem_message_records = defaultdict(list)
    LOG.info("Syncing metadata records from %s.", geoportal["eug"].database.name)
    # Clear old staging XML files.
    os.chdir(os.path.join(PATH["staging"], geoportal["eug"].database.name))
    for file_name in os.listdir(os.getcwd()):
        if os.path.isfile(file_name) and file_name.lower().endswith(".xml"):
            os.remove(file_name)
    # Must run generator through, or hanging session will block write sessions.
    records = list(geoportal["eug"].records(descending_date=True))
    for i, record in enumerate(records, start=1):
        LOG.info("\n\nRecord %s:", i)
        LOG.info("Metadata record: %s (id=%s).", record.title, record.id)
        message = adjust_record_xml(record)
        if message:
            problem_message_records[message].append(record)
            continue

        # Not-approved records are ignored.
        if record.approval_status in {None, "disapproved", "draft", "incomplete"}:
            LOG.info(
                """Approval status is "%s": Skipping sync.""", record.approval_status
            )
            continue

        if not record.dataset_path:
            message = "Missing dataset path"
            problem_message_records[message].append(record)
            LOG.warning("%s.", message)
            continue

        # Must happen before checking dataset existance.
        if any(
            [record.title in parent_info["cpa"], record.title in parent_info["eug"]]  # pylint: disable=bad-continuation
        ):
            message = None
            if record.title in parent_info["cpa"]:
                message = assign_record_parent(
                    record, parent_info["cpa"], geoportal["cpa"]
                )
            elif record.title in parent_info["eug"]:
                message = assign_record_parent(
                    record, parent_info["eug"], geoportal["eug"]
                )
            if message:
                problem_message_records[message].append(record)
            continue

        if arcpy.Exists(record.dataset_path) is False:
            # Metadata for web services don't need to sync.
            if record.dataset_path.split("//")[0] in ["http:", "https:"]:
                LOG.info("Dataset path is web URL. Skipping sync.")
                continue

            # record.approval_status = "incomplete"
            # message = "Invalid dataset path: Metadata status set to incomplete"
            message = "Invalid dataset path"
            problem_message_records[message].append(record)
            LOG.warning("%s.", message)
            continue

        if record.dataset_path.lower() in datasets_synced:
            # record.approval_status = "incomplete"
            # message = (
            #     "Duplicate path of newer record: Metadata status set to incomplete"
            # )
            message = "Duplicate path of newer record"
            problem_message_records[message].append(record)
            LOG.warning("%s.", message)
            continue

        message = send_metadata_to_dataset(record)
        if message:
            problem_message_records[message].append(record)
            continue

        message = return_metadata_to_geoportal(record)
        if message:
            problem_message_records[message].append(record)
            LOG.warning("%s.", message)
            continue

        else:
            datasets_synced.add(record.dataset_path.lower())
    if any(records for records in problem_message_records.values()):
        send_problems_report(problem_message_records)


# Jobs.


WEEKLY_JOB = Job("Eugene_Metadata_Weekly", etls=[eug_metadata_sync])


# Execution.


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument("pipelines", nargs="*", help="Pipeline(s) to run")
    # Collect pipeline objects.
    pipelines = (
        [globals()[arg] for arg in args.parse_args().pipelines]
        if args.parse_args().pipelines
        else []
    )
    # Execute.
    for pipeline in pipelines:
        execute_pipeline(pipeline)


if __name__ == "__main__":
    main()
