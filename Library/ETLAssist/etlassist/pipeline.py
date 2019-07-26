"""Processing pipeline objects."""
import functools
import logging
import operator
import os
import types

import pyodbc

from . import communicate
from . import path


__all__ = (
    'ETL_LOAD_A_ODBC_STRING',
    'STATUS_DESCRIPTION_MAP',
    'Job',
    'batch_job_status_details',
    'batch_metadata',
    'batch_name_id_map',
    'execute_pipeline',
    'init_root_logger',
    'job_run_status',
    'send_batch_notification',
    )
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

##TODO: Replace this with a better reference?
##TODO: Or move batch metadata to sqllite database?
ETL_LOAD_A_ODBC_STRING = ('Driver={SQL Server Native Client 11.0};'
                          'Server=GISRV106;Database=ETL_Load_A;'
                          'Trusted_Connection=yes;')
STATUS_DESCRIPTION_MAP = {0: 'complete', 1: 'failed', -1: 'incomplete'}


##TODO: Subsume batch-related functions into a new Batch/BatchInfo() class.


##TODO: Status property+setter.
class Job(object):
    """Representation of a batch job for pipeline processing."""
    def __init__(self, name, etls=None):
        self.name = name
        self._etls = []
        self.etls = etls

    ##TODO: Consider renaming etls --> procedures?
    @property
    def etls(self):
        """Collection of ETLs attached to the job."""
        return self._etls

    @etls.setter
    def etls(self, value):
        """ETLs property setter."""
        if value is None:
            self._etls = []
        else:
            self._etls = list(value)


##TODO: Subsume pipeline-related functions into a new Pipeline class?

def batch_job_status_details(batch_name):
    """Generate status detail dictionaries for jobs in ETL batch."""
    sql = """
        select
            job_id, job_name, job_status, job_status_description,
            start_time, end_time
        from dbo.Metadata_ETL_Job_Status_vw
        where batch_name = '{}';
        """.format(batch_name)
    with pyodbc.connect(ETL_LOAD_A_ODBC_STRING) as conn:
        cursor = conn.execute(sql)
        field_names = [column[0] for column in cursor.description]
        for row in cursor:
            yield dict(zip(field_names, row))


def batch_metadata(batch_name):
    """Return metadata dictionary for ETL batch."""
    sql = """
        select top 1 batch_id, batch_name, notification_email_recipients
        from dbo.Metadata_ETL_Batch
        where batch_name = '{}';
        """.format(batch_name)
    with pyodbc.connect(ETL_LOAD_A_ODBC_STRING) as conn:
        cursor = conn.execute(sql)
        field_names = [column[0] for column in cursor.description]
        try:
            meta = next(dict(zip(field_names, row)) for row in cursor)
        except StopIteration:
            raise AttributeError("batch_name does not exist in"
                                 " batch metadata table.")
    return meta


def batch_name_id_map():
    """Return ETL batch name & ID as a key/value pair in a dictionary."""
    sql = "select batch_name, batch_id from dbo.Metadata_ETL_Batch;"
    with pyodbc.connect(ETL_LOAD_A_ODBC_STRING) as conn:
        cursor = conn.execute(sql)
        return dict(cursor)


def execute_pipeline(exec_object, log_folder_path=None):
    """Execute pipeline for the given execution object."""
    if isinstance(exec_object, Job):
        pipeline = {'name': exec_object.name, 'type': 'job',
                    'etls': exec_object.etls}
        pipeline['id'], pipeline['status'] = job_run_status(pipeline['name'])
    # Functions are assumed to be ETLs or similar standalone pipelines.
    elif isinstance(exec_object, (types.FunctionType, functools.partial)):
        pipeline = {'name': getattr(exec_object, '__name__', 'Unnamed ETL'),
                    'type': 'ETL', 'etls': [exec_object],
                    'id': None, 'status': -1}
    else:
        raise ValueError("Unknown exec_object type.")
    if log_folder_path is None:
        log_folder_path = path.LOGFILES
    logfile = os.path.join(log_folder_path, '{}.log'.format(pipeline['name']))
    init_root_logger(logfile, file_mode='w', file_level=10)
    LOG.info("Starting %s: %s.", pipeline['type'], pipeline['name'])
    # Run pipeline ETLs.
    for etl in pipeline['etls']:
        try:
            etl()
        except:
            LOG.exception("Unhandled exception.")
            raise
    pipeline['status'] = 0  # Successful execution.
    if pipeline['type'] == 'job':
        job_run_status(pipeline['name'], pipeline['status'], pipeline['id'])
    LOG.info("%s complete.", pipeline['name'])
    return pipeline['status']


def init_root_logger(file_path=None, file_mode='a', file_level=None):
    """Return initialized file logger object."""
    # Logger.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    # Formatters.
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    # Handlers.
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    if file_path:
        file_handler = logging.FileHandler(filename=file_path, mode=file_mode)
        file_handler.setLevel(file_level if file_level else logging.DEBUG)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    return root_logger


def job_run_status(job_name, run_status=-1, run_id=None):
    """Set job run status on the job history table & return run ID & status.

    run_status: 0 (complete), 1 (failed), -1 (incomplete).
    If run_id not defined, method will create new job run row in the table.
    Returns a tuple of run_id & run_status.
    """
    # If already started, update row status and end time.
    if run_id:
        sql = """
            update dbo.Metadata_ETL_Job_History
            set job_status = {}, end_time = getdate()
            output inserted.etl_job_history_id
            where etl_job_history_id = {};
            """.format(run_status, run_id)
    # Add new row for the run.
    else:
        sql = """
            insert into dbo.Metadata_ETL_Job_History(
                job_id, job_name, start_time, job_status
                )
            output inserted.etl_job_history_id
            select job_id, job_name, getdate(), -1
            from dbo.Metadata_ETL_Job where job_name = '{}';
            """.format(job_name)
    with pyodbc.connect(ETL_LOAD_A_ODBC_STRING) as conn:
        if run_status in (-1, 0, 1):
            with conn.execute(sql) as cursor:
                output = cursor.fetchall()
            if len(output) < 1:
                raise RuntimeError(
                    "No job named {} in job metadata table.".format(job_name)
                    )
            else:
                run_id = output[0][0]
            conn.commit()
        else:
            raise ValueError("Run status must be -1, 0, or 1.")
    return run_id, run_status


def send_batch_notification(batch_name):
    """Send notification for given batch."""
    meta = batch_metadata(batch_name)
    status_details = sorted(batch_job_status_details(batch_name),
                            key=operator.itemgetter('start_time', 'end_time'),
                            reverse=True)
    batch_status = (0 if all(job['job_status'] == 0 for job in status_details)
                    else -1)
    if meta['notification_email_recipients']:
        html_table_start_tag = '<table style="width:100%">'
        html_table_header_row = ('<tr><th>Name</th><th>Status</th>'
                                 '<th>Start Time</th><th>End Time</th></tr>')
        html_table_row_template = (
            '<tr><td>{job_name}</td><td>{job_status_description}</td>'
            '<td>{start}</td><td>{end}</tr>'
            )
        html_table_end_tag = '</table>'
        html_table_rows = []
        for job in status_details:
            job['start'] = (job['start_time'].strftime('%Y-%m-%d %H:%M')
                            if job['start_time'] else 'n/a')
            job['end'] = (job['end_time'].strftime('%Y-%m-%d %H:%M')
                          if job['end_time'] else 'n/a')
            html_table_rows.append(html_table_row_template.format(**job))
        html_full_string = "{start}{header}{rows}{end}".format(
            start=html_table_start_tag, header=html_table_header_row,
            rows=''.join(html_table_rows), end=html_table_end_tag
            )
        communicate.send_email(
            subject="ETL Batch: {} ({})".format(
                batch_name, STATUS_DESCRIPTION_MAP[batch_status]
                ),
            recipients=meta['notification_email_recipients'],
            body=html_full_string, body_format='HTML'
            )
    return html_full_string
