"""Communication objects."""
from collections import Iterable
import logging
import re
import sys

import pyodbc

from . import database

if sys.version_info.major >= 3:
    basestring = str


__all__ = (
    'extract_email_addresses',
    'send_email',
    'send_links_email',
    )
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def extract_email_addresses(*args):
    """Generate email addresses parsed from various objects."""
    for element in args:
        if isinstance(element, basestring):
            addresses = (a for a in re.findall(r'[\w\.-]+@[\w\.-]+', element))
        elif isinstance(element, Iterable):
            addresses = extract_email_addresses(*element)
        elif isinstance(element, dict):
            addresses = extract_email_addresses(*element.items())
        # If type is unsupported, just make empty generator. This makes
        # parsing emails from mixed-type collections easier without filtering.
        else:
            addresses = (_ for _ in ())
        for address in addresses:
            yield address


def send_email(subject, recipients, body=None, **kwargs):
    """Send email (via SQL Server).

    Args:
        subject (str): Email subject line.
        recipients (list, str): Email addresses for recipient(s).
        body (str): Message body text.
    Kwargs:
        body_format (str): Format of body text. Options are 'text' and 'html'.
        copy_recipients (list, str): Email addresses for message copy- (cc)
            recipients.
        blind_copy_recipients (list, str): Email addresses for message blind
            copy- (bcc) recipients.
        reply_to (list, str): Email address for message reply recipient.

    Unused sp_send_dbmail arguments:
        @from_address, @importance, @sensitivity, @file_attachments, @query,
        @execute_query_database, @attach_query_result_as_file,
        @query_attachment_filename, @query_result_header, @query_result_width,
        @query_result_separator, @exclude_query_output, @append_query_error,
        @query_no_truncate, @query_result_no_padding, @mailitem_id
    """
    sql = "exec dbo.sp_send_dbmail @profile_name = 'Geodatabase Server'"
    sql += ", @subject = '{}'".format(subject)
    sql += ", @recipients = '{}'".format(";".join(extract_email_addresses(recipients)))
    for kwarg in 'copy_recipients', 'blind_copy_recipients', 'reply_to':
        if kwarg in kwargs:
            sql += ", @{} = '{}'".format(
                kwarg, ";".join(extract_email_addresses(kwargs[kwarg]))
                )
    if body:
        # Need to escape single-quotes for SQL.
        sql += ", @body = '{}'".format(body.replace("'", "''"))
        sql += ", @body_format = '{}'".format(kwargs.get('body_format', 'text'))
    sql += ";"
    with pyodbc.connect(database.MSDB.odbc_string) as conn:  # pylint: disable=no-member
        conn.execute(sql)
    return body


def send_links_email(urls, subject, recipients, body_pre_links=None,
                     body_post_links=None, **kwargs):
    """Send email with a listing of URLs.

    Args:
        urls (iter): Iterable of URL strings.
        subject (str): Email subject line.
        recipients (iter, str): Email addresses for recipient(s).
        body_pre_links (str): Message body text.
        body_post_links (str): Message body text.
    Kwargs:
        See kwargs for send_email().
    """
    list_item_template = '<li><a href="{0}">{0}</a></li>'
    list_items = '\n'.join('<ul>{}</ul>'.format(list_item_template.format(url))
                           for url in urls)
    body = ''
    if body_pre_links:
        body += body_pre_links
    body += list_items
    if body_post_links:
        body += body_post_links
    send_email(subject, recipients, body, body_format='HTML', **kwargs)
