"""URL objects."""
import ftplib
import logging
import os
import subprocess
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from . import path


__all__ = (
    'send_file_by_ftp',
    'send_file_by_sftp',
    )
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


# TODO: Create send_file_to_remote function (parses URL for protocol).


def send_file_by_ftp(file_path, output_url, username=None, password=None):
    """Send file to host via FTP protocol."""
    def send_file(ftp_instance, file_path, output_name, output_directory_path):
        """Commands to send the file with the FTP instance."""
        if output_directory_path:
            ftp_instance.cwd(output_directory_path)
        with open(file_path, mode='rb') as file:
            ftp_instance.storbinary('STOR {}'.format(output_name), file)
    LOG.info("Sending %s to %s via FTP protocol.", file_path, output_url)
    parsed_url = urlparse(output_url)
    output_directory_path, output_name = os.path.split(parsed_url.path)
    # FTP class context manager only exists in Python 3+.
    try:
        ftp = ftplib.FTP(parsed_url.netloc, username, password)
        send_file(ftp, file_path, output_name, output_directory_path)
    finally:
        ftp.quit()
    LOG.info("%s sent.", file_path)
    return output_url


# TODO: Replace PSFTP usage with PySFTP (https://pysftp.readthedocs.io).
def send_file_by_sftp(file_path, output_url, username=None, password=None,
                      keyfile_path=None, **kwargs):
    """Send files to server via SCP/SFTP protocol."""
    LOG.info("Sending %s to %s via SCP/SFTP protocol.", file_path, output_url)
    parsed_url = urlparse(output_url)
    pscp_options = ' '.join((
        '-q',  # Quiet, don't show statistics.
        '-v',  # Show verbose messages.
        '-P {}'.format(kwargs.get('host_port', 22)),
        '-l {}'.format(username) if username else '',
        '-pw {}'.format(password) if password else '',
        '-C', # Enable compression.
        '-i {}'.format(keyfile_path) if keyfile_path else '',
        '-batch',  # Disable all interactive prompts.
        '-sftp',  # Force use of SFTP protocol.
        ))
    # Execution: pscp.exe [options] source [source...] [user@]host:target
    call_string = '{exe} {options} {source} {target}'.format(
        exe=path.PSCP, options=pscp_options,
        source=file_path, target=parsed_url.netloc + parsed_url.path
        )
    subprocess.check_call(call_string)
    LOG.info("%s sent.", file_path)
    return output_url
