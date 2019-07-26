"""Credential store access objects.

The current credential store is a configuration file in the resources
subfolder. Though this seems like bad practice, the file has been set up to
extremely limit access; also the resources folder is ignored by the
repository.

"""
from configparser import ConfigParser
import logging
import os
import subprocess

from . import path


__all__ = (
    "CEDOM100",
    'CPA_MAP_SERVER',
    'EPERMITTING_ACCELA_FTP',
    'EPERMITTING_ADDRESSES_FTP',
    'LANE_TAX_MAP_DATABASE',
    'OEM_FILE_SHARING',
    'RLID_DATA_SHARE',
    'RLID_MAPS',
    'UNCPathCredential',
    )
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


parser = ConfigParser()  # pylint: disable=invalid-name
parser.read(os.path.join(path.RESOURCES, 'credentials.cfg'))


CEDOM100 = dict(parser.items('CEDOM100'))

CPA_MAP_SERVER = dict(parser.items('CPA Map Server'))

EPERMITTING_ACCELA_FTP = dict(parser.items('ePermitting Accela FTP'))
EPERMITTING_ADDRESSES_FTP = dict(parser.items('ePermitting Addresses FTP'))

LANE_TAX_MAP_DATABASE = dict(parser.items('Lane Tax Map Database'))

OEM_FILE_SHARING = dict(parser.items('OEM File Sharing'))

RLID_DATA_SHARE = dict(parser.items('RLID Data Share'))
RLID_MAPS = dict(parser.items('RLID Maps'))


class UNCPathCredential(object):
    """Simple manager for UNC credentials.

    ##TODO: Attributes.
    """

    def __init__(self, unc_path, username=None, password=None):
        """Initialize CredentialUNC instance.

        ##TODO: Args, Keyword Args, Returns.
        """
        self.path = unc_path
        self.username = username
        self.__password = password

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()

    def connect(self, username=None, password=None):
        """Connects the UNC directory."""
        LOG.info("Connecting UNC path %s.", self.path)
        call_string = 'net use "{}"'.format(self.path)
        if password or self.__password:
            call_string += ' {}'.format(password if password
                                        else self.__password)
        if username or self.username:
            call_string += ' /user:"{}"'.format(username if username
                                                else self.username)
        subprocess.check_call(call_string)

    def disconnect(self):
        """Disconnects the UNC directory."""
        LOG.info("Disconnecting UNC path %s.", self.path)
        call_string = 'net use "{}" /delete /yes'.format(self.path)
        try:
            subprocess.check_call(call_string)
        except subprocess.CalledProcessError as disconnect_error:
            if disconnect_error.returncode == 2:
                LOG.debug("Network resource %s already disconnected.",
                          self.path)

    def __str__(self):
        return self.path
