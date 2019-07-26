"""URL objects

Extends the `url` submodule from the ETLAssist package.
"""
try:
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin

from etlassist.url import *  # pylint: disable=wildcard-import, unused-wildcard-import


EPERMITTING_ACCELA_FTP = "sftp://64.74.214.187:/"
EPERMITTING_ADDRESSES_FTP = "sftp://imd20.cbs.state.or.us:/home/lane_co/"

OEM_FILE_SHARING = "https://upload.oregonem.com/"

RLID = "https://www.rlid.org/"
RLID_MAPS = "https://open.maps.rlid.org/"

RLID_IMAGE_SHARE = urljoin(RLID, "/ImageShare")
RLID_PROPERTY_SEARCH = urljoin(
    RLID, "/property_search/standard.cfm?do=propsearch_standard.reprocess"
)
