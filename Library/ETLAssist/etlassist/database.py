"""Database objects."""
import logging
import os
try:
    from urllib.parse import quote_plus
except ImportError:
    from urllib import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from . import credential
from . import path


__all__ = [
    "ADDRESSING",
    "CPA_ADMIN",
    "DEVELOPMENT",
    "ETL_LOAD_A",
    "EUGENEGEO",
    "GEOPORTAL_EUGENE",
    "GEOPORTAL_EUGENE_DEV",
    "GEOPORTAL_RLID",
    "GEOPORTAL_RLID_DEV",
    "LCIS_SANDBOX",
    "LCOG_CLMPO",
    "LCOG_TILLAMOOK_ECD",
    "LCOGGEO",
    "REGIONAL",
    "RLID",
    "RLID_DEEDREC",
    "RLID_EXPORT_ACCELA",
    "RLID_STAGING",
    "RLID_WEBAPP_V3",
    "RLIDGEO",
    "RLIDGEO_LASTLOAD",
    "RLIDGEO_LASTLOAD_TEST",
    "RLIDGEO_TEST",
    "SPRINGFIELD_SANDBOX",
    "GISRV106_DATABASES",
    "TAX_MAP_DISTRIBUTION",
    "Database",
    "access_odbc_string",
    "sql_server_odbc_string",
]
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


class Database(object):
    """Representation of database information.

    Attributes:
        name (str): Name of the database.
        host (str): Name of the SQL Server instance host.
        description (str): Description of database.
        path (str): SDE-style path to database.

    """

    def __init__(self, name, host, sde_path=None, **kwargs):
        """Initialize instance.

        Args:
            name (str): Name of the database.
            host (str): Name of the SQL Server instance host.
            sde_path (str): SDE-style path to database.
            **kwargs: Arbitrary keyword arguments. See below.

        Keyword Args:
            description (str): Description of database.
            credential (dict): Mapping of 'username' and 'password' keys to values.
            username (str): Name of user for credential. Not applicable if username
                already defined in credential.
            password (str): Password for credential. Not applicable if password already
                defined in credential.
            back_up_build_sql (bool): Flag indicating whether to back up build SQL
                script. Default is False.
            back_up_gdb_data (bool): Flag indicating whether to back up geodatabase
                data. Default is False.
            back_up_gdb_schema (bool): Flag indicating whether to back up geodatabase
                schema. Default is False.
            compress (bool): Flag indicating whether to compress the geodatabase.
                Default is False.

        """
        self.name = name
        self.host = host
        self.description = kwargs.get('description')
        self.path = sde_path
        self._credential = kwargs.get('credential', {})
        self._credential.setdefault('username', kwargs.get('username'))
        self._credential.setdefault('password', kwargs.get('password'))
        self._flags = {
            'back_up_build_sql': kwargs.get('back_up_build_sql', False),
            'back_up_gdb_data': kwargs.get('back_up_gdb_data', False),
            'back_up_gdb_schema': kwargs.get('back_up_gdb_schema', False),
            'compress': kwargs.get('compress', False),
        }
        self._sqlalchemy = {}

    def __repr__(self):
        return "{}(name={!r}, host={!r})".format(
            self.__class__.__name__, self.name, self.host
        )

    @property
    def back_up_build_sql(self):
        """bool: Flag indicating whether to back up the build SQL script."""
        return self._flags['back_up_build_sql']

    @property
    def back_up_gdb_data(self):
        """bool: Flag indicating whether to back up the geodatabase data."""
        return self._flags['back_up_gdb_data']

    @property
    def back_up_gdb_schema(self):
        """bool: Flag indicating whether to back up the geodatabase schema."""
        return self._flags['back_up_gdb_schema']

    @property
    def compress(self):
        """bool: Flag indicating whether to compress the geodatabase."""
        return self._flags['compress']

    @property
    def odbc_string(self):
        """str: String necessary for ODBC connection. Assumes trusted connection."""
        return sql_server_odbc_string(self.host, self.name, **self._credential)

    def create_session(self):
        """Return SQLAlchemy session instance to database.

        Returns:
            sqlalchemy.orm.session.Session: Session object connected to the database.

        """
        url = self._sqlalchemy.setdefault(
            'url',
            "mssql+pyodbc:///?odbc_connect={}".format(quote_plus(self.odbc_string))
        )
        engine = self._sqlalchemy.setdefault('engine', create_engine(url))
        return self._sqlalchemy.setdefault(
            'SessionFactory', sessionmaker(bind=engine)
        )()


def access_odbc_string(database_path):
    """Return ODBC connection string for use by ODBC libraries & apps.

    Args:
        database_path (str): Path top the Access database.

    Returns
        str: string for ODBC connection to Access database.

    """
    _string = "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + database_path
    return _string


def sql_server_odbc_string(host, database_name=None, username=None, password=None,
                           **kwargs):
    """Return ODBC connection string for use by ODBC libraries & apps.

    Defaults to trusted connection. If username and password are defined, they
    will override the trusted connection setting.
    Depending on your ODBC setup, omitting a login and trusted will either
    fail or prompt for credentials.

    Args:
        host (str): Name of the SQL Server instance host.
        database_name (str, None): Name of the database (optional).
        username (str): Username to connect with (optional).
        password (str): Password to connect with (optional).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        driver_string (str): ODBC string for driver & version. Default is "{SQL Server
            Native Client 11.0}".

    Returns:
        str: string for ODBC connection to SQL Server database.

    """
    kwargs.setdefault("driver_string", "{SQL Server Native Client 11.0}")
    _string = "Driver={};Server={};".format(kwargs["driver_string"], host)
    if database_name:
        _string += "Database={};".format(database_name)
    if username and password:
        _string += "Uid={};Pwd={};".format(username, password)
    else:
        _string += 'Trusted_Connection=yes;'
    return _string


# GISQL113 databases.

GEOPORTAL_EUGENE = Database(
    name="Geoportal_Eugene",
    host="gisql113.ris5.net",
    description="Eugene Geoportal database.",
)
GEOPORTAL_EUGENE_DEV = Database(
    name="Geoportal_Eugene_Dev",
    host="gisql113.ris5.net",
    description="Eugene Geoportal database (development).",
)
GEOPORTAL_RLID = Database(
    name="Geoportal_RLID",
    host="gisql113.ris5.net",
    description="CPA/RLID Geoportal database.",
)
GEOPORTAL_RLID_DEV = Database(
    name="Geoportal_RLID_Dev",
    host="gisql113.ris5.net",
    description="CPA/RLID Geoportal database (development).",
)
RLID = Database(
    name="RLID",
    host="gisql113.ris5.net",
    description="RLID warehouse database.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisql113.rlid.sde"),
)
RLID_DEEDREC = Database(
    name="RLID_DeedRec",
    host="gisql113.ris5.net",
    description="RLID deeds & records database.",
)
RLID_EXPORT_ACCELA = Database(
    name="RLID_Export_Accela",
    host="gisql113.ris5.net",
    description="RLID Accela export database.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisql113.rlid_export_accela.sde"),
)
RLID_STAGING = Database(
    name="RLID_Staging",
    host="gisql113.ris5.net",
    description="RLID staging database.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisql113.rlid_staging.sde"),
)
RLID_WEBAPP_V3 = Database(
    name="RLID_WebApp_V3",
    host="gisql113.ris5.net",
    description="RLID web application database.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisql113.rlid_webapp_v3.sde"),
)


# GISRV106 databases.

# System.

MSDB = Database(
    name="MSDB",
    host="gisrv106.ris5.net",
    description="Microsoft SQL Server system database.",
)

# User.

ADDRESSING = Database(
    name="Addressing",
    host="gisrv106.ris5.net",
    description="Addressing production geodatabase.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.addressing.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=True,
)
CPA_ADMIN = Database(
    name="CPA_Admin",
    host="gisrv106.ris5.net",
    description="CPA project administration database.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.cpa_admin.sde"),
    back_up_build_sql=True,
    back_up_gdb_schema=False,
    back_up_gdb_data=False,
)
DEVELOPMENT = Database(
    name="Development",
    host="gisrv106.ris5.net",
    description="Development geodatabase.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.development.sde"),
    compress=False,
    back_up_build_sql=False,
    back_up_gdb_schema=False,
    back_up_gdb_data=False,
)
ETL_LOAD_A = Database(
    name="ETL_Load_A",
    host="gisrv106.ris5.net",
    description="ETL loading database.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.etl_load_a.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=False,
)
EUGENEGEO = Database(
    name="EugeneGeo",
    host="gisrv106.ris5.net",
    description="Eugene production geodatabase.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.eugenegeo.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=True,
)
LCIS_SANDBOX = Database(
    name="LCIS_Sandbox",
    host="gisrv106.ris5.net",
    description="Lane County IS Sandbox Geodatabase",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.lcis_sandbox.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=True,
)
LCOG_CLMPO = Database(
    name="LCOG_CLMPO",
    host="gisrv106.ris5.net",
    description="Central Lane MPO database.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.lcog_clmpo.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=True,
)
LCOG_TILLAMOOK_ECD = Database(
    name="LCOG_TillamookECD",
    host="gisrv106.ris5.net",
    description="Production database for Tillamook ECD/OEM contract.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.lcog_tillamookecd.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=True,
)
LCOGGEO = Database(
    name="LCOGGeo",
    host="gisrv106.ris5.net",
    description="LCOG production geodatabase.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.lcoggeo.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=True,
)
REGIONAL = Database(
    name="Regional",
    host="gisrv106.ris5.net",
    description="Lane A&T/regional production geodatabase.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.regional.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=True,
)
RLIDGEO = Database(
    name="RLIDGeo",
    host="gisrv106.ris5.net",
    description="Regional publication geodatabase.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.rlidgeo.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=False,
)
RLIDGEO_LASTLOAD = Database(
    name="RLIDGeo_LastLoad",
    host="gisrv106.ris5.net",
    description="Last-load copy of regional publication geodatabase.",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.rlidgeo_lastload.sde"),
    compress=True,
    # Schema back up currently on this hangs the whole job.
    # Not changing enough to be that important.
    back_up_build_sql=True,
    back_up_gdb_schema=False,
    back_up_gdb_data=False,
)
RLIDGEO_LASTLOAD_TEST = Database(
    name="RLIDGeo_LastLoad_Test",
    host="gisrv106.ris5.net",
    description="Last-load copy of regional publication geodatabase (test version).",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.rlidgeo_lastload_test.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=False,
    back_up_gdb_data=False,
)
RLIDGEO_TEST = Database(
    name="RLIDGeo_Test",
    host="gisrv106.ris5.net",
    description="Regional publication geodatabase (test version).",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.rlidgeo_test.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=False,
    back_up_gdb_data=False,
)
SPRINGFIELD_SANDBOX = Database(
    name="Springfield_Sandbox",
    host="gisrv106.ris5.net",
    description="Springfield GIS Sandbox Geodatabase",
    sde_path=os.path.join(path.SDE_CONNECTIONS, "gisrv106.springfield_sandbox.sde"),
    compress=True,
    back_up_build_sql=True,
    back_up_gdb_schema=True,
    back_up_gdb_data=True,
)
GISRV106_DATABASES = [
    ADDRESSING,
    CPA_ADMIN,
    DEVELOPMENT,
    ETL_LOAD_A,
    EUGENEGEO,
    LCIS_SANDBOX,
    LCOG_TILLAMOOK_ECD,
    LCOGGEO,
    REGIONAL,
    RLIDGEO,
    RLIDGEO_LASTLOAD,
    RLIDGEO_TEST,
    SPRINGFIELD_SANDBOX,
]


# Other instance databases.

TAX_MAP_DISTRIBUTION = Database(
    name="TaxMapDistribution",
    host="lcsql128.lc100.net",
    description="Lane County A&T tax map distribution database.",
    credential=credential.LANE_TAX_MAP_DATABASE,
)
