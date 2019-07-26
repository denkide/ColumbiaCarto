"""SQLAlchemy models."""
from sqlalchemy import Column
from sqlalchemy.types import Boolean, DateTime, Integer, Numeric, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()  # pylint: disable=invalid-name


class LaneTaxMapReleaseLog(Base):  # pylint: disable=too-few-public-methods
    """Model for Lane County tax map release log table."""

    __tablename__ = "ReleaseLog"
    release_log_id = Column("ReleaseLogID", Integer, primary_key=True)
    release_datetime = Column("ReleaseDateTime", DateTime)
    map_name = Column("MapName", String)
    map_path = Column("MapPathname", String)


class LicenseArcGISDesktop(Base):  # pylint: disable=too-few-public-methods
    """Model for ArcGIS Desktop license table."""

    __tablename__ = "License_ArcGIS_Desktop"
    id = Column(Integer, primary_key=True)  # pylint: disable=invalid-name
    internal_name = Column(String, unique=True, nullable=False)
    common_name = Column(String, nullable=False)
    slug = Column(String, nullable=False)
    is_application = Column(Boolean, nullable=False, default=False)
    is_extension = Column(Boolean, nullable=False, default=False)


class LicenseUsage(Base):  # pylint: disable=too-few-public-methods
    """Model for license usage table."""

    __tablename__ = "License_Usage"
    id = Column(Integer, primary_key=True)  # pylint: disable=invalid-name
    usage_check_time = Column(DateTime, nullable=False)
    user_handle = Column(String)
    user_host = Column(String, nullable=False)
    license_internal_name = Column(String, nullable=False)
    checkout_time = Column(DateTime, nullable=False)
    is_borrowed = Column(Boolean, nullable=False)


class RLIDAccount(Base):  # pylint: disable=too-few-public-methods
    """Model for account table in the RLID warehouse."""

    __tablename__ = "Account"
    prop_id = Column(Integer, primary_key=True)
    account_int = Column(Integer)
    account_stripped = Column(String)
    account_type = Column(String)
    maptaxlot = Column("maplot", String)
    spcl_int_code = Column(String)
    active_this_year = Column(String)
    new_acct_active_next_year = Column(String)


class RLIDDeedRecDocImage(Base):  # pylint: disable=too-few-public-methods
    """Model for deeds & records image table in the RLID warehouse."""

    __tablename__ = "vw_DeedRec_Image"
    image_id = Column(Integer, primary_key=True)
    doc_id = Column(Integer)
    image_file_name = Column(Integer)
    doc_year = Column(Integer)
    doc_num = Column(Integer)
    doc_type = Column(String)
    doc_type_desc = Column(String)
    recording_date = Column(DateTime)
    image_exists_flag = Column(String)


class RLIDExemption(Base):  # pylint: disable=too-few-public-methods
    """Model for exemption table in the RLID warehouse."""

    __tablename__ = "Exemption"
    prop_id = Column(Integer, primary_key=True)
    tax_year = Column(String, primary_key=True)
    exm_id = Column(Integer, primary_key=True)
    account_int = Column(Integer)
    maptaxlot = Column("maplot", String)
    spcl_int_code = Column(String)
    amt = Column(Numeric)
    description = Column(String)


class RLIDImprovement(Base):  # pylint: disable=too-few-public-methods
    """Model for improvement table in the RLID warehouse."""

    __tablename__ = "Improvement"
    prop_id = Column(Integer, primary_key=True)
    extension = Column(String, primary_key=True)
    dwelling_number = Column(Integer, primary_key=True)
    account_int = Column(Integer)
    maptaxlot = Column("maplot", String)
    spcl_int_code = Column(String)
    bldg_type = Column(String)
    year_built = Column(String)
    total_finish_sqft = Column(Integer)


class RLIDMetadataDataCurrency(Base):  # pylint: disable=too-few-public-methods
    """Model for data currency metadata table in the RLID warehouse."""

    __tablename__ = "MD_Data_Currency"
    source_database = Column(String, primary_key=True)
    data_description = Column(String, primary_key=True)
    writeoff_date = Column(DateTime)
    load_date = Column(DateTime)


class RLIDMetadataTaxYear(Base):  # pylint: disable=too-few-public-methods
    """Model for tax-year metadata table in the RLID warehouse."""

    __tablename__ = "Metadata_Tax_Year"
    md_tax_year_id = Column(Integer, primary_key=True)
    current_tax_year = Column(String)


class RLIDOwner(Base):  # pylint: disable=too-few-public-methods
    """Model for owner table in the RLID warehouse."""

    __tablename__ = "Owner"
    owner_id = Column(Integer, primary_key=True)
    account_int = Column(Integer)
    account_active_latest_year = Column(String)
    maptaxlot = Column("maplot", String)
    spcl_int_code = Column(String)
    owner_name = Column(String)
    addr_line1 = Column(String)
    addr_line2 = Column(String)
    addr_line3 = Column(String)
    city = Column(String)
    prov_state = Column(String)
    zip_code = Column(String)
    country = Column(String)


class RLIDParties(Base):  # pylint: disable=too-few-public-methods
    """Model for parties table in the RLID warehouse."""

    __tablename__ = "Parties"
    prop_id = Column(Integer, primary_key=True)
    ppi_id = Column(Integer, primary_key=True)
    account_int = Column(Integer)
    role_cd = Column(Integer)
    role_description = Column(String)
    party_id = Column(Integer)
    party_name = Column(String)


class RLIDTaxMapArchive(Base):  # pylint: disable=too-few-public-methods
    """Model for tax map archive table in the RLID warehouse."""

    __tablename__ = "TaxMap_Archive"
    taxmap_archive_id = Column(Integer, primary_key=True)
    taxmap_filename = Column(String(30), nullable=False)
    taxmap_archive_filename = Column(String(30), nullable=False)
    date_archived = Column(DateTime)
    archived_no_replacement = Column(String(1))


class RLIDTaxMapImage(Base):  # pylint: disable=too-few-public-methods
    """Model for tax map image table in the RLID warehouse."""

    __tablename__ = "Taxmap_Image"
    taxmap_image_id = Column(Integer, primary_key=True)
    image_filename = Column(String(30), nullable=False)
    date_modified = Column(DateTime)


class RLIDTaxYear(Base):  # pylint: disable=too-few-public-methods
    """Model for tax-year table in the RLID warehouse."""

    __tablename__ = "Tax_Year"
    prop_id = Column(Integer, primary_key=True)
    tax_year = Column(String, primary_key=True)
    account_int = Column(Integer)
    account_stripped = Column(String)
    maptaxlot = Column("maplot", String)
    tca = Column(String)
    code_split_ind = Column(String)
    prop_class = Column(String)
    prop_class_desc = Column(String)
    stat_class = Column(String)
    stat_class_desc = Column(String)
    land_acres = Column(Numeric)
    rmv_land_value = Column(Integer)
    rmv_imp_value = Column(Integer)
    rmv_total_value = Column(Integer)
    assd_total_value = Column(Integer)
    exm_amt_reg_value = Column(Integer)
    taxable_value = Column(Integer)
