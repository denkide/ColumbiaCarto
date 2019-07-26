"""Dataset objects for non-CPA projects.

Extends the ETLAssist dataset submodule.
"""
import os

from etlassist.dataset import *  # pylint: disable=wildcard-import,unused-wildcard-import

from . import database
from . import path


"""##TODO:
    * Make dataset a subpackage (folder + __init__.py).
    * Then projects into submodules (e.g. `dataset.oem.TILLAMOOK_*`).
        * Alternatively, `datset.oem.tillamook.*`
"""


GBF_FREE_BIKE_STATUS = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "bike_id", "type": "text", "length": 32, "is_id": True},
        {"name": "name", "type": "text", "length": 64},
        {"name": "lon", "type": "double"},
        {"name": "lat", "type": "double"},
        {"name": "is_reserved", "type": "short"},
        {"name": "is_disabled", "type": "short"},
    ],
    path={
        "source": "free_bike_status",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_Free_Bike_Status"),
    },
)

GBF_GBFS = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "name", "type": "text", "length": 64, "is_id": True},
        {"name": "url", "type": "text", "length": 128},
    ],
    path={
        "source":"gbfs",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_GBFS"),
    },
)

GBF_STATION_INFORMATION = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "station_id", "type": "text", "length": 32, "is_id": True},
        {"name": "name", "type": "text", "length": 64},
        {"name": "short_name", "type": "text", "length": 32},
        {"name": "lon", "type": "double"},
        {"name": "lat", "type": "double"},
        {"name": "address", "type": "text", "length": 128},
        {"name": "cross_street", "type": "text", "length": 32},
        {"name": "region_id", "type": "text", "length": 32},
        {"name": "post_code", "type": "text", "length": 16},
        {"name": "rental_methods", "type": "text", "length": 128},
        {"name": "capacity", "type": "long"},
    ],
    path={
        "source": "station_information",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_Station_Information"),
    },
)

GBF_STATION_STATUS = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "station_id", "type": "text", "length": 16, "is_id": True},
        {"name": "num_bikes_available", "type": "long"},
        {"name": "num_bikes_disabled", "type": "long"},
        {"name": "num_docks_available", "type": "long"},
        {"name": "num_docks_disabled", "type": "long"},
        {"name": "is_installed", "type": "short"},
        {"name": "is_renting", "type": "short"},
        {"name": "is_returning", "type": "short"},
        {"name": "last_reported", "type": "long"},
    ],
    path={
        "source": "station_status",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_Station_Status"),
    },
)

GBF_SYSTEM_ALERTS = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "alert_id", "type": "text", "length": 32, "is_id": True},
        {"name": "type", "type": "text", "length": 16},
        {"name": "times", "type": "text", "length": 16},
        {"name": "station_ids", "type": "text", "length": 128},
        {"name": "region_ids", "type": "text", "length": 128},
        {"name": "url", "type": "text", "length": 128},
        {"name": "summary", "type": "text", "length": 32},
        {"name": "description", "type": "text", "length": 256},
        {"name": "last_updated", "type": "long"},
    ],
    path={
        "source": "system_alerts",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_System_Alerts"),
    },
)

GBF_SYSTEM_CALENDAR = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "start_month", "type": "short", "is_id": True},
        {"name": "start_day", "type": "short", "is_id": True},
        {"name": "start_year", "type": "short", "is_id": True},
        {"name": "end_month", "type": "short", "is_id": True},
        {"name": "end_day", "type": "short", "is_id": True},
        {"name": "end_year", "type": "short", "is_id": True},
    ],
    path={
        "source": "system_calendar",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_System_Calendar"),
    },
)

GBF_SYSTEM_HOURS = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "user_types", "type": "text", "length": 64, "is_id": True},
        {"name": "days", "type": "text", "length": 64},
        {"name": "start_time", "type": "text", "length": 8},
        {"name": "end_time", "type": "text", "length": 8},
    ],
    path={
        "source": "system_hours",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_System_Hours"),
    },
)

GBF_SYSTEM_INFORMATION = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "system_id", "type": "text", "length": 32, "is_id": True},
        {"name": "language", "type": "text", "length": 2},
        {"name": "name", "type": "text", "length": 64},
        {"name": "short_name", "type": "text", "length": 32},
        {"name": "operator", "type": "text", "length": 32},
        {"name": "url", "type": "text", "length": 128},
        {"name": "purchase_url", "type": "text", "length": 128},
        {"name": "start_date", "type": "text", "length": 16},
        {"name": "phone_number", "type": "text", "length": 16},
        {"name": "email", "type": "text", "length": 32},
        {"name": "timezone", "type": "text", "length": 32},
        {"name": "license_url", "type": "text", "length": 128},
    ],
    path={
        "source": "system_information",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_System_Information"),
    },
)

GBF_SYSTEM_PRICING_PLANS = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "plan_id", "type": "text", "length": 32, "is_id": True},
        {"name": "url", "type": "text", "length": 128},
        {"name": "name", "type": "text", "length": 64},
        {"name": "currency", "type": "text", "length": 4},
        {"name": "price", "type": "double"},
        {"name": "is_taxable", "type": "short"},
        {"name": "description", "type": "text", "length": 256},
    ],
    path={
        "source": "system_pricing_plans",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_System_Pricing_Plans"),
    },
)

GBF_SYSTEM_REGIONS = Dataset(
    fields=[
        {"name": "feed_last_updated", "type": "long", "is_id": True},
        {"name": "feed_ttl", "type": "long"},
        {"name": "region_id", "type": "text", "length": 32, "is_id": True},
        {"name": "name", "type": "text", "length": 64},
    ],
    path={
        "source": "system_regions",
        "pub": os.path.join(database.LCOG_CLMPO.path, "dbo.GBF_System_Regions"),
    },
)

TILLAMOOK_ADDRESS_POINT = Dataset(
    fields=[
        # Core address attributes.
        {"name": "address_id", "type": "long", "tags": ["maint", "pub"]},
        {"name": "address", "type": "text", "length": 128, "tags": ["pub"]},
        {"name": "stnum", "type": "long", "tags": ["maint", "pub"]},
        {"name": "stnumsuf", "type": "text", "length": 8, "tags": ["maint", "pub"]},
        {"name": "predir", "type": "text", "length": 2, "tags": ["maint", "pub"]},
        {"name": "name", "type": "text", "length": 32, "tags": ["maint", "pub"]},
        {"name": "type", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "sufdir", "type": "text", "length": 2, "tags": ["maint", "pub"]},
        {"name": "unit_type", "type": "text", "length": 8, "tags": ["maint", "pub"]},
        {"name": "unit", "type": "text", "length": 8, "tags": ["maint", "pub"]},
        {"name": "postcomm", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "join_id", "type": "long", "tags": ["pub"]},
        # Extended address attributes.
        {"name": "zip", "type": "text", "length": 5, "tags": ["maint", "pub"]},
        {"name": "county", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "state", "type": "text", "length": 2, "tags": ["pub"]},
        # Maintenance attributes.
        {"name": "valid", "type": "text", "length": 1, "tags": ["maint", "pub"]},
        {"name": "archived", "type": "text", "length": 1, "tags": ["maint", "pub"]},
        {"name": "confidence", "type": "text", "length": 1, "tags": ["maint", "pub"]},
        {"name": "issue", "type": "text", "length": 32, "tags": ["maint", "pub"]},
        {"name": "notes", "type": "text", "length": 256, "tags": ["maint"]},
        {"name": "init_date", "type": "date", "tags": ["maint", "pub"]},
        {"name": "mod_date", "type": "date", "tags": ["maint", "pub"]},
        {"name": "editor", "type": "text", "length": 16, "tags": ["maint"]},
        {"name": "auto_notes", "type": "text", "length": 512, "tags": ["maint"]},
        # City attributes.
        {"name": "city_limit", "type": "text", "length": 16, "tags": ["pub"]},
        # Public safety attributes.
        {"name": "ems", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "fire", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "police", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "esn", "type": "text", "length": 5, "tags": ["pub"]},
        # Geometry attributes.
        {"name": "lon", "type": "double", "tags": ["pub"]},
        {"name": "lat", "type": "double", "tags": ["pub"]},
    ],
    geometry_type="point",
    path={
        "maint": os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Address_Point"),
        "pub": os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.ETL_Address_Point"),
    },
)

TILLAMOOK_ADDRESS_POINT_ISSUES = Dataset(
    fields=[
        {"name": "address_id", "type": "long"},
        {"name": "address", "type": "text", "length": 128},
        {"name": "postcomm", "type": "text", "length": 16},
        {"name": "description", "type": "text", "length": 64},
        {"name": "ok_to_publish", "type": "short"},
        {"name": "maint_notes", "type": "text", "length": 128},
        {"name": "maint_init_date", "type": "date"},
    ],
    geometry_type="point",
    path=os.path.join(
        path.TILLAMOOK_PROJECT, "QC_Reference.gdb", "Address_Point_Issues"
    ),
)

TILLAMOOK_ADDRESS_POINT_NOT_IN_ROAD_RANGE = Dataset(
    fields=[
        {"name": "address_id", "type": "long"},
        {"name": "address", "type": "text", "length": 128},
        {"name": "postcomm", "type": "text", "length": 16},
        {"name": "description", "type": "text", "length": 64},
        {"name": "ok_to_publish", "type": "short"},
        {"name": "maint_notes", "type": "text", "length": 128},
        {"name": "maint_init_date", "type": "date"},
    ],
    geometry_type="point",
    path=os.path.join(
        path.TILLAMOOK_PROJECT, "QC_Reference.gdb", "Address_Point_Not_In_Road_Range"
    ),
)

TILLAMOOK_ALTERNATE_STREET_NAME = Dataset(
    fields=[
        {"name": "prime_predir", "type": "text", "length": 2},
        {"name": "prime_name", "type": "text", "length": 32},
        {"name": "prime_type", "type": "text", "length": 4},
        {"name": "prime_sufdir", "type": "text", "length": 2},
        {"name": "alt_predir", "type": "text", "length": 2},
        {"name": "alt_pretype", "type": "text", "length": 4},
        {"name": "alt_name", "type": "text", "length": 32},
        {"name": "alt_type", "type": "text", "length": 4},
        {"name": "alt_sufdir", "type": "text", "length": 2},
        {"name": "join_id", "type": "long"},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Alternate_Street_Name"),
)

TILLAMOOK_CITY_LIMITS = Dataset(
    fields=[
        {"name": "id", "type": "long"},
        {"name": "city", "type": "text", "length": 16},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    geometry_type="polygon",
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.City_Limits"),
)

TILLAMOOK_EMERGENCY_SERVICE_NUMBER = Dataset(
    fields=[
        {"name": "esn", "type": "text", "length": 5},
        {"name": "police", "type": "text", "length": 64},
        {"name": "fire", "type": "text", "length": 64},
        {"name": "ems", "type": "text", "length": 64},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Emergency_Service_Number"),
)

TILLAMOOK_EMERGENCY_SERVICE_ZONE = Dataset(
    fields=[
        {"name": "esn", "type": "text", "length": 5},
        {"name": "police", "type": "text", "length": 64},
        {"name": "fire", "type": "text", "length": 64},
        {"name": "ems", "type": "text", "length": 64},
    ],
    geometry_type="polygon",
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Emergency_Service_Zone"),
)

TILLAMOOK_EMS = Dataset(
    fields=[
        {"name": "id", "type": "long"},
        {"name": "district", "type": "text", "length": 64},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    geometry_type="polygon",
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.EMS"),
)

TILLAMOOK_EMS_ARA = Dataset(
    fields=[
        {"name": "id", "type": "long"},
        {"name": "district", "type": "text", "length": 64},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    geometry_type="polygon",
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.EMS_ARA"),
)

TILLAMOOK_FIRE = Dataset(
    fields=[
        {"name": "id", "type": "long"},
        {"name": "district", "type": "text", "length": 64},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    geometry_type="polygon",
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Fire"),
)

TILLAMOOK_FIRE_ARA = Dataset(
    fields=[
        {"name": "id", "type": "long"},
        {"name": "district", "type": "text", "length": 64},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    geometry_type="polygon",
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Fire_ARA"),
)

TILLAMOOK_METADATA_DELIVERABLES = Dataset(
    fields=[
        {"name": "dataset_name", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "last_change_date", "type": "date", "tags": ["pub"]},
    ],
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Metadata_Deliverables"),
)

TILLAMOOK_MSAG_RANGE = Dataset(
    fields=[
        {"name": "msag_id", "type": "guid"},
        {"name": "emergency_service_number", "type": "long"},
        {"name": "parity_code", "type": "text", "length": 1},
        {"name": "parity", "type": "text", "length": 8},
        {"name": "from_structure_number", "type": "long"},
        {"name": "to_structure_number", "type": "long"},
        {"name": "prefix_direction_code", "type": "text", "length": 2},
        {"name": "street_name", "type": "text", "length": 32},
        {"name": "street_type_code", "type": "text", "length": 5},
        {"name": "suffix_direction_code", "type": "text", "length": 2},
        {"name": "postal_community", "type": "text", "length": 16},
        {"name": "effective_date", "type": "date"},
        {"name": "expiration_date", "type": "date"},
    ],
    geometry_type="polygon",
    path={
        "current": os.path.join(
            database.LCOG_TILLAMOOK_ECD.path, "dbo.MSAG_Range_Current"
        ),
        "master": os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.MSAG_Range"),
    },
)

TILLAMOOK_POLICE = Dataset(
    fields=[
        {"name": "id", "type": "long"},
        {"name": "district", "type": "text", "length": 64},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    geometry_type="polygon",
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Police"),
)

TILLAMOOK_POLICE_ARA = Dataset(
    fields=[
        {"name": "id", "type": "long"},
        {"name": "district", "type": "text", "length": 64},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    geometry_type="polygon",
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Police_ARA"),
)

TILLAMOOK_POSTAL_COMMUNITY = Dataset(
    fields=[
        {"name": "postcomm", "type": "text", "length": 16},
        {"name": "zip", "type": "text", "length": 5},
        {"name": "init_date", "type": "date"},
        {"name": "mod_date", "type": "date"},
        {"name": "editor", "type": "text", "length": 16},
    ],
    geometry_type="polygon",
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Postal_Community"),
)

TILLAMOOK_ROAD_CENTERLINE = Dataset(
    fields=[
        # Core road attributes.
        {"name": "segid", "type": "long", "tags": ["maint", "pub"]},
        {"name": "full_name", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "predir", "type": "text", "length": 2, "tags": ["maint", "pub"]},
        {"name": "name", "type": "text", "length": 32, "tags": ["maint", "pub"]},
        {"name": "type", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "sufdir", "type": "text", "length": 2, "tags": ["maint", "pub"]},
        {"name": "join_id", "type": "long", "tags": ["pub"]},
        # Geocoding attributes.
        {"name": "fromleft", "type": "long", "tags": ["maint", "pub"]},
        {"name": "toleft", "type": "long", "tags": ["maint", "pub"]},
        {"name": "fromright", "type": "long", "tags": ["maint", "pub"]},
        {"name": "toright", "type": "long", "tags": ["maint", "pub"]},
        {"name": "postcomm_L", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "postcomm_R", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "zip_L", "type": "text", "length": 5, "tags": ["maint", "pub"]},
        {"name": "zip_R", "type": "text", "length": 5, "tags": ["maint", "pub"]},
        {"name": "county_L", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "county_R", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "state_L", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "state_R", "type": "text", "length": 2, "tags": ["pub"]},
        # Classification attributes.
        {"name": "road_class", "type": "text", "length": 5, "tags": ["maint", "pub"]},
        {"name": "cclass", "type": "short", "tags": ["maint", "pub"]},
        {"name": "st_class", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "surface", "type": "text", "length": 24, "tags": ["maint", "pub"]},
        {"name": "spdlimit", "type": "double", "tags": ["maint", "pub"]},
        # Network definition attributes.
        {"name": "oneway", "type": "text", "length": 2, "tags": ["maint", "pub"]},
        {"name": "F_elev", "type": "short", "tags": ["maint", "pub"]},
        {"name": "T_elev", "type": "short", "tags": ["maint", "pub"]},
        # Maintenance attributes.
        {"name": "notes", "type": "text", "length": 256, "tags": ["maint"]},
        {"name": "init_date", "type": "date", "tags": ["maint", "pub"]},
        {"name": "mod_date", "type": "date", "tags": ["maint", "pub"]},
        {"name": "editor", "type": "text", "length": 16, "tags": ["maint"]},
        {"name": "auto_notes", "type": "text", "length": 512, "tags": ["maint"]},
        # Public safety attributes.
        {"name": "esn_L", "type": "text", "length": 5, "tags": ["pub"]},
        {"name": "esn_R", "type": "text", "length": 5, "tags": ["pub"]},
    ],
    geometry_type="polyline",
    path={
        "maint": os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Road_Centerline"),
        "pub": os.path.join(
            database.LCOG_TILLAMOOK_ECD.path, "dbo.ETL_Road_Centerline"
        ),
    },
)

TILLAMOOK_ROAD_CENTERLINE_ISSUES = Dataset(
    fields=[
        {"name": "segid", "type": "long"},
        {"name": "full_name", "type": "text", "length": 64},
        {"name": "postcomm_L", "type": "text", "length": 16},
        {"name": "postcomm_R", "type": "text", "length": 16},
        {"name": "description", "type": "text", "length": 64},
        {"name": "ok_to_publish", "type": "short"},
        {"name": "maint_notes", "type": "text", "length": 128},
        {"name": "maint_init_date", "type": "date"},
    ],
    geometry_type="point",
    path=os.path.join(
        path.TILLAMOOK_PROJECT, "QC_Reference.gdb", "Road_Centerline_Issues"
    ),
)

OEM_VALID_COUNTY = Dataset(
    fields=[{"name": "county", "type": "text", "length": 16}],
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Valid_County"),
)

OEM_VALID_NUMBER_SUFFIX = Dataset(
    fields=[
        {"name": "code", "type": "text", "length": 8},
        {"name": "description", "type": "text", "length": 16},
    ],
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Valid_Number_Suffix"),
)

OEM_VALID_STREET_DIRECTION = Dataset(
    fields=[
        {"name": "code", "type": "text", "length": 4},
        {"name": "description", "type": "text", "length": 16},
    ],
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Valid_Street_Direction"),
)

OEM_VALID_STREET_TYPE = Dataset(
    fields=[
        {"name": "code", "type": "text", "length": 4},
        {"name": "description", "type": "text", "length": 16},
    ],
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Valid_Street_Type"),
)

OEM_VALID_UNIT = Dataset(
    fields=[{"name": "unit", "type": "text", "length": 8}],
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Valid_Unit"),
)

OEM_VALID_UNIT_TYPE = Dataset(
    fields=[
        {"name": "code", "type": "text", "length": 8},
        {"name": "description", "type": "text", "length": 16},
    ],
    path=os.path.join(database.LCOG_TILLAMOOK_ECD.path, "dbo.Valid_Unit_Type"),
)
