"""Dataset objects.

Extends the `dataset` submodule from the ETLAssist package.
"""
import os

from etlassist.dataset import *  # pylint: disable=wildcard-import, unused-wildcard-import

from . import database  # pylint: disable=relative-beyond-top-level
from . import path  # pylint: disable=relative-beyond-top-level


##TODO: Consider how to have these constructed when requested, rather than all at import.


ACCELA_PARCEL_OVERLAYS = [
    Dataset(
        # Skipping documenting the numerous fields.
        fields=[],
        path={
            "maint": os.path.join(
                path.REGIONAL_STAGING,
                "Accela\\Creswell\\Accela.gdb",
                "AccelaCreswellGIS",
            ),
            "pub": os.path.join(
                database.RLID_EXPORT_ACCELA.path,
                "dbo.Staging_Creswell_Accela_Parcel_Overlay",
            ),
        },
    ),
    Dataset(
        # Skipping documenting the numerous fields.
        fields=[],
        path={
            "maint": os.path.join(
                path.REGIONAL_STAGING, "Accela\\Lane\\Accela.gdb", "AccelaLCGIS"
            ),
            "pub": os.path.join(
                database.RLID_EXPORT_ACCELA.path,
                "dbo.Staging_Lane_Accela_Parcel_Overlay",
            ),
        },
    ),
]
ADDRESS_3RD_PARTY_USPS = Dataset(
    fields=[
        {"name": "address", "type": "text", "length": 128, "is_id": True},
        {"name": "city", "type": "text", "length": 30, "is_id": True},
        {"name": "exists_usps_exact", "type": "long"},
        {"name": "exists_usps_ignore_unit", "type": "long"},
    ],
    path=os.path.join(database.RLID_STAGING.path, "dbo.Address_QA_USPS"),
)
ADDRESS_ASSESS_TAX_INFO = Dataset(
    fields=[
        {"name": "geofeat_id", "type": "long", "is_id": True},
        {"name": "tax_code_overlay", "type": "text", "length": 6},
        {"name": "maptaxlot", "type": "text", "length": 13},
        {"name": "account", "type": "text", "length": 7},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.Assess_Tax_Info"),
)
ADDRESS_ISSUES = Dataset(
    fields=[
        {"name": "site_address_gfid", "type": "guid", "is_id": True},
        {"name": "geofeat_id", "type": "long"},
        {"name": "concat_address", "type": "text", "length": 128},
        {"name": "city_name", "type": "text", "length": 32},
        {"name": "description", "type": "text", "length": 128, "is_id": True},
        {"name": "update_publication", "type": "short"},
        {"name": "maint_notes", "type": "text", "length": 128},
        {"name": "maint_init_date", "type": "date"},
    ],
    geometry_type="point",
    path=os.path.join(database.ADDRESSING.path, "dbo.Issues"),
)
ADDRESS_POSTAL_INFO = Dataset(
    fields=[
        {"name": "geofeat_id", "type": "long", "is_id": True},
        {"name": "is_matched", "type": "text", "length": 1},
        {"name": "is_matched_no_unit", "type": "text", "length": 1},
        {"name": "address_type", "type": "text", "length": 1},
        {"name": "address_type_desc", "type": "text", "length": 43},
        {"name": "zip_code_overlay", "type": "text", "length": 5},
        {"name": "zip_code", "type": "text", "length": 5},
        {"name": "plus_four_code", "type": "text", "length": 4},
        {"name": "zip_type", "type": "text", "length": 1},
        {"name": "zip_type_desc", "type": "text", "length": 43},
        {"name": "delivery_point_code", "type": "text", "length": 3},
        {"name": "carrier_route", "type": "text", "length": 4},
        {"name": "lot_number", "type": "text", "length": 4},
        {"name": "lot_order", "type": "text", "length": 1},
        {"name": "is_cmra", "type": "text", "length": 1},
        {"name": "is_vacant", "type": "text", "length": 1},
        {"name": "has_mail_service", "type": "text", "length": 1},
        {"name": "issues", "type": "text", "length": 256},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.Postal_Info"),
)
ADDRESS_WORKFILE = Dataset(
    fields=[
        {"name": "geofeat_id", "type": "long", "is_id": True},
        # M+4: ADDRESS ("Delivery Address" button).
        {"name": "concat_address", "type": "text", "length": 128},
        # M+4: STREET NUMBER ("Number" button).
        {"name": "house", "type": "text", "length": 16},
        {"name": "mp4_house", "type": "text", "length": 16},
        # M+4: PRE DIRECTION.
        {"name": "pre_direction_code", "type": "text", "length": 2},
        {"name": "mp4_pre_direction_code", "type": "text", "length": 2},
        # M+4: STREET NAME ("Name" button).
        {"name": "street_name", "type": "text", "length": 30},
        {"name": "mp4_street_name", "type": "text", "length": 30},
        # M+4: STREET SUFFIX ("Suffix" button).
        {"name": "street_type_code", "type": "text", "length": 5},
        {"name": "mp4_street_type_code", "type": "text", "length": 5},
        # M+4: SUITE NAME.
        {"name": "unit_type_code", "type": "text", "length": 5},
        {"name": "mp4_unit_type_code", "type": "text", "length": 5},
        # M+4: SUITE NUMBER.
        {"name": "unit_id", "type": "text", "length": 5},
        {"name": "mp4_unit_id", "type": "text", "length": 5},
        # M+4: CITY.
        {"name": "city_name", "type": "text", "length": 30},
        {"name": "mp4_city_name", "type": "text", "length": 30},
        # M+4: STATE.
        {"name": "state_code", "type": "text", "length": 2},
        # M+4: ADDRESS_TYPE.
        {"name": "address_type", "type": "text", "length": 1},
        # M+4: ZIP.
        {"name": "zip_code", "type": "text", "length": 5},
        # M+4: PLUS4.
        {"name": "plus_four_code", "type": "text", "length": 4},
        # M+4: ZIP TYPE.
        {"name": "zip_type", "type": "text", "length": 1},
        # M+4: DELIVERY POINT.
        {"name": "delivery_point_code", "type": "text", "length": 3},
        # M+4: CRRT.
        {"name": "carrier_route", "type": "text", "length": 4},
        # M+4: LOT NUMBER.
        {"name": "lot_number", "type": "text", "length": 4},
        # M+4: LOT ORDER.
        {"name": "lot_order", "type": "text", "length": 1},
        # M+4: ADDRESS ERROR.
        {"name": "error_code", "type": "text", "length": 1},
        # M+4: Result Code.
        {"name": "result_code", "type": "text", "length": 64},
    ],
    path=os.path.join(path.REGIONAL_STAGING, "MailersPlus4", "Address.mdb", "Workfile"),
)
ADDRESS_WORKFILE_3RD_PARTY = Dataset(
    fields=ADDRESS_WORKFILE.fields,
    path=os.path.join(
        path.REGIONAL_STAGING, "MailersPlus4\\Address_Third_Party.mdb", "Workfile"
    ),
)
AMBULANCE_SERVICE_AREA = Dataset(
    fields=[
        {"name": "asacode", "type": "text", "length": 2, "is_id": True},
        {"name": "asa_num", "type": "long", "is_id": True},
        {"name": "asa", "type": "text", "length": 50},
        {"name": "providercode", "type": "text", "length": 3},
        {"name": "provider", "type": "text", "length": 40},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.AmbulanceServiceArea"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.AmbulanceServiceArea"),
    },
)
ANNEXATION_HISTORY = Dataset(
    fields=[
        {"name": "annexcity", "type": "text", "length": 3},
        {"name": "annexnum", "type": "text", "length": 50},
        {"name": "annexdate", "type": "date"},
        {"name": "effecdate", "type": "date"},
        {"name": "annexyearn", "type": "short"},
        {"name": "annexyear", "type": "text", "length": 4},
    ],
    geometry_type="polygon",
    path={
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.AnnexHist"),
        "inserts": [
            os.path.join(database.LCOGGEO.path, "lcogadm.CobAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.CotAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.CreAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.DunAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.EugAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.FloAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.JunAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.LowAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.OakAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.SprAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.VenAnxHist"),
            os.path.join(database.LCOGGEO.path, "lcogadm.WesAnxHist"),
        ],
    },
)
BIKE_FACILITY = Dataset(
    fields=[
        {
            "name": "geofeature_id",
            "type": "guid",
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        {"name": "bike_segid", "type": "long", "tags": ["maint", "pub"]},
        {"name": "eug_id", "type": "long", "tags": ["maint", "pub"]},
        {"name": "name", "type": "text", "length": 64, "tags": ["maint", "pub"]},
        {"name": "ftype", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "ftypedes", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "lane_type", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "lane_typedes", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "status", "type": "text", "length": 8, "tags": ["maint", "pub"]},
        {"name": "maint", "type": "text", "length": 8, "tags": ["maint", "pub"]},
        {"name": "source", "type": "text", "length": 64, "tags": ["maint", "pub"]},
        {"name": "easement", "type": "text", "length": 32, "tags": ["maint", "pub"]},
        {"name": "lane_num", "type": "short", "tags": ["maint", "pub"]},
        {
            "name": "lane_placement",
            "type": "text",
            "length": 10,
            "tags": ["maint", "pub"],
        },
        {"name": "created_date", "type": "date", "tags": ["maint"]},
        {"name": "created_user", "type": "text", "length": 32, "tags": ["maint"]},
        {"name": "last_edited_date", "type": "date", "tags": ["maint"]},
        {"name": "last_edited_user", "type": "text", "length": 32, "tags": ["maint"]},
    ],
    geometry_type="polyline",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.BikeFacility"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.BikeFacility"),
    },
)
BRIDGE = Dataset(
    fields=[
        {
            "name": "geofeature_id",
            "type": "guid",
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        # bridge_id too ambiguous: replaced by state_bridge_id in pub.
        {"name": "bridge_id", "type": "text", "length": 6, "tags": ["maint"]},
        {"name": "state_bridge_id", "type": "text", "length": 6, "tags": ["pub"]},
        # No idea what this refers to; omitting.
        {"name": "gis_no", "type": "long", "tags": ["maint"]},
        {"name": "local_name", "type": "text", "length": 64, "tags": ["maint", "pub"]},
        {
            "name": "covered_bridge",
            "type": "text",
            "length": 1,
            "tags": ["maint", "pub"],
        },
        {"name": "lifeline", "type": "long", "tags": ["maint", "pub"]},
        {"name": "psap_id", "type": "double", "tags": ["maint", "pub"]},
        {"name": "route_id", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "route_seq", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "source", "type": "text", "length": 64, "tags": ["maint", "pub"]},
        {"name": "inspect_form", "type": "text", "length": 256, "tags": ["maint"]},
        {"name": "comments", "type": "text", "length": 64, "tags": ["maint", "pub"]},
        {"name": "angle", "type": "short", "tags": ["maint", "pub"]},
        {"name": "created_date", "type": "date", "tags": ["maint"]},
        {"name": "created_user", "type": "text", "length": 32, "tags": ["maint"]},
        {"name": "last_edited_date", "type": "date", "tags": ["maint"]},
        {"name": "last_edited_user", "type": "text", "length": 32, "tags": ["maint"]},
    ],
    geometry_type="point",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.Bridge"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.Bridge"),
    },
)
BUILDING = Dataset(
    fields=[
        {
            "name": "geofeature_id",
            "type": "guid",
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        ##TODO: Eventually drop.
        {"name": "globalid", "type": "guid", "tags": ["maint"]},
        {"name": "height", "type": "short", "tags": ["maint", "pub"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.Building"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.Building"),
    },
)
CITY = Dataset(
    fields=[
        {"name": "citynameabbr", "type": "text", "length": 3, "is_id": True},
        {"name": "cityname", "type": "text", "length": 16},
        {"name": "isinccity", "type": "text", "length": 1},
        {"name": "ismailcity", "type": "text", "length": 1},
    ],
    path=os.path.join(database.LCOGGEO.path, "lcogadm.City"),
)
CITY_COUNCILOR = Dataset(
    fields=[
        {"name": "city", "type": "text", "length": 16, "is_id": True},
        {"name": "ward", "type": "text", "length": 3, "is_id": True},
        {"name": "councilor", "type": "text", "length": 64},
    ],
    path=os.path.join(database.LCOGGEO.path, "lcogadm.CityCouncilor"),
)
CITY_WARD = Dataset(
    fields=[
        {"name": "wardcity", "type": "text", "length": 3},
        {"name": "ward", "type": "text", "length": 3, "is_id": True},
        {"name": "councilor", "type": "text", "length": 50},
    ],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.CityWard"),
)
COMPARABLE_SALE_TAXLOT = Dataset(
    fields=[{"name": "maptaxlot", "type": "text", "length": 13, "is_id": True}],
    geometry_type="polygon",
    path={
        "source": os.path.join(
            database.RLID_WEBAPP_V3.path, "dbo.WebApp_Comp_Sales_Comp"
        ),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.ComparableSaleTaxlot"),
    },
)
COT_CITY_WARD = Dataset(
    fields=[{"name": "ward", "type": "text", "length": 3, "is_id": True}],
    geometry_type="polygon",
    path=os.path.join(database.LCOGGEO.path, "lcogadm.CotCityWard"),
)
COUNTY_BOUNDARY = Dataset(
    fields=[{"name": "name", "type": "text", "length": 16, "is_id": True}],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.CountyBoundary"),
)
COUNTY_COMMISSIONER_DISTRICT = Dataset(
    fields=[
        {"name": "commrdist", "type": "text", "length": 1, "is_id": True},
        {"name": "cmdistname", "type": "text", "length": 16},
        {"name": "commrname", "type": "text", "length": 64},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(
            database.LCOGGEO.path, "lcogadm.CountyCommissionerDistrict"
        ),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.CountyCommissionerDist"),
    },
)
ELECTION_PRECINCT = Dataset(
    fields=[{"name": "precntnum", "type": "text", "length": 6, "is_id": True}],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.ElectionPrecinct"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.ElectionPrecinct"),
    },
)
ELEMENTARY_SCHOOL_AREA = Dataset(
    fields=[
        {"name": "attend", "type": "text", "length": 4, "is_id": True},
        {"name": "elem_school", "type": "text", "length": 20},
        {"name": "district", "type": "text", "length": 3},
        {"name": "names", "type": "text", "length": 24},
        {"name": "oldcodes", "type": "text", "length": 3},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.Elementary_School_Areas"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.ElementarySchoolArea"),
    },
)
ELEMENTARY_SCHOOL_LINE = Dataset(
    fields=[],
    geometry_type="polyline",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.ElementarySchoolLine"),
)
##TODO: Change emergency_service_number to long.
EMERGENCY_SERVICE_NUMBER = Dataset(
    fields=[
        {"name": "emergency_service_number", "type": "text", "length": 2},
        {"name": "city_limits", "type": "text", "length": 3},
        {"name": "law_provider", "type": "text", "length": 32},
        {"name": "fire_district", "type": "text", "length": 3},
        {"name": "fire_coverage_description", "type": "text", "length": 128},
        {"name": "asa_code", "type": "text", "length": 2},
        {"name": "psap_code", "type": "text", "length": 2},
        {"name": "comments", "type": "text", "length": 128},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.EmergencyServiceNumber"),
)
EMERGENCY_SERVICE_ZONE = Dataset(
    fields=[
        {"name": "emergency_service_number", "type": "long", "is_id": True},
        {"name": "law_provider", "type": "text", "length": 32},
        {
            "name": "fire_coverage_description",
            "type": "text",
            "length": 128,
            "is_id": True,
        },
        {"name": "asa_code", "type": "text", "length": 2, "is_id": True},
        {"name": "asa_name", "type": "text", "length": 64},
        {"name": "psap_code", "type": "text", "length": 2, "is_id": True},
        {"name": "psap_name", "type": "text", "length": 32},
    ],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.EmergencyServiceZone"),
)
EPUD_SUBDISTRICT = Dataset(
    fields=[
        {"name": "boardid_", "type": "text", "length": 254, "tags": ["source"]},
        {"name": "boardid", "type": "long", "tags": ["pub"], "is_id": True},
        {"name": "boardmbr", "type": "text", "length": 64, "tags": ["source", "pub"]},
    ],
    geometry_type="polygon",
    path={
        "source": os.path.join(
            path.LCOG_GIS_PROJECTS, "UtilityDistricts\\epud", "epudsvc.shp"
        ),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.EPUDSubdistrict"),
    },
)
##TODO: Deprecate this somehow (used in a view).
EUGENE_GEODART_EXTENT = Dataset(
    fields=[{"name": "is_inside", "type": "text", "length": 1, "is_id": True}],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.Eugene_GeoDARTExtent"),
)
EWEB_COMMISSIONER = Dataset(
    fields=[
        {"name": "city_council_ward", "type": "text", "length": 8, "is_id": True},
        {"name": "eweb_commissioner_name", "type": "text", "length": 64},
    ],
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.EWEBCommissioner"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.EWEBCommissioner"),
    },
)
FACILITY = Dataset(
    fields=[
        {"name": "label", "type": "text", "length": 32, "tags": ["maint", "pub"]},
        {
            "name": "label_full",
            "type": "text",
            "length": 128,
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        {"name": "label_add", "type": "text", "length": 150, "tags": ["maint"]},
        {"name": "type", "type": "text", "length": 8, "tags": ["maint", "pub"]},
        {"name": "type_full", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "eug_amenity", "type": "text", "length": 1, "tags": ["maint"]},
        {"name": "dist", "type": "text", "length": 3, "tags": ["maint"]},
        {"name": "geofeat_id", "type": "double", "tags": ["maint"]},
        {"name": "address_intid", "type": "long", "tags": ["pub"]},
        {"name": "address_uuid", "type": "guid", "tags": ["pub"]},
        {"name": "polygonid", "type": "long", "tags": ["maint"]},
        {"name": "scale", "type": "float", "tags": ["maint"]},
        {"name": "angle", "type": "float", "tags": ["maint"]},
        {"name": "xcoord", "type": "double", "tags": ["maint"]},
        {"name": "ycoord", "type": "double", "tags": ["maint"]},
        {"name": "x_coordinate", "type": "double", "tags": ["pub"], "is_id": True},
        {"name": "y_coordinate", "type": "double", "tags": ["pub"], "is_id": True},
        {"name": "longitude", "type": "double", "tags": ["pub"]},
        {"name": "latitude", "type": "double", "tags": ["pub"]},
    ],
    geometry_type="point",
    path={
        "maint": os.path.join(path.REGIONAL_DATA, "base\\Facilities.gdb", "Facilities"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.Facility"),
    },
)
FIRE_PROTECTION_AREA = Dataset(
    fields=[
        # Core attributes.
        {
            "name": "fireprotprov",
            "type": "text",
            "length": 3,
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        {"name": "fpprovname", "type": "text", "length": 50, "tags": ["pub"]},
        {"name": "fireprottype", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "fptypename", "type": "text", "length": 50, "tags": ["pub"]},
        {"name": "dateformed", "type": "date", "tags": ["maint", "pub"]},
        {"name": "taxdist", "type": "text", "length": 1, "tags": ["maint", "pub"]},
        # Extended attributes.
        {"name": "contact_phone", "type": "text", "length": 16, "tags": ["pub"]},
        {"name": "contact_email", "type": "text", "length": 64, "tags": ["pub"]},
        {
            "name": "contact_mailing_address",
            "type": "text",
            "length": 128,
            "tags": ["pub"],
        },
        {"name": "website_link", "type": "text", "length": 64, "tags": ["pub"]},
        # Maintenance attributes.
        {"name": "notes", "type": "text", "length": 50, "tags": ["maint"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.FireProtectionArea"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.FireProtectionArea"),
    },
)
FIRE_PROTECTION_PROVIDER = Dataset(
    fields=[
        {"name": "provider_code", "type": "text", "length": 3, "is_id": True},
        {"name": "provider_name", "type": "text", "length": 32},
        {"name": "contact_phone", "type": "text", "length": 16},
        {"name": "contact_email", "type": "text", "length": 64},
        {"name": "contact_mailing_address", "type": "text", "length": 128},
        {"name": "website_link", "type": "text", "length": 64},
        {"name": "created_date", "type": "date"},
        {"name": "last_updated_date", "type": "date"},
    ],
    path=os.path.join(database.LCOGGEO.path, "lcogadm.FireProtectionProvider"),
)
HIGH_SCHOOL_AREA = Dataset(
    fields=[
        {"name": "attend", "type": "text", "length": 4, "is_id": True},
        {"name": "high_school", "type": "text", "length": 20},
        {"name": "district", "type": "text", "length": 3},
        {"name": "names", "type": "text", "length": 24},
        {"name": "oldcodes", "type": "text", "length": 3},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.High_School_Areas"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.HighSchoolArea"),
    },
)
HIGH_SCHOOL_LINE = Dataset(
    fields=[],
    geometry_type="polyline",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.HighSchoolLine"),
)
HOUSE_SUFFIX = Dataset(
    fields=[
        {"name": "code", "type": "text", "length": 4},
        {"name": "description", "type": "text", "length": 16},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.HouseSuffix"),
)
HYDRANT = Dataset(
    fields=[{"name": "owner", "type": "text", "length": 50}],
    geometry_type="point",
    path=os.path.join(path.REGIONAL_DATA, "base\\Facilities.gdb", "Hydrants"),
)
INCORPORATED_CITY_LIMITS = Dataset(
    fields=[
        {"name": "inccityabbr", "type": "text", "length": 3, "is_id": True},
        {"name": "inccityname", "type": "text", "length": 30},
    ],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.IncCityLimits"),
)
LAND_USE_AREA = Dataset(
    fields=[
        # Core attributes.
        {
            "name": "maptaxlot",
            "type": "text",
            "length": 13,
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        {"name": "maptaxlot_hyphen", "type": "text", "length": 17, "tags": ["pub"]},
        {"name": "mapnumber", "type": "text", "length": 20, "tags": ["pub"]},
        {"name": "taxlot", "type": "text", "length": 5, "tags": ["pub"]},
        {"name": "floor", "type": "short", "tags": ["maint"]},
        {"name": "usecode", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "usecodedes", "type": "text", "length": 256, "tags": ["pub"]},
        {"name": "landuse", "type": "long", "tags": ["maint", "pub"], "is_id": True},
        {"name": "landusedes", "type": "text", "length": 256, "tags": ["pub"]},
        # Extended attributes.
        {"name": "landusecount", "type": "long", "tags": ["pub"]},
        {"name": "units", "type": "short", "tags": ["maint", "pub"]},
        # Maintenance attributes.
        {"name": "editwhen", "type": "date", "tags": ["maint"]},
        {"name": "editwho", "type": "text", "length": 32, "tags": ["maint"]},
        {"name": "editwhat", "type": "text", "length": 64, "tags": ["maint"]},
        {"name": "editsource", "type": "text", "length": 64, "tags": ["maint"]},
        {"name": "edittrust", "type": "text", "length": 8, "tags": ["maint"]},
        {"name": "fieldverif", "type": "date", "tags": ["maint"]},
        # City attributes.
        {"name": "geocity", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "yearanx", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "ugb", "type": "text", "length": 3, "tags": ["pub"]},
        # Planning & zoning attributes.
        {"name": "greenwy", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "neighbor", "type": "text", "length": 2, "tags": ["pub"]},
        # Public safety attributes.
        {"name": "firedist", "type": "text", "length": 3, "tags": ["pub"]},
        # Election attributes.
        {"name": "lcczone", "type": "text", "length": 1, "tags": ["pub"]},
        # Education attributes.
        {"name": "elem", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "middle", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "high", "type": "text", "length": 4, "tags": ["pub"]},
        # Transportation attributes.
        {"name": "ltddist", "type": "text", "length": 1, "tags": ["pub"]},
        # Natural attributes.
        {"name": "flood", "type": "text", "length": 4, "tags": ["pub"]},
        # Census attributes.
        {"name": "ctract", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "blockgr", "type": "text", "length": 4, "tags": ["pub"]},
        # Geometry attributes.
        {"name": "acres", "type": "double", "tags": ["pub"]},
        {"name": "xcoord", "type": "double", "tags": ["pub"]},
        {"name": "ycoord", "type": "double", "tags": ["pub"]},
        {"name": "longitude", "type": "double", "tags": ["pub"]},
        {"name": "latitude", "type": "double", "tags": ["pub"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.LandUseArea"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.LandUseArea"),
    },
)
LAND_USE_CODES_DETAILED = Dataset(
    fields=[
        {"name": "landuse", "type": "long", "is_id": True},
        {"name": "landusec", "type": "text", "length": 4},
        {"name": "ludesc", "type": "text", "length": 256},
        {"name": "shortdesc", "type": "text", "length": 80},
        {"name": "usecode", "type": "text", "length": 1},
        {"name": "usagenotes", "type": "text", "length": 128},
    ],
    path={
        "maint": os.path.join(database.LCOGGEO.path, "dbo.LU_LandUse"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.LanduseCodesDetailed"),
    },
)
LAND_USE_CODES_USE_CODES = Dataset(
    fields=[
        {"name": "usecode", "type": "text", "length": 1, "is_id": True},
        {"name": "ucname", "type": "text", "length": 32},
        {"name": "ucclass", "type": "text", "length": 32},
    ],
    path={
        "maint": os.path.join(database.LCOGGEO.path, "dbo.LU_UseCodes"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.LanduseCodesUseCodes"),
    },
)
LCC_BOARD_ZONE = Dataset(
    fields=[
        {"name": "lccbrdzone", "type": "text", "length": 1, "is_id": True},
        {"name": "lccbrdrep", "type": "text", "length": 64},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.LCCBoardZone"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.LCCBoardZone"),
    },
)
MAILING_CITY_AREA = Dataset(
    fields=[
        {"name": "city_code", "type": "text", "length": 3, "is_id": True},
        {"name": "city_name", "type": "text", "length": 32},
    ],
    geometry_type="polygon",
    path=os.path.join(database.ADDRESSING.path, "dbo.MailingCityArea"),
)
METADATA_DATASET_UPDATE = Dataset(
    fields=[
        {"name": "update_id", "type": "guid", "is_id": True},
        {"name": "dataset_name", "type": "text", "length": 64},
        {"name": "checked", "type": "date"},
        {"name": "deleted_row_count", "type": "long"},
        {"name": "inserted_row_count", "type": "long"},
        {"name": "altered_row_count", "type": "long"},
        {"name": "unchanged_row_count", "type": "long"},
        {"name": "total_row_count", "type": "long"},
        {"name": "updated_row_count", "type": "long"},
    ],
    path={
        "RLIDGeo": os.path.join(database.RLIDGEO.path, "dbo.Metadata_Dataset_Update"),
        "RLIDGeo_LastLoad": os.path.join(
            database.RLIDGEO_LASTLOAD.path, "dbo.Metadata_Dataset_Update"
        ),
    },
)
METRO_PLAN_BOUNDARY = Dataset(
    fields=[{"name": "planjuris", "type": "text", "length": 3, "is_id": True}],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.MetroPlanDes"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.MetroPlanBoundary"),
    },
)
MIDDLE_SCHOOL_AREA = Dataset(
    fields=[
        {"name": "attend", "type": "text", "length": 4, "is_id": True},
        {"name": "middle_school", "type": "text", "length": 20},
        {"name": "district", "type": "text", "length": 3},
        {"name": "names", "type": "text", "length": 24},
        {"name": "oldcodes", "type": "text", "length": 3},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.Middle_School_Areas"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.MiddleSchoolArea"),
    },
)
MIDDLE_SCHOOL_LINE = Dataset(
    fields=[],
    geometry_type="polyline",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.MiddleSchoolLine"),
)
MSAG_RANGE = Dataset(
    fields=[
        {"name": "msag_id", "type": "guid", "is_id": True},
        {"name": "emergency_service_number", "type": "long"},
        {"name": "parity_code", "type": "text", "length": 1},
        {"name": "parity", "type": "text", "length": 8},
        {"name": "from_house_number", "type": "long"},
        {"name": "to_house_number", "type": "long"},
        {"name": "pre_direction_code", "type": "text", "length": 2},
        {"name": "street_name", "type": "text", "length": 32},
        {"name": "street_type_code", "type": "text", "length": 5},
        {"name": "city_code", "type": "text", "length": 3},
        {"name": "city_name", "type": "text", "length": 32},
        {"name": "effective_date", "type": "date"},
        {"name": "expiration_date", "type": "date"},
    ],
    geometry_type="polygon",
    path={
        "current": os.path.join(database.ETL_LOAD_A.path, "dbo.MSAG_Range_Current"),
        "master": os.path.join(database.RLIDGEO.path, "dbo.MSAG_Range"),
    },
)
NODAL_DEVELOPMENT_AREA = Dataset(
    fields=[
        {"name": "nodearea", "type": "text", "length": 2, "is_id": True},
        {"name": "nodename", "type": "text", "length": 50},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.NodalDevelopmentArea"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.NodalDevelopmentArea"),
    },
)
ODOT_BRIDGE_INVENTORY = Dataset(
    fields=[
        {"name": "bridge_id", "type": "text", "length": 8, "is_id": True},
        {"name": "hwynumb", "type": "text", "length": 24},
        {"name": "mp", "type": "double"},
        {"name": "bridge_nam", "type": "text", "length": 64},
        {"name": "carries", "type": "text", "length": 32},
        {"name": "crosses", "type": "text", "length": 32},
        {"name": "length_ft", "type": "double"},
        {"name": "width_ft", "type": "double"},
        {"name": "lanes", "type": "long"},
        {"name": "main_spans", "type": "long"},
        {"name": "appr_spans", "type": "long"},
        {"name": "design", "type": "text", "length": 24},
        {"name": "material", "type": "text", "length": 24},
        {"name": "service_on", "type": "text", "length": 36},
        {"name": "service_un", "type": "text", "length": 36},
        {"name": "owner", "type": "text", "length": 24},
        {"name": "nbi_bridge", "type": "text", "length": 1},
        {"name": "nhs_ind", "type": "text", "length": 1},
        {"name": "posting", "type": "text", "length": 24},
        {"name": "sd_fo", "type": "text", "length": 2},
        {"name": "rtgoal_shv", "type": "text", "length": 16},
        {"name": "suf_rating", "type": "double"},
        {"name": "horz_clr_f", "type": "double"},
        {"name": "movable_b", "type": "text", "length": 3},
        {"name": "detour_len", "type": "double"},
        {"name": "insp_date", "type": "date"},
        {"name": "custodian", "type": "text", "length": 24},
        {"name": "dgn_load", "type": "text", "length": 24},
        {"name": "weight_res", "type": "text", "length": 256},
        {"name": "appr_type", "type": "text", "length": 24},
        {"name": "appr_mat", "type": "text", "length": 24},
        {"name": "deck_rd_wd", "type": "double"},
        {"name": "lanes_un", "type": "long"},
        {"name": "traf_dir", "type": "text", "length": 32},
        {"name": "year", "type": "long"},
        {"name": "status", "type": "text", "length": 128},
        {"name": "nav_cntl", "type": "text", "length": 24},
        {"name": "brkey", "type": "text", "length": 16},
        {"name": "struc_typ", "type": "text", "length": 24},
        {"name": "dkrating", "type": "text", "length": 14},
        {"name": "suprating", "type": "text", "length": 14},
        {"name": "subrating", "type": "text", "length": 14},
        {"name": "culvrating", "type": "text", "length": 14},
        {"name": "scourcrit", "type": "text", "length": 22},
        {"name": "timber_sub", "type": "text", "length": 19},
        {"name": "fill", "type": "long"},
        {"name": "hc1", "type": "text", "length": 10},
        {"name": "hc2", "type": "text", "length": 10},
        {"name": "spans", "type": "text", "length": 192},
        {"name": "descriptio", "type": "text", "length": 96},
        {"name": "railrating", "type": "text", "length": 21},
        {"name": "railcond", "type": "text", "length": 4},
        {"name": "railmat", "type": "text", "length": 20},
        {"name": "nchrp_230", "type": "text", "length": 1},
        {"name": "ds", "type": "long"},
        {"name": "fc", "type": "text", "length": 23},
        {"name": "vulnerability_index", "type": "text", "length": 22},
        {"name": "odot_reg", "type": "text", "length": 8},
        {"name": "odot_dist", "type": "long"},
        {"name": "cnty_name", "type": "text", "length": 16},
        {"name": "city_name", "type": "text", "length": 16},
        {"name": "lrs_key", "type": "text", "length": 16},
        {"name": "lat", "type": "double"},
        {"name": "long", "type": "double"},
        {"name": "gis_prc_dt", "type": "date"},
        {"name": "effectv_dt", "type": "long"},
    ],
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.ODOTBridge"),
        # "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.ODOT_Bridge_Inventory"),
    },
)
PLAN_DESIGNATION_CITY = Dataset(
    fields=[
        {"name": "planjuris", "type": "text", "length": 3, "is_id": True},
        {"name": "plandes", "type": "text", "length": 10, "is_id": True},
        {"name": "plandesnam", "type": "text", "length": 50},
        {"name": "finalorder", "type": "text", "length": 8, "is_id": True},
    ],
    geometry_type="polygon",
    path={
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.PlanDesignation_City"),
        "inserts": [
            os.path.join(database.LCOGGEO.path, "lcogadm.CobPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.CotPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.CresPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.DunPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.FloPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.JunPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.LowPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.MetroPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.OakPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.VenPlanDes"),
            os.path.join(database.LCOGGEO.path, "lcogadm.WesPlanDes"),
        ],
    },
)
PLAN_DESIGNATION_COUNTY = Dataset(
    fields=[
        {
            "name": "planjuris",
            "type": "text",
            "length": 3,
            "tags": ["pub"],
            "is_id": True,
        },
        {
            "name": "plandes",
            "type": "text",
            "length": 5,
            "tags": ["pub"],
            "is_id": True,
        },
        {"name": "ZONE_", "type": "text", "length": 10, "tags": ["maint"]},
        {"name": "plandesnam", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "ZONE_NAME", "type": "text", "length": 100, "tags": ["maint"]},
        {
            "name": "finalorder",
            "type": "text",
            "length": 8,
            "tags": ["pub"],
            "is_id": True,
        },
        {"name": "PLOT_NUM", "type": "text", "length": 5, "tags": ["maint"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(path.REGIONAL_STAGING, "LCZoning", "lcplandes.shp"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.PlanDesignation_County"),
    },
)
PLAN_DESIGNATION = Dataset(
    fields=[
        {"name": "plandes_id", "type": "long"},
        {"name": "planjuris", "type": "text", "length": 3, "is_id": True},
        {"name": "plandes", "type": "text", "length": 10, "is_id": True},
        {"name": "plandesnam", "type": "text", "length": 50},
        {"name": "finalorder", "type": "text", "length": 8, "is_id": True},
    ],
    geometry_type="polygon",
    path={
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.PlanDesignation"),
        "inserts": [
            PLAN_DESIGNATION_CITY.path("pub"),
            PLAN_DESIGNATION_COUNTY.path("pub"),
        ],
    },
)
PLAT = Dataset(
    fields=[
        {
            "name": "platname",
            "type": "text",
            "length": 128,
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        {
            "name": "docnumber",
            "type": "text",
            "length": 16,
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        {"name": "agencydocn", "type": "long", "tags": ["maint", "pub"], "is_id": True},
        # Shouldn"t be an ID member, but I can"t control multiple dates recorded.
        {"name": "daterecord", "type": "date", "tags": ["maint", "pub"], "is_id": True},
        # No idea what these codes are for.
        {"name": "sourcetype", "type": "text", "length": 16, "tags": ["maint"]},
        # No idea what these numbers are for.
        {"name": "source", "type": "text", "length": 32, "tags": ["maint"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.Plats"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.Plat"),
    },
)
PLSS_DLC = Dataset(
    fields=[
        {"name": "name", "type": "text", "length": 250, "tags": ["maint"]},
        {
            "name": "dlcname",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
            "is_id": True,
        },
        {"name": "trs", "type": "text", "length": 8, "tags": ["maint", "pub"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.PLSSDLCs"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.PLSSDLC"),
    },
)
PLSS_QUARTER_SECTION = Dataset(
    fields=[
        {"name": "trsq", "type": "long", "tags": ["maint", "pub"], "is_id": True},
        {"name": "trs", "type": "long", "tags": ["maint", "pub"]},
        {"name": "tnum", "type": "short", "tags": ["maint", "pub"]},
        {"name": "rnum", "type": "short", "tags": ["maint", "pub"]},
        {"name": "sec", "type": "short", "tags": ["maint", "pub"]},
        {"name": "qtr", "type": "short", "tags": ["maint", "pub"]},
        {"name": "xcoord", "type": "double", "tags": ["maint"]},
        {"name": "ycoord", "type": "double", "tags": ["maint"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.PLSSQuarterSections"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.PLSSQuarterSection"),
    },
)
PLSS_SECTION = Dataset(
    fields=[
        {"name": "trs", "type": "long", "tags": ["maint", "pub"], "is_id": True},
        {"name": "trsalt", "type": "text", "length": 16, "tags": ["pub"]},
        {"name": "tnum", "type": "short", "tags": ["maint", "pub"]},
        {"name": "rnum", "type": "short", "tags": ["maint", "pub"]},
        {"name": "sec", "type": "short", "tags": ["maint", "pub"]},
        {"name": "tr", "type": "short", "tags": ["maint"]},
        {"name": "xcoord", "type": "double", "tags": ["maint"]},
        {"name": "ycoord", "type": "double", "tags": ["maint"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.PLSSSections"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.PLSSSection"),
    },
)
PLSS_TOWNSHIP = Dataset(
    fields=[
        {"name": "tr", "type": "short", "tags": ["maint", "pub"], "is_id": True},
        {"name": "tnum", "type": "short", "tags": ["pub"]},
        {"name": "rnum", "type": "short", "tags": ["pub"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.PLSSTownships"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.PLSSTownship"),
    },
)
PROPOSED_STREET_NAME = Dataset(
    fields=[
        {"name": "pre_direction_code", "type": "text", "length": 2},
        {"name": "street_name", "type": "text", "length": 32},
        {"name": "street_type_code", "type": "text", "length": 5},
        {"name": "city_name", "type": "text", "length": 32},
        {"name": "jurisdiction_name", "type": "text", "length": 32},
        {"name": "subdivision_name", "type": "text", "length": 64},
        {"name": "proposal_type", "type": "text", "length": 8},
        {"name": "proposal_date", "type": "date"},
        {"name": "requestor_name", "type": "text", "length": 64},
        {"name": "proposal_status", "type": "text", "length": 32},
        {"name": "approved_date", "type": "date"},
        {"name": "denied_reason", "type": "text", "length": 128},
        {"name": "display_in_rlid_flag", "type": "text", "length": 1},
        {"name": "memo", "type": "text", "length": 256},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.Proposed_StreetName"),
)
PSAP_AREA = Dataset(
    fields=[
        {"name": "psap_code", "type": "text", "length": 2, "is_id": True},
        {"name": "psap_name", "type": "text", "length": 32},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.PSAPArea"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.PSAPArea"),
    },
)
ROAD = Dataset(
    fields=[
        # Core road attributes.
        {
            "name": "road_segment_gfid",
            "type": "guid",
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        {"name": "seg_id", "type": "long", "tags": ["maint", "pub"]},
        {"name": "lcpwid", "type": "long", "tags": ["maint", "pub"]},
        {"name": "eugid", "type": "long", "tags": ["maint", "pub"]},
        {"name": "sprid", "type": "long", "tags": ["maint", "pub"]},
        {"name": "lcogid", "type": "long", "tags": ["maint", "pub"]},
        {"name": "airsname", "type": "text", "length": 38, "tags": ["maint", "pub"]},
        {"name": "rlidname", "type": "text", "length": 38, "tags": ["pub"]},
        {"name": "dir", "type": "text", "length": 2, "tags": ["maint", "pub"]},
        {"name": "name", "type": "text", "length": 30, "tags": ["maint", "pub"]},
        {"name": "type", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "sgname", "type": "text", "length": 64, "tags": ["maint"]},
        # Geocoding attributes.
        {"name": "l_ladd", "type": "long", "tags": ["maint", "pub"]},
        {"name": "l_hadd", "type": "long", "tags": ["maint", "pub"]},
        {"name": "r_ladd", "type": "long", "tags": ["maint", "pub"]},
        {"name": "r_hadd", "type": "long", "tags": ["maint", "pub"]},
        {"name": "mailcity", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "cityL", "type": "text", "length": 4, "tags": ["maint"]},
        {"name": "cityR", "type": "text", "length": 4, "tags": ["maint"]},
        {"name": "county", "type": "text", "length": 10, "tags": ["maint", "pub"]},
        {"name": "state", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "zipcode", "type": "text", "length": 5, "tags": ["pub"]},
        # Classification attributes.
        {"name": "fclass", "type": "text", "length": 8, "tags": ["maint", "pub"]},
        {"name": "fed_class", "type": "text", "length": 36, "tags": ["maint", "pub"]},
        {"name": "airsclass", "type": "text", "length": 8, "tags": ["maint", "pub"]},
        {"name": "cclass", "type": "short", "tags": ["pub"]},
        {"name": "paved", "type": "text", "length": 1, "tags": ["maint", "pub"]},
        {"name": "speed", "type": "short", "tags": ["maint", "pub"]},
        {"name": "speedfrwrd", "type": "short", "tags": ["maint"]},
        {"name": "speedback", "type": "short", "tags": ["maint"]},
        {"name": "snowroute", "type": "short", "tags": ["maint", "pub"]},
        # Jurisdiction & inventory attributes.
        {"name": "owner", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "maint", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "source", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "method", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "contributor", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        # Extended attributes.
        {"name": "ugbcity", "type": "text", "length": 3, "tags": ["maint", "pub"]},
        # Network definition attributes.
        {"name": "one_way", "type": "text", "length": 2, "tags": ["maint", "pub"]},
        {"name": "flow", "type": "text", "length": 4, "tags": ["maint"]},
        {"name": "fnode", "type": "long", "tags": ["pub"]},
        {"name": "tnode", "type": "long", "tags": ["pub"]},
        {"name": "f_zlev", "type": "short", "tags": ["maint", "pub"]},
        {"name": "t_zlev", "type": "short", "tags": ["maint", "pub"]},
        {"name": "optcost", "type": "double", "tags": ["pub"]},
        {"name": "ltd_a_cost", "type": "double", "tags": ["pub"]},
        {"name": "ltd_b_cost", "type": "double", "tags": ["pub"]},
        {"name": "ltd_c_cost", "type": "double", "tags": ["pub"]},
        {"name": "ltd_d_cost", "type": "double", "tags": ["pub"]},
        # Maintenance attributes.
        {"name": "created_date", "type": "date", "tags": ["maint"]},
        {"name": "created_user", "type": "text", "length": 32, "tags": ["maint"]},
        {"name": "last_edited_date", "type": "date", "tags": ["maint"]},
        {"name": "last_edited_user", "type": "text", "length": 32, "tags": ["maint"]},
    ],
    geometry_type="polyline",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.Street"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.Road"),
    },
)
ROAD_GENERAL_ARTERIAL = Dataset(
    fields=[
        # Core road attributes.
        {"name": "airsname", "type": "text", "length": 38},
        {"name": "rlidname", "type": "text", "length": 38},
        {"name": "dir", "type": "text", "length": 2},
        {"name": "name", "type": "text", "length": 30},
        {"name": "type", "type": "text", "length": 4},
        # Classification attributes.
        {"name": "fclass", "type": "text", "length": 8},
        {"name": "fed_class", "type": "text", "length": 36},
        {"name": "airsclass", "type": "text", "length": 8},
        {"name": "cclass", "type": "short"},
        # Extra attributes.
        {"name": "ugbcity", "type": "text", "length": 3},
        # Network definition attributes.
        {"name": "f_zlev", "type": "short"},
        {"name": "t_zlev", "type": "short"},
    ],
    geometry_type="polyline",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.RoadGeneralArterial"),
)
ROAD_HIGHWAY = Dataset(
    fields=[
        # Core road attributes.
        {"name": "airsname", "type": "text", "length": 38},
        {"name": "rlidname", "type": "text", "length": 38},
        {"name": "dir", "type": "text", "length": 2},
        {"name": "name", "type": "text", "length": 30},
        {"name": "type", "type": "text", "length": 4},
        # Classification attributes.
        {"name": "fclass", "type": "text", "length": 8},
        {"name": "fed_class", "type": "text", "length": 36},
        {"name": "airsclass", "type": "text", "length": 8},
        {"name": "cclass", "type": "short"},
        # Extra attributes.
        {"name": "ugbcity", "type": "text", "length": 3},
        # Network definition attributes.
        {"name": "f_zlev", "type": "short"},
        {"name": "t_zlev", "type": "short"},
    ],
    geometry_type="polyline",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.RoadHighway"),
)
ROAD_INTERSECTION = Dataset(
    fields=[
        # Core intersection attributes.
        {"name": "nodeid", "type": "long"},
        {"name": "intersection_name", "type": "text", "length": 128, "is_id": True},
        {"name": "intersection_name_reverse", "type": "text", "length": 128},
        {"name": "rlidname01", "type": "text", "length": 38},
        {"name": "rlidname02", "type": "text", "length": 38},
        {"name": "dir01", "type": "text", "length": 2},
        {"name": "name01", "type": "text", "length": 30},
        {"name": "type01", "type": "text", "length": 4},
        {"name": "dir02", "type": "text", "length": 2},
        {"name": "name02", "type": "text", "length": 30},
        {"name": "type02", "type": "text", "length": 4},
        # Geocoding attributes.
        {"name": "mailcity", "type": "text", "length": 16},
        # Extra attributes.
        {"name": "inccity", "type": "text", "length": 3},
        {"name": "inccity_name", "type": "text", "length": 32},
        {"name": "ugbcity", "type": "text", "length": 3},
        {"name": "ugbcity_name", "type": "text", "length": 32},
        {"name": "neighbor", "type": "text", "length": 2},
        {"name": "neighborhood_name", "type": "text", "length": 64},
        # Network definition attributes.
        {"name": "numzlevs", "type": "short"},
        {"name": "zlev01", "type": "short", "is_id": True},
        {"name": "zlev02", "type": "short", "is_id": True},
        # Geometry attributes.
        # Coordinates are IDs: road nodes not yet persistent, multiple crossings.
        ##TODO: Make road nodes persistent.
        {"name": "xcoord", "type": "double", "is_id": True},
        {"name": "ycoord", "type": "double", "is_id": True},
        {"name": "longitude", "type": "double"},
        {"name": "latitude", "type": "double"},
    ],
    geometry_type="point",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.RoadIntersection"),
)
SCHOOL_DISTRICT = Dataset(
    fields=[
        {"name": "district", "type": "text", "length": 3, "is_id": True},
        {"name": "names", "type": "text", "length": 24},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.School_Districts"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.SchoolDist"),
    },
)
SIMPLE_NETWORK = Dataset(
    fields=[],
    path=os.path.join(path.BUILT_RESOURCES, "Simple_Network", "Simple_Network_ND"),
)
"""Dataset: Representation of the Simple Network network dataset.

Configuration:
    Name: Simple_Network_ND.
    Version: 10.1.
    Model turns: Yes. Turn sources: <Global Turns>.
    Connectivity: Default.
    Model elevation: Using elevation fields (f_zlev, t_zlev).
    Attributes (all expressions Python).
        Length (cost - feet - double)
            From-To Value: !shape!
            To-From Value: !shape!
        Oneway (restriction - boolean)
            From-To Value: !one_way! in ("N", "NT", "TF", "T")
            To-From Value: !one_way! in ("N", "NT", "FT", "F")
    Travel mode: Ignore this.
    Driving directions: No.
    Build service area index: Yes (IMPORTANT).
"""
SIMPLE_NETWORK_EDGES = [
    Dataset(
        fields=[
            # Core road attributes.
            {"name": "road_segment_gfid", "type": "guid", "is_id": True},
            {"name": "airsname", "type": "text", "length": 38},
            {"name": "rlidname", "type": "text", "length": 38},
            {"name": "dir", "type": "text", "length": 2},
            {"name": "name", "type": "text", "length": 30},
            {"name": "type", "type": "text", "length": 4},
            # Geocoding attributes.
            {"name": "l_ladd", "type": "long"},
            {"name": "l_hadd", "type": "long"},
            {"name": "r_ladd", "type": "long"},
            {"name": "r_hadd", "type": "long"},
            {"name": "mailcity", "type": "text", "length": 16},
            {"name": "county", "type": "text", "length": 10},
            {"name": "zipcode", "type": "text", "length": 5},
            # Classification attributes.
            {"name": "fclass", "type": "text", "length": 8},
            {"name": "fed_class", "type": "text", "length": 36},
            {"name": "airsclass", "type": "text", "length": 8},
            {"name": "cclass", "type": "short"},
            {"name": "paved", "type": "text", "length": 1},
            {"name": "speed", "type": "short"},
            {"name": "snowroute", "type": "short"},
            # Extra attributes.
            {"name": "ugbcity", "type": "text", "length": 3},
            # Network definition attributes.
            {"name": "one_way", "type": "text", "length": 2},
            {"name": "fnode", "type": "long"},
            {"name": "tnode", "type": "long"},
            {"name": "f_zlev", "type": "short"},
            {"name": "t_zlev", "type": "short"},
            {"name": "optcost", "type": "double"},
        ],
        geometry_type="polyline",
        path={
            "source": ROAD.path("pub"),
            "pub": os.path.join(path.BUILT_RESOURCES, "Road_Edges"),
        },
    )
]
SITE_ADDRESS = Dataset(
    fields=[
        # Core attributes.
        {"name": "site_address_gfid", "type": "guid", "tags": ["maint", "pub"]},
        {"name": "geofeat_id", "type": "long", "tags": ["maint", "pub"], "is_id": True},
        {"name": "concat_address", "type": "text", "length": 128, "tags": ["pub"]},
        {"name": "concat_address_full", "type": "text", "length": 128, "tags": ["pub"]},
        {
            "name": "concat_address_no_direction",
            "type": "text",
            "length": 128,
            "tags": ["pub"],
        },
        {
            "name": "concat_address_no_unit",
            "type": "text",
            "length": 128,
            "tags": ["pub"],
        },
        {"name": "house_nbr", "type": "long", "tags": ["maint", "pub"]},
        {
            "name": "house_suffix_code",
            "type": "text",
            "length": 4,
            "tags": ["maint", "pub"],
        },
        {
            "name": "pre_direction_code",
            "type": "text",
            "length": 2,
            "tags": ["maint", "pub"],
        },
        {"name": "pre_direction", "type": "text", "length": 16, "tags": ["pub"]},
        {"name": "street_name", "type": "text", "length": 32, "tags": ["maint", "pub"]},
        {
            "name": "street_type_code",
            "type": "text",
            "length": 5,
            "tags": ["maint", "pub"],
        },
        {"name": "street_type", "type": "text", "length": 16, "tags": ["pub"]},
        {"name": "street_name_full", "type": "text", "length": 64, "tags": ["pub"]},
        {
            "name": "unit_type_code",
            "type": "text",
            "length": 5,
            "tags": ["maint", "pub"],
        },
        {"name": "unit_type", "type": "text", "length": 16, "tags": ["pub"]},
        {"name": "unit_id", "type": "text", "length": 5, "tags": ["maint", "pub"]},
        {"name": "city_name_abbr", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "city_name", "type": "text", "length": 32, "tags": ["maint", "pub"]},
        # Extended attributes.
        {"name": "state_code", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "state_name", "type": "text", "length": 16, "tags": ["pub"]},
        {
            "name": "five_digit_zip_code",
            "type": "text",
            "length": 5,
            "tags": ["maint", "pub"],
        },
        {"name": "four_digit_zip_code", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "city_state_zip", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "county_name", "type": "text", "length": 16, "tags": ["pub"]},
        {"name": "postal_carrier_route", "type": "text", "length": 4, "tags": ["pub"]},
        {
            "name": "usps_delivery_point_code",
            "type": "text",
            "length": 3,
            "tags": ["pub"],
        },
        {"name": "usps_is_cmra", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "usps_is_vacant", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "usps_has_mail_service", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "landuse", "type": "text", "length": 4, "tags": ["maint", "pub"]},
        {"name": "landuse_desc", "type": "text", "length": 256, "tags": ["pub"]},
        {"name": "usecode", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "usedesc", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "infill", "type": "text", "length": 1, "tags": ["maint"]},
        {"name": "structure", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        {"name": "drive_id", "type": "text", "length": 16, "tags": ["maint", "pub"]},
        # Maintenance attributes.
        {"name": "valid", "type": "text", "length": 1, "tags": ["maint", "pub"]},
        {"name": "archived", "type": "text", "length": 1, "tags": ["maint", "pub"]},
        {"name": "address_confidence", "type": "text", "length": 1, "tags": ["maint"]},
        {"name": "location", "type": "text", "length": 32, "tags": ["maint", "pub"]},
        {"name": "location_checked", "type": "date", "tags": ["maint", "pub"]},
        {"name": "outofseq", "type": "text", "length": 1, "tags": ["maint", "pub"]},
        {
            "name": "source_from_field_ind",
            "type": "text",
            "length": 1,
            "tags": ["maint"],
        },
        {"name": "renumbering_ind", "type": "text", "length": 1, "tags": ["maint"]},
        {"name": "memo", "type": "text", "length": 256, "tags": ["maint"]},
        {"name": "point_review", "type": "text", "length": 3, "tags": ["maint"]},
        {"name": "point_review_date", "type": "date", "tags": ["maint"]},
        {"name": "problem_address", "type": "text", "length": 3, "tags": ["maint"]},
        {"name": "problem_memo", "type": "text", "length": 256, "tags": ["maint"]},
        {"name": "sent_to_juris", "type": "text", "length": 1, "tags": ["maint"]},
        {"name": "conflict_w_juris", "type": "text", "length": 1, "tags": ["maint"]},
        {"name": "initial_create_date", "type": "date", "tags": ["maint", "pub"]},
        {
            "name": "initial_create_editor",
            "type": "text",
            "length": 32,
            "tags": ["maint"],
        },
        {"name": "last_update_date", "type": "date", "tags": ["maint", "pub"]},
        {"name": "last_update_editor", "type": "text", "length": 32, "tags": ["maint"]},
        {"name": "auto_memo", "type": "text", "length": 512, "tags": ["maint"]},
        # A&T attributes.
        {"name": "tca", "type": "text", "length": 6, "tags": ["pub"]},
        {"name": "maptaxlot", "type": "text", "length": 13, "tags": ["maint", "pub"]},
        {"name": "maptaxlot_hyphen", "type": "text", "length": 17, "tags": ["pub"]},
        {"name": "mapnumber", "type": "text", "length": 8, "tags": ["pub"]},
        {"name": "taxlot", "type": "text", "length": 5, "tags": ["pub"]},
        {"name": "account", "type": "text", "length": 7, "tags": ["maint", "pub"]},
        # City attributes.
        {"name": "geocity", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "geocity_name", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "annexhist", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "yearanx", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "ugb", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "ugb_city_name", "type": "text", "length": 32, "tags": ["pub"]},
        # Planning & zoning attributes.
        {"name": "greenwy", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "neighbor", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "neighborhood_name", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "nodaldev", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "nodaldev_name", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "plandes_id", "type": "long", "tags": ["pub"]},
        {"name": "plandesjuris", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "plandes", "type": "text", "length": 5, "tags": ["pub"]},
        {"name": "plandesdesc", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "sprsvcbndy", "type": "text", "length": 1, "tags": ["pub"]},
        # Public safety attributes.
        {"name": "ambulance_district", "type": "text", "length": 2, "tags": ["pub"]},
        {
            "name": "ambulance_service_area",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
        },
        {
            "name": "ambulance_service_provider",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
        },
        {"name": "firedist", "type": "text", "length": 3, "tags": ["pub"]},
        {
            "name": "fire_protection_provider",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
        },
        {"name": "psap_code", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "psap_name", "type": "text", "length": 32, "tags": ["pub"]},
        {
            "name": "emergency_service_number",
            "type": "text",
            "length": 2,
            "tags": ["pub"],
        },
        {"name": "police_beat", "type": "text", "length": 4, "tags": ["pub"]},
        # Election attributes.
        {"name": "electionpr", "type": "text", "length": 6, "tags": ["pub"]},
        {"name": "ccward", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "city_councilor", "type": "text", "length": 64, "tags": ["pub"]},
        ##TODO: Add to RLIDGeo.
        {"name": "clpud_subdivision", "type": "long", "tags": ["pub"]},
        {"name": "cocommdist", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "cocommdist_name", "type": "text", "length": 16, "tags": ["pub"]},
        {"name": "county_commissioner", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "epud", "type": "text", "length": 1, "tags": ["pub"]},
        {
            "name": "eweb_commissioner_name",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
        },
        ##TODO: Add to RLIDGeo.
        {"name": "hwpud_subdivision", "type": "long", "tags": ["pub"]},
        {"name": "lcczone", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "strepdist", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "state_representative", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "senatedist", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "state_senator", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "swcd", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "swcdzone", "type": "text", "length": 1, "tags": ["pub"]},
        # Education attributes.
        {"name": "schooldist", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "schooldist_name", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "elem", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "elem_name", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "middle", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "middle_name", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "high", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "high_name", "type": "text", "length": 32, "tags": ["pub"]},
        # Transportation attributes.
        {"name": "cats", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "ltddist", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "ltdridesrc", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "trans_analysis_zone", "type": "long", "tags": ["pub"]},
        # Natural attributes.
        {"name": "firmnumber", "type": "text", "length": 12, "tags": ["pub"]},
        {"name": "firmprinted", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "firm_community_id", "type": "text", "length": 6, "tags": ["pub"]},
        {"name": "firm_community_post_firm_date", "type": "date", "tags": ["pub"]},
        {"name": "soilkey", "type": "text", "length": 30, "tags": ["pub"]},
        {"name": "soiltype", "type": "text", "length": 5, "tags": ["pub"]},
        {"name": "wetland", "type": "text", "length": 32, "tags": ["pub"]},
        # Census attributes.
        {"name": "ctract", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "blockgr", "type": "text", "length": 4, "tags": ["pub"]},
        # Geometry attributes.
        {"name": "x_coordinate", "type": "double", "tags": ["pub"]},
        {"name": "y_coordinate", "type": "double", "tags": ["pub"]},
        {"name": "longitude", "type": "double", "tags": ["pub"]},
        {"name": "latitude", "type": "double", "tags": ["pub"]},
    ],
    geometry_type="point",
    path={
        "maint": os.path.join(database.ADDRESSING.path, "dbo.SiteAddress"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.SiteAddress"),
    },
)
SITE_ADDRESS_CLOSEST_FIRE_STATION = Dataset(
    fields=[
        {"name": "site_address_uuid", "type": "guid", "is_id": True},
        {"name": "site_address_intid", "type": "long"},
        {"name": "provider_name", "type": "text", "length": 64},
        {"name": "facility_uuid", "type": "guid"},
        {"name": "facility_intid", "type": "long"},
        {"name": "facility_name", "type": "text", "length": 64},
        {"name": "facility_distance_feet", "type": "double"},
        {"name": "facility_x_coordinate", "type": "double"},
        {"name": "facility_y_coordinate", "type": "double"},
        {"name": "facility_longitude", "type": "double"},
        {"name": "facility_latitude", "type": "double"},
    ],
    geometry_type="polyline",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.SiteAddress_Closest_FireStation"),
)
SITE_ADDRESS_CLOSEST_HYDRANT = Dataset(
    fields=[
        {"name": "site_address_uuid", "type": "guid", "is_id": True},
        {"name": "site_address_intid", "type": "long"},
        {"name": "provider_name", "type": "text", "length": 64},
        {"name": "facility_uuid", "type": "guid"},
        {"name": "facility_intid", "type": "long"},
        {"name": "facility_name", "type": "text", "length": 64},
        {"name": "facility_distance_feet", "type": "double"},
        {"name": "facility_x_coordinate", "type": "double"},
        {"name": "facility_y_coordinate", "type": "double"},
        {"name": "facility_longitude", "type": "double"},
        {"name": "facility_latitude", "type": "double"},
    ],
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.SiteAddress_Closest_Hydrant"),
)
SOIL_WATER_CONSERVATION_DISTRICT = Dataset(
    fields=[
        {"name": "swcdist", "type": "text", "length": 64, "is_id": True},
        {"name": "swczone", "type": "text", "length": 1, "is_id": True},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(
            database.LCOGGEO.path, "lcogadm.SoilWaterConservationDistrict"
        ),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.SoilWaterConservationDist"),
    },
)
##TODO: Deprecate this somehow (used in oiverlay); think is just UGB buffer.
SPRINGFIELD_HANSEN_EXTENT = Dataset(
    fields=[{"name": "is_inside", "type": "text", "length": 1, "is_id": True}],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.Springfield_HansenExtent"),
)
STATE_REPRESENTATIVE_DISTRICT = Dataset(
    fields=[
        {"name": "repdist", "type": "text", "length": 2, "is_id": True},
        {"name": "repname", "type": "text", "length": 64},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.StateRepDist"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.StateRepDist"),
    },
)
STATE_SENATOR_DISTRICT = Dataset(
    fields=[
        {"name": "sendist", "type": "text", "length": 2, "is_id": True},
        {"name": "senname", "type": "text", "length": 64},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.StateSenDist"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.StateSenDist"),
    },
)
STREET_DIRECTION = Dataset(
    fields=[
        {"name": "code", "type": "text", "length": 2},
        {"name": "description", "type": "text", "length": 16},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.StreetDirection"),
)
STREET_NAME_CITY = Dataset(
    fields=[
        {"name": "pre_direction_code", "type": "text", "length": 2},
        {"name": "street_name", "type": "text", "length": 30},
        {"name": "street_type_code", "type": "text", "length": 5},
        {"name": "city_name", "type": "text", "length": 30},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.StreetNameCity"),
)
STREET_TYPE = Dataset(
    fields=[
        {"name": "code", "type": "text", "length": 5},
        {"name": "description", "type": "text", "length": 16},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.StreetType"),
)
STRUCTURE_TYPE = Dataset(
    fields=[
        {"name": "code", "type": "text", "length": 16},
        {"name": "description", "type": "text", "length": 64},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.StructureType"),
)
TAX_CODE_AREA = Dataset(
    fields=[
        {"name": "taxcode", "type": "text", "length": 8},
        {"name": "source", "type": "text", "length": 20},
        {"name": "yearcreated", "type": "date"},
        {"name": "ordinance", "type": "text", "length": 20},
        {"name": "schooldist", "type": "text", "length": 4},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.Taxcode"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.TaxcodeArea"),
    },
)
TAX_CODE_AREA_CERTIFIED = Dataset(
    fields=TAX_CODE_AREA.fields,
    geometry_type="polygon",
    path={
        2009: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2009_07_31"),
        2010: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2010_07_30"),
        2011: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2011_07_29"),
        2012: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2012_08_10"),
        2013: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2013_08_05"),
        2014: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2014_08_21"),
        2015: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2015_08_21"),
        2016: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2016_08_15"),
        2017: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2017_08_28"),
        2018: os.path.join(database.RLIDGEO.path, "dbo.TaxCodeArea_2018_08_29"),
    },
)
TAX_CODE_DETAIL = Dataset(
    fields=[
        {"name": "taxcode", "type": "text", "length": 6, "is_id": True},
        {"name": "orgname", "type": "text", "length": 64, "is_id": True},
    ],
    path={
        "source": os.path.join(database.RLID.path, "dbo.Tax_Code_Area_Detail"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.TaxCodeDetail"),
        ##TODO: Remove "rlidgeo" tag (no others track this).
        "rlidgeo": os.path.join(database.RLIDGEO.path, "dbo.TaxCodeDetail"),
    },
)
TAXLOT = Dataset(
    fields=[
        # Core attributes.
        {"name": "taxlot_geofeature_id", "type": "long", "tags": ["pub"]},
        {
            "name": "maptaxlot",
            "type": "text",
            "length": 24,
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        {"name": "maptaxlot_hyphen", "type": "text", "length": 17, "tags": ["pub"]},
        {"name": "map", "type": "text", "length": 8, "tags": ["pub"]},
        {"name": "supplemental_map", "type": "text", "length": 20, "tags": ["pub"]},
        {"name": "taxlot", "type": "text", "length": 5, "tags": ["maint", "pub"]},
        {"name": "specialint", "type": "text", "length": 5, "tags": ["maint"]},
        {"name": "floor", "type": "short", "tags": ["maint", "pub"], "is_id": True},
        # Maintenance attributes.
        ##TODO: Create mapping to standardize `oldmaplot` & `oldmaplot2` (maybe as new `oldmaplots`). Then drop from ID field names.
        {
            "name": "oldmaplot",
            "type": "text",
            "length": 13,
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        {
            "name": "oldmaplot2",
            "type": "text",
            "length": 13,
            "tags": ["maint", "pub"],
            "is_id": True,
        },
        # A&T attributes.
        {"name": "numaccnts", "type": "short", "tags": ["pub"]},
        {"name": "account_int", "type": "long", "tags": ["pub"]},
        {"name": "acctno", "type": "text", "length": 7, "tags": ["pub"]},
        {"name": "numowners", "type": "short", "tags": ["pub"]},
        {"name": "owner_id", "type": "long", "tags": ["pub"]},
        {"name": "ownname", "type": "text", "length": 128, "tags": ["pub"]},
        {"name": "addr1", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "addr2", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "addr3", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "ownercity", "type": "text", "length": 40, "tags": ["pub"]},
        {"name": "ownerprvst", "type": "text", "length": 30, "tags": ["pub"]},
        {"name": "ownerzip", "type": "text", "length": 10, "tags": ["pub"]},
        {"name": "ownercntry", "type": "text", "length": 30, "tags": ["pub"]},
        {"name": "taxcode", "type": "text", "length": 5, "tags": ["pub"]},
        {"name": "txcdspl", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "propcl", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "propcldes", "type": "text", "length": 256, "tags": ["pub"]},
        {"name": "statcl", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "statcldes", "type": "text", "length": 256, "tags": ["pub"]},
        ##TODO: Should be long to match RLID.
        {"name": "landval", "type": "double", "tags": ["pub"]},
        ##TODO: Should be long to match RLID.
        {"name": "impval", "type": "double", "tags": ["pub"]},
        {"name": "totval", "type": "long", "tags": ["pub"]},
        {"name": "assdtotval", "type": "long", "tags": ["pub"]},
        {"name": "exm_amt_reg_value", "type": "long", "tags": ["pub"]},
        {"name": "taxable_value", "type": "long", "tags": ["pub"]},
        {"name": "exemptdesc", "type": "text", "length": 48, "tags": ["pub"]},
        {"name": "bldgtype", "type": "text", "length": 35, "tags": ["pub"]},
        {"name": "yearblt", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "mbhm", "type": "text", "length": 1, "tags": ["pub"]},
        # City attributes.
        {"name": "geocity", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "geocity_name", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "annexhist", "type": "text", "length": 50, "tags": ["pub"]},
        {"name": "yearanx", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "ugb", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "ugb_name", "type": "text", "length": 32, "tags": ["pub"]},
        # Planning & zoning attributes.
        {"name": "greenwy", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "neighbor", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "neighborhood_name", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "nodaldev", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "nodaldev_name", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "numlanduse", "type": "short", "tags": ["pub"]},
        {"name": "plandes_id", "type": "long", "tags": ["pub"]},
        {"name": "plandesjuris", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "plandes", "type": "text", "length": 5, "tags": ["pub"]},
        {"name": "plandesdesc", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "zoning_id", "type": "long", "tags": ["pub"]},
        {"name": "zoningjuris", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "zoning", "type": "text", "length": 10, "tags": ["pub"]},
        {"name": "zoningdesc", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "subarea", "type": "text", "length": 8, "tags": ["pub"]},
        {"name": "overlay1", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "overlay2", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "overlay3", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "overlay4", "type": "text", "length": 4, "tags": ["pub"]},
        # Public safety attributes.
        {"name": "ambulance_district", "type": "text", "length": 2, "tags": ["pub"]},
        {
            "name": "ambulance_service_area",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
        },
        {
            "name": "ambulance_service_provider",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
        },
        {"name": "firedist", "type": "text", "length": 3, "tags": ["pub"]},
        {
            "name": "fire_protection_provider",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
        },
        # Election attributes.
        {"name": "electionpr", "type": "text", "length": 6, "tags": ["pub"]},
        {"name": "ccward", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "city_councilor", "type": "text", "length": 64, "tags": ["pub"]},
        ##TODO: Add to RLIDGeo.
        {"name": "clpud_subdivision", "type": "long", "tags": ["pub"]},
        {"name": "cocommdist", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "cocommdist_name", "type": "text", "length": 16, "tags": ["pub"]},
        {"name": "county_commissioner", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "epud", "type": "text", "length": 1, "tags": ["pub"]},
        {
            "name": "eweb_commissioner_name",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
        },
        ##TODO: Add to RLIDGeo.
        {"name": "hwpud_subdivision", "type": "long", "tags": ["pub"]},
        {"name": "lcczone", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "strepdist", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "state_representative", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "senatedist", "type": "text", "length": 2, "tags": ["pub"]},
        {"name": "state_senator", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "swcd", "type": "text", "length": 50, "tags": ["pub"]},
        {"name": "swcdzone", "type": "text", "length": 1, "tags": ["pub"]},
        # Education attributes.
        {"name": "schooldist", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "schooldist_name", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "elem", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "elem_name", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "middle", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "middle_name", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "high", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "high_name", "type": "text", "length": 32, "tags": ["pub"]},
        # Transportation attributes.
        {"name": "cats", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "ltddist", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "ltdridesrc", "type": "text", "length": 3, "tags": ["pub"]},
        {"name": "trans_analysis_zone", "type": "long", "tags": ["pub"]},
        # Natural attributes.
        {"name": "firmnumber", "type": "text", "length": 12, "tags": ["pub"]},
        {"name": "firmprinted", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "firm_community_id", "type": "text", "length": 32, "tags": ["pub"]},
        {"name": "firm_community_post_firm_date", "type": "date", "tags": ["pub"]},
        {"name": "soilkey", "type": "text", "length": 30, "tags": ["pub"]},
        {"name": "soiltype", "type": "text", "length": 5, "tags": ["pub"]},
        {"name": "wetland", "type": "text", "length": 40, "tags": ["pub"]},
        # Census attributes.
        {"name": "ctract", "type": "text", "length": 4, "tags": ["pub"]},
        {"name": "blockgr", "type": "text", "length": 4, "tags": ["pub"]},
        # External reference attributes.
        {"name": "rlid_link", "type": "text", "length": 128, "tags": ["pub"]},
        # Geometry attributes.
        {"name": "mapacres", "type": "double", "tags": ["maint", "pub"]},
        {"name": "xcoord", "type": "double", "tags": ["pub"]},
        {"name": "ycoord", "type": "double", "tags": ["pub"]},
        {"name": "longitude", "type": "double", "tags": ["pub"]},
        {"name": "latitude", "type": "double", "tags": ["pub"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.Taxlot"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.Taxlot"),
    },
)
TAXLOT_CERTIFIED = Dataset(
    fields=TAXLOT.fields,
    geometry_type="polygon",
    path={
        2009: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2009_07_31"),
        2010: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2010_07_30"),
        2011: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2011_07_29"),
        2012: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2012_08_10"),
        2013: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2013_08_05"),
        2014: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2014_08_21"),
        2015: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2015_08_21"),
        2016: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2016_08_15"),
        2017: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2017_08_28"),
        2018: os.path.join(database.RLIDGEO.path, "dbo.Taxlot_2018_08_29"),
    },
)
TAXLOT_FOCUS_BOX = Dataset(
    fields=[{"name": "maptaxlot", "type": "text", "length": 13, "is_id": True}],
    geometry_type="polygon",
    path={
        "large": os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotFocusBox_Large"),
        "small": os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotFocusBox_Small"),
    },
)
TAXLOT_FIRE_PROTECTION = Dataset(
    fields=[
        {"name": "maptaxlot", "type": "text", "length": 13, "is_id": True},
        {"name": "maptaxlot_hyphen", "type": "text", "length": 17},
        {"name": "map", "type": "text", "length": 8},
        {"name": "taxlot", "type": "text", "length": 5},
        {"name": "provider_code", "type": "text", "length": 3, "is_id": True},
        {"name": "provider_name", "type": "text", "length": 32},
        {"name": "protection_type_code", "type": "text", "length": 4},
        {"name": "protection_type_description", "type": "text", "length": 64},
        {"name": "tax_district", "type": "text", "length": 1},
        {"name": "contact_phone", "type": "text", "length": 16},
        {"name": "contact_email", "type": "text", "length": 64},
        {"name": "contact_mailing_address", "type": "text", "length": 128},
        {"name": "website_link", "type": "text", "length": 64},
        {"name": "approx_acres", "type": "double"},
        {"name": "approx_taxlot_acres", "type": "double"},
        {"name": "taxlot_area_ratio", "type": "double"},
    ],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotFireProtection"),
)
TAXLOT_FLOOD_HAZARD = Dataset(
    fields=[
        {"name": "maptaxlot", "type": "text", "length": 13, "is_id": True},
        {"name": "maptaxlot_hyphen", "type": "text", "length": 17},
        {"name": "map", "type": "text", "length": 8},
        {"name": "taxlot", "type": "text", "length": 5},
        {"name": "flood_area_id", "type": "text", "length": 32, "is_id": True},
        {"name": "flood_zone_code", "type": "text", "length": 17},
        {"name": "flood_zone_subtype", "type": "text", "length": 64},
        {"name": "old_flood_zone_code", "type": "text", "length": 2},
        {"name": "flood_zone_description", "type": "text", "length": 256},
        {"name": "approx_acres", "type": "double"},
        {"name": "approx_taxlot_acres", "type": "double"},
        {"name": "taxlot_area_ratio", "type": "double"},
    ],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotFloodHazard"),
)
TAXLOT_LINE = Dataset(
    fields=[
        {"name": "linetype", "type": "short", "tags": ["maint", "pub"]},
        {"name": "linetypedesc", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "floor", "type": "short", "tags": ["maint", "pub"]},
    ],
    geometry_type="polyline",
    path={
        "maint": os.path.join(database.REGIONAL.path, "lcadm.TaxlotLines"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotLine"),
    },
)
TAXLOT_OWNER = Dataset(
    fields=[
        {"name": "maptaxlot", "type": "text", "length": 13},
        {"name": "acctno", "type": "text", "length": 7},
        {"name": "ownname", "type": "text", "length": 128},
        {"name": "addr1", "type": "text", "length": 64},
        {"name": "addr2", "type": "text", "length": 64},
        {"name": "addr3", "type": "text", "length": 64},
        {"name": "ownercity", "type": "text", "length": 40},
        {"name": "ownerprvst", "type": "text", "length": 30},
        {"name": "ownerzip", "type": "text", "length": 10},
        {"name": "ownercntry", "type": "text", "length": 30},
    ],
    path={
        "source": os.path.join(database.RLID.path, "dbo.Owner"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotOwnerRealActive"),
    },
)
TAXLOT_PETITION_DOCUMENT = Dataset(
    fields=[
        {"name": "maptaxlot", "type": "text", "length": 13, "is_id": True},
        {"name": "maptaxlot_hyphen", "type": "text", "length": 17},
        {"name": "map", "type": "text", "length": 8},
        {"name": "taxlot", "type": "text", "length": 5},
        {
            "name": "petition_jurisdiction_code",
            "type": "text",
            "length": 3,
            "is_id": True,
        },
        {"name": "petition_id", "type": "text", "length": 8, "is_id": True},
        {"name": "petition_number", "type": "text", "length": 8},
        {"name": "petition_type_code", "type": "text", "length": 1},
        {"name": "petition_type_description", "type": "text", "length": 32},
        {"name": "petition_date", "type": "date"},
        {"name": "is_active", "type": "text", "length": 3},
        {"name": "alley_petition", "type": "text", "length": 3},
        {"name": "bikepath_petition", "type": "text", "length": 3},
        {"name": "paving_petition", "type": "text", "length": 3},
        {"name": "pedway_petition", "type": "text", "length": 3},
        {"name": "rehab_petition", "type": "text", "length": 3},
        {"name": "sanitary_petition", "type": "text", "length": 3},
        {"name": "sidewalk_petition", "type": "text", "length": 3},
        {"name": "storm_petition", "type": "text", "length": 3},
        {"name": "streetlight_petition", "type": "text", "length": 3},
        {"name": "document_name", "type": "text", "length": 32, "is_id": True},
        # Part of ID because some documents listed a mutliple types.
        {"name": "document_type", "type": "text", "length": 16, "is_id": True},
        {"name": "rlid_image_url", "type": "text", "length": 128},
        {"name": "rlid_document_url", "type": "text", "length": 128},
    ],
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotPetitionDocument"),
)
TAXLOT_PLAT_DOCUMENT = Dataset(
    fields=[
        {"name": "maptaxlot", "type": "text", "length": 13, "is_id": True},
        {"name": "maptaxlot_hyphen", "type": "text", "length": 17},
        {"name": "map", "type": "text", "length": 8},
        {"name": "taxlot", "type": "text", "length": 5},
        {"name": "plat_name", "type": "text", "length": 128, "is_id": True},
        {"name": "document_number", "type": "long"},
        {"name": "document_name", "type": "text", "length": 32, "is_id": True},
        # Part of ID because some documents listed a mutliple types.
        {"name": "document_type", "type": "text", "length": 16, "is_id": True},
        {"name": "rlid_image_url", "type": "text", "length": 128},
        {"name": "rlid_document_url", "type": "text", "length": 128},
    ],
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotPlatDocument"),
)
TAXLOT_SOIL = Dataset(
    fields=[
        {"name": "maptaxlot", "type": "text", "length": 13, "is_id": True},
        {"name": "maptaxlot_hyphen", "type": "text", "length": 17},
        {"name": "map", "type": "text", "length": 8},
        {"name": "taxlot", "type": "text", "length": 5},
        {"name": "mukey", "type": "text", "length": 30, "is_id": True},
        {"name": "musym", "type": "text", "length": 6},
        {"name": "muname", "type": "text", "length": 175},
        {"name": "land_capability_class", "type": "text", "length": 1},
        {"name": "hydric", "type": "text", "length": 1},
        {"name": "hydric_presence", "type": "text", "length": 3},
        {"name": "farm_high_value", "type": "text", "length": 1},
        {"name": "farm_high_value_if_drained", "type": "text", "length": 1},
        {"name": "farm_high_value_if_protected", "type": "text", "length": 1},
        {"name": "farm_potential_high_value", "type": "text", "length": 1},
        {"name": "all_components", "type": "text", "length": 256},
        {"name": "compname1", "type": "text", "length": 64},
        {"name": "nirrcapcl1", "type": "text", "length": 4},
        {"name": "compname2", "type": "text", "length": 64},
        {"name": "nirrcapcl2", "type": "text", "length": 4},
        {"name": "compname3", "type": "text", "length": 64},
        {"name": "nirrcapcl3", "type": "text", "length": 4},
        {"name": "approx_acres", "type": "double"},
        {"name": "approx_taxlot_acres", "type": "double"},
        {"name": "taxlot_area_ratio", "type": "double"},
    ],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotSoil"),
)
TAXLOT_SPLIT_ZONING = Dataset(
    fields=[{"name": "maptaxlot", "type": "text", "length": 13}],
    path=os.path.join(database.LCOGGEO.path, "dbo.Taxlot_Split_Zoning"),
)
TAXLOT_ZONING = Dataset(
    fields=[
        {"name": "maptaxlot", "type": "text", "length": 13, "is_id": True},
        {"name": "maptaxlot_hyphen", "type": "text", "length": 17},
        {"name": "map", "type": "text", "length": 8},
        {"name": "taxlot", "type": "text", "length": 5},
        {"name": "zonejuris", "type": "text", "length": 3, "is_id": True},
        {"name": "zonejuris_name", "type": "text", "length": 16},
        {"name": "zonecode", "type": "text", "length": 10, "is_id": True},
        {"name": "zonename", "type": "text", "length": 64},
        {"name": "subarea", "type": "text", "length": 8, "is_id": True},
        {"name": "subareaname", "type": "text", "length": 64},
        {"name": "alloverlays", "type": "text", "length": 64, "is_id": True},
        {"name": "overlay1", "type": "text", "length": 4},
        {"name": "overlay1_name", "type": "text", "length": 64},
        {"name": "overlay2", "type": "text", "length": 4},
        {"name": "overlay2_name", "type": "text", "length": 64},
        {"name": "overlay3", "type": "text", "length": 4},
        {"name": "overlay3_name", "type": "text", "length": 64},
        {"name": "overlay4", "type": "text", "length": 4},
        {"name": "overlay4_name", "type": "text", "length": 64},
        {"name": "taxlot_zoning_count", "type": "long"},
        {"name": "at_taxlot_centroid", "type": "text", "length": 1},
        {"name": "in_split_table", "type": "text", "length": 1},
        {"name": "approx_acres", "type": "double"},
        {"name": "approx_taxlot_acres", "type": "double"},
        {"name": "taxlot_area_ratio", "type": "double"},
        {"name": "compactness_ratio", "type": "double"},
    ],
    geometry_type="polygon",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.TaxlotZoning"),
)
UGB = Dataset(
    fields=[
        {"name": "ugbcity", "type": "text", "length": 3, "is_id": True},
        {"name": "ugbcityname", "type": "text", "length": 30},
        {"name": "xmin", "type": "double"},
        {"name": "ymin", "type": "double"},
        {"name": "xmax", "type": "double"},
        {"name": "ymax", "type": "double"},
    ],
    geometry_type="polygon",
    path={
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.UGB"),
        "inserts": [
            os.path.join(database.LCOGGEO.path, "lcogadm.CobUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.CotUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.CreUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.DunUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.EugUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.FloUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.JunUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.LowUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.OakUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.SprUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.VenUGB"),
            os.path.join(database.LCOGGEO.path, "lcogadm.WesUGB"),
        ],
    },
)
UGB_LINE = Dataset(
    fields=[],
    geometry_type="polyline",
    path=os.path.join(database.ETL_LOAD_A.path, "dbo.UGBLine"),
)
UNIT_TYPE = Dataset(
    fields=[
        {"name": "code", "type": "text", "length": 5},
        {"name": "description", "type": "text", "length": 16},
    ],
    path=os.path.join(database.ADDRESSING.path, "dbo.UnitType"),
)
UNIT_ID = Dataset(
    fields=[{"name": "unit_id", "type": "text", "length": 5}],
    path=os.path.join(database.ADDRESSING.path, "dbo.UnitID"),
)
WILLAMETTE_RIVER_GREENWAY = Dataset(
    fields=[{"name": "greenway", "type": "text", "length": 1, "is_id": True}],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.WillametteRiverGreenway"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.WillametteRiverGreenway"),
    },
)
ZIP_CODE_AREA = Dataset(
    fields=[
        {"name": "zipcode", "type": "text", "length": 5, "is_id": True},
        {"name": "mailcitycode", "type": "text", "length": 3},
        {"name": "mailcity", "type": "text", "length": 16},
        {"name": "altcity", "type": "text", "length": 16},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(database.LCOGGEO.path, "lcogadm.ZIPCode"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.ZIPCode"),
    },
)
ZONING_CITY = Dataset(
    fields=[
        # Core attributes.
        {"name": "zonejuris", "type": "text", "length": 3, "is_id": True},
        {"name": "zonecode", "type": "text", "length": 10, "is_id": True},
        {"name": "zonename", "type": "text", "length": 64},
        {"name": "finalorder", "type": "text", "length": 8, "is_id": True},
        {"name": "subarea", "type": "text", "length": 8, "is_id": True},
        {"name": "subareaname", "type": "text", "length": 64},
        # Overlay attributes.
        {"name": "alloverlays", "type": "text", "length": 64, "is_id": True},
        {"name": "over10", "type": "text", "length": 1},
        {"name": "over12", "type": "text", "length": 1},
        {"name": "over13", "type": "text", "length": 1},
        {"name": "over20", "type": "text", "length": 1},
        {"name": "over40", "type": "text", "length": 1},
        {"name": "over82", "type": "text", "length": 1},
        {"name": "over89", "type": "text", "length": 1},
        {"name": "overa", "type": "text", "length": 1},
        {"name": "overbw", "type": "text", "length": 1},
        {"name": "overcas", "type": "text", "length": 1},
        {"name": "overcl", "type": "text", "length": 1},
        {"name": "overec", "type": "text", "length": 1},
        {"name": "overfp", "type": "text", "length": 1},
        {"name": "overgw", "type": "text", "length": 1},
        {"name": "overh", "type": "text", "length": 1},
        {"name": "overhd", "type": "text", "length": 1},
        {"name": "overm", "type": "text", "length": 1},
        {"name": "overmhpud", "type": "text", "length": 1},
        {"name": "overmum", "type": "text", "length": 1},
        {"name": "overnd", "type": "text", "length": 1},
        {"name": "overp", "type": "text", "length": 1},
        {"name": "overpd", "type": "text", "length": 1},
        {"name": "overpud", "type": "text", "length": 1},
        {"name": "overrc", "type": "text", "length": 1},
        {"name": "overscw", "type": "text", "length": 1},
        {"name": "oversdp", "type": "text", "length": 1},
        {"name": "oversr", "type": "text", "length": 1},
        {"name": "overtd", "type": "text", "length": 1},
        {"name": "overul", "type": "text", "length": 1},
        {"name": "overwb", "type": "text", "length": 1},
        {"name": "overwp", "type": "text", "length": 1},
        {"name": "overwq", "type": "text", "length": 1},
        {"name": "overwr", "type": "text", "length": 1},
        {"name": "overx", "type": "text", "length": 1},
    ],
    geometry_type="polygon",
    path={
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.Zoning_City"),
        "inserts": [
            os.path.join(database.LCOGGEO.path, "lcogadm.CobZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.CotZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.CreZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.DunZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.EugZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.FloZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.JunZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.LowZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.OakZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.SprZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.VenZoning"),
            os.path.join(database.LCOGGEO.path, "lcogadm.WesZoning"),
        ],
    },
)
ZONING_COUNTY = Dataset(
    fields=[
        # Core attributes.
        {
            "name": "zonejuris",
            "type": "text",
            "length": 3,
            "tags": ["pub"],
            "is_id": True,
        },
        {
            "name": "zonecode",
            "type": "text",
            "length": 10,
            "tags": ["pub"],
            "is_id": True,
        },
        {"name": "ZONE_", "type": "text", "length": 10, "tags": ["maint"]},
        {"name": "zonename", "type": "text", "length": 64, "tags": ["pub"]},
        {"name": "ZONE_NAME", "type": "text", "length": 100, "tags": ["maint"]},
        {"name": "PLOT_NUM", "type": "text", "length": 5, "tags": ["maint"]},
        # Sub-area codes.
        {"name": "coastalzonecode", "type": "text", "length": 4, "tags": ["pub"]},
        # Overlay attributes.
        {
            "name": "alloverlays",
            "type": "text",
            "length": 64,
            "tags": ["pub"],
            "is_id": True,
        },
        {"name": "overas", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overbd", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overcas", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overce", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overde", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overdms", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overmd", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overne", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overnrc", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overpw", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overrd", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "oversn", "type": "text", "length": 1, "tags": ["pub"]},
        {"name": "overu", "type": "text", "length": 1, "tags": ["pub"]},
    ],
    geometry_type="polygon",
    path={
        "maint": os.path.join(path.REGIONAL_STAGING, "LCZoning", "lczoning.shp"),
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.Zoning_County"),
        "insert": os.path.join(path.REGIONAL_STAGING, "LCZoning", "ugb_lczoning.shp"),
    },
)
ZONING = Dataset(
    fields=[
        # Core attributes.
        {"name": "zoning_id", "type": "long"},
        {"name": "zonejuris", "type": "text", "length": 3, "is_id": True},
        {"name": "zonecode", "type": "text", "length": 10, "is_id": True},
        {"name": "zonename", "type": "text", "length": 64, "is_id": True},
        {"name": "finalorder", "type": "text", "length": 8, "is_id": True},
        # subarea is not an ID field, because it only applies to Eug (add when needed).
        {"name": "subarea", "type": "text", "length": 8},
        {"name": "subareaname", "type": "text", "length": 64},
        # Overlay attributes.
        {"name": "alloverlays", "type": "text", "length": 64, "is_id": True},
        {"name": "over10", "type": "text", "length": 1},
        {"name": "over12", "type": "text", "length": 1},
        {"name": "over13", "type": "text", "length": 1},
        {"name": "over20", "type": "text", "length": 1},
        {"name": "over40", "type": "text", "length": 1},
        {"name": "over82", "type": "text", "length": 1},
        {"name": "over89", "type": "text", "length": 1},
        {"name": "overa", "type": "text", "length": 1},
        {"name": "overas", "type": "text", "length": 1},
        {"name": "overbd", "type": "text", "length": 1},
        {"name": "overbw", "type": "text", "length": 1},
        {"name": "overcas", "type": "text", "length": 1},
        {"name": "overce", "type": "text", "length": 1},
        {"name": "overcl", "type": "text", "length": 1},
        {"name": "overde", "type": "text", "length": 1},
        {"name": "overdms", "type": "text", "length": 1},
        {"name": "overec", "type": "text", "length": 1},
        {"name": "overfp", "type": "text", "length": 1},
        {"name": "overgw", "type": "text", "length": 1},
        {"name": "overh", "type": "text", "length": 1},
        {"name": "overhd", "type": "text", "length": 1},
        {"name": "overm", "type": "text", "length": 1},
        {"name": "overmd", "type": "text", "length": 1},
        {"name": "overmhpud", "type": "text", "length": 1},
        {"name": "overmum", "type": "text", "length": 1},
        {"name": "overnd", "type": "text", "length": 1},
        {"name": "overne", "type": "text", "length": 1},
        {"name": "overnrc", "type": "text", "length": 1},
        {"name": "overp", "type": "text", "length": 1},
        {"name": "overpd", "type": "text", "length": 1},
        {"name": "overpud", "type": "text", "length": 1},
        {"name": "overpw", "type": "text", "length": 1},
        {"name": "overrc", "type": "text", "length": 1},
        {"name": "overrd", "type": "text", "length": 1},
        {"name": "overscw", "type": "text", "length": 1},
        {"name": "oversdp", "type": "text", "length": 1},
        {"name": "oversn", "type": "text", "length": 1},
        {"name": "oversr", "type": "text", "length": 1},
        {"name": "overtd", "type": "text", "length": 1},
        {"name": "overu", "type": "text", "length": 1},
        {"name": "overul", "type": "text", "length": 1},
        {"name": "overwb", "type": "text", "length": 1},
        {"name": "overwp", "type": "text", "length": 1},
        {"name": "overwq", "type": "text", "length": 1},
        {"name": "overwr", "type": "text", "length": 1},
        {"name": "overx", "type": "text", "length": 1},
        {"name": "overlay1", "type": "text", "length": 4},
        {"name": "overlay2", "type": "text", "length": 4},
        {"name": "overlay3", "type": "text", "length": 4},
        {"name": "overlay4", "type": "text", "length": 4},
    ],
    geometry_type="polygon",
    path={
        "pub": os.path.join(database.ETL_LOAD_A.path, "dbo.Zoning"),
        "inserts": [ZONING_CITY.path("pub"), ZONING_COUNTY.path("pub")],
    },
)
ZONING_OVERLAY = Dataset(
    fields=[
        {"name": "zoning_juris", "type": "text", "length": 3},
        {"name": "overlay_code", "type": "text", "length": 5},
        {"name": "overlay_name", "type": "text", "length": 64},
    ],
    path=os.path.join(database.LCOGGEO.path, "lcogadm.ZoningOverlay"),
)
