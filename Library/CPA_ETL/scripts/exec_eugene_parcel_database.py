"""Execution code for Eugene parcel database extract processing."""
import argparse
import csv
import logging
import os

import pyodbc

from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import path


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

EXTRACT_TABLE_QUERY_SQL = {
    "LandUseGeneralizations": """
        select
            landuse_general_code,
            isnull(min(landuse_general_desc), '')
        from RLID.dbo.LandUse
        where landuse_general_code is not null
        group by landuse_general_code;
    """,
    "LandUses": """
        select
            landuse_code,
            isnull(min(landuse_desc), ''),
            isnull(min(landuse_general_code), '')
        from RLID.dbo.LandUse
        where landuse_code is not null
        group by landuse_code;
    """,
    "Owners": """
        select
            isnull(substring(party.maplot, 1, 8), ''),
            isnull(substring(party.maplot, 9, 5), ''),
            isnull(nullif(ltrim(rtrim(party.spcl_int_code)), ''), '000'),
            party.party_name,
            isnull(party.addr_line1, ''),
            isnull(party.addr_line2, ''),
            isnull(party.addr_line3, ''),
            isnull(party.city, ''),
            isnull(party.prov_state, ''),
            isnull(party.country, ''),
            isnull(ltrim(rtrim(party.zip_code)), '')
        from RLID.dbo.Parties as party
            inner join RLID.dbo.Account as account on party.prop_id = account.prop_id
        where
            party.role_description = 'Owner'
            -- Omit parties with inactive or personal property accounts.
            and (
                account.active_this_year = 'Y'
                or account.new_acct_active_next_year = 'Y'
            )
            and account.account_int < 5000000
            -- Get only parties associated with parcels.
            and party.maplot is not null
            and ltrim(rtrim(party.maplot)) != ''
        group by
            party.maplot,
            party.spcl_int_code,
            party.party_name,
            party.addr_line1,
            party.addr_line2,
            party.addr_line3,
            party.city,
            party.prov_state,
            party.zip_code,
            party.country;
    """,
    "ParcelAddresses": """
        select
            isnull(address.mail_city_name_abbr, ''),
            street_name = case
                when isnumeric(substring(address.street_name, 1, 2)) = 1
                    then '000' + address.street_name
                when isnumeric(substring(address.street_name, 1, 1)) = 1
                    then '0000' + address.street_name
                else address.street_name
            end,
            isnull(address.street_type_code, ''),
            isnull(ltrim(rtrim(address.pre_direction_code)), ''),
            address.house_number,
            isnull(address.house_suffix, ''),
            isnull(address.unit_type_code, ''),
            isnull(address.unit_number, ''),
            isnull(substring(account.maplot, 1, 8), ''),
            isnull(substring(account.maplot, 9, 5), ''),
            isnull(nullif(ltrim(rtrim(account.spcl_int_code)), ''), '000')
        from RLID.dbo.Site_Address as address
            left join RLID.dbo.Account as account
                on address.account_int = account.account_int;
    """,
    "ParcelLandUses": """
        select
            substring(tax_year.maplot, 1, 8),
            substring(tax_year.maplot, 9, 5),
            isnull(nullif(ltrim(rtrim(tax_year.spcl_int_code)), ''), '000'),
            land_use.landuse_code
        from RLID.dbo.Tax_Year as tax_year
            inner join RLID.dbo.LandUse as land_use
                on tax_year.maplot = land_use.maplot
        where
            tax_year.maplot is not null
            and ltrim(rtrim(tax_year.maplot)) != ''
            and tax_year.latest_year_flag = 'Y'
        group by tax_year.maplot, tax_year.spcl_int_code, land_use.landuse_code;
    """,
    "Parcels": """
        exec RLID_Staging.dbo.proc_Update_EugParcelExtract;
        select
            map,
            taxlot,
            isnull(special_interest_code, '000'),
            isnull(addition, ''),
            isnull(block, ''),
            isnull(block_group, ''),
            isnull(census_tract, ''),
            isnull(flood_plain_type, ''),
            isnull(greenway_code, ''),
            isnull(has_multiple_taims_accounts, ''),
            isnull(is_in_eug_city_limits, ''),
            isnull(is_in_eug_ugb, ''),
            isnull(is_in_river_rd_santaclara_hw99, ''),
            isnull(is_wetland, ''),
            isnull(lot, ''),
            isnull(neighborhood_group, ''),
            isnull(acreage, 0),
            isnull(subdivision, ''),
            isnull(x_coordinate, 0),
            isnull(y_coordinate, 0),
            isnull(year_annexed, 0),
            isnull(year_built, '')
        from RLID_Staging.dbo.EugParcelExtract;
    """,
    "Parties": """
        select
            party.prop_id,
            party.ppi_id,
            party.account_padded,
            ltrim(rtrim(party.account_stripped)),
            party.account_int,
            party.maplot,
            party.maplot_hyphen,
            isnull(nullif(ltrim(rtrim(party.spcl_int_code)), ''), '000'),
            isnull(ltrim(rtrim(party.maplotspi)), ''),
            isnull(party.role_cd, ''),
            isnull(party.role_description, ''),
            isnull(party.party_id, ''),
            isnull(party.party_subtype, ''),
            isnull(party.party_name, ''),
            isnull(party.addr_line1, ''),
            isnull(party.addr_line2, ''),
            isnull(party.addr_line3, ''),
            isnull(party.city, ''),
            isnull(party.prov_state, ''),
            isnull(party.state_code, ''),
            isnull(party.country, ''),
            isnull(ltrim(rtrim(party.zip_code)), ''),
            isnull(party.city_state_zip, '')
         from RLID.dbo.Parties as party
             inner join RLID.dbo.Account as account on party.prop_id = account.prop_id
         where
             -- Omit parties with inactive or personal property accounts.
             (account.active_this_year = 'Y' or account.new_acct_active_next_year = 'Y')
             and account.account_int < 5000000
             -- Get only parties associated with parcels.
             and party.maplot is not null
             and ltrim(rtrim(party.maplot)) != '';
    """,
    "TaxAccountParcels": """
        select
            isnull(tax_year.account_padded, ''),
            substring(tax_year.maplot, 1, 8),
            substring(tax_year.maplot, 9, 5),
            isnull(nullif(ltrim(rtrim(tax_year.spcl_int_code)), ''), '000'),
            isnull(tax_year.rmv_imp_value, 0),
            isnull(tax_year.rmv_land_value, 0),
            isnull(tax_year.assd_total_value, 0),
            isnull(tax_year.prop_class, ''),
            isnull(sum(floor_.base_area), 0),
            isnull(tax_year.tax_year, '')
        from RLID.dbo.Tax_Year as tax_year
            full join RLID.dbo.Floor as floor_ on tax_year.prop_id = floor_.prop_id
        where
            tax_year.maplot is not null
            and ltrim(rtrim(tax_year.maplot)) != ''
            and tax_year.latest_year_flag = 'Y'
        group by
            tax_year.account_padded,
            tax_year.maplot,
            tax_year.spcl_int_code,
            tax_year.rmv_imp_value,
            tax_year.rmv_land_value,
            tax_year.assd_total_value,
            tax_year.prop_class,
            tax_year.tax_year;
    """,
    "Zoning": """
        select
            zoning_id,
            isnull(taxlot_summary_id, ''),
            map,
            ltrim(rtrim(lot)),
            maplot,
            isnull(approx_acres, 0),
            isnull(zone_name, ''),
            isnull(ltrim(rtrim(jurisdiction)), ''),
            zone,
            isnull(ltrim(rtrim(overlay1)), ''),
            isnull(ltrim(rtrim(overlay2)), ''),
            isnull(ltrim(rtrim(overlay3)), ''),
            isnull(ltrim(rtrim(overlay4)), ''),
            overlay5 = '',
            overlay6 = '',
            timestamp = ''
        from RLID.dbo.Zoning
        where maplot is not null and zone is not null;
    """,
}


# Helpers.


def extract_database_file(file_path, sql):
    """Extract CSVfile for Eugene Parcel Database."""
    LOG.info("Start: Create %s.", file_path)
    with pyodbc.connect(database.RLID.odbc_string) as conn:
        cursor = conn.execute(sql)
        with open(file_path, "wb") as csvfile:
            csvwriter = csv.writer(csvfile, delimiter="|")
            for row in cursor:
                csvwriter.writerow(row)
    LOG.info("End: Create.")


# ETLs.


def eugene_parcel_database_etl():
    """Run ETL for extracts forEugene Parcel Database CESQL024 (dev: CESQL023).

    Confirmed by a message from Barry Bogart (2014-10-27), the parcel database is only
    in use to support the legacy app Special Assessments/Accounts Receivable (SPAARS).
    Barry: "SPAARS is an older app that will presumably be replaced before too many
    years from now, but I am not aware of any active project at this time."
    """
    for table_name, sql in EXTRACT_TABLE_QUERY_SQL.items():
        file_path = os.path.join(
            path.REGIONAL_STAGING, "EugeneParcelDB", table_name + ".txt"
        )
        extract_database_file(file_path, sql)


# Jobs.


WEEKLY_JOB = Job("Eugene_Parcel_Database_Weekly", etls=[eugene_parcel_database_etl])


# Execution.


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument("pipelines", nargs="*", help="Pipeline(s) to run")
    available_names = {key for key in list(globals()) if not key.startswith("__")}
    pipeline_names = args.parse_args().pipelines
    if pipeline_names and available_names.issuperset(pipeline_names):
        pipelines = [globals()[arg] for arg in args.parse_args().pipelines]
        for pipeline in pipelines:
            execute_pipeline(pipeline)
    else:
        console = logging.StreamHandler()
        LOG.addHandler(console)
        if not pipeline_names:
            LOG.error("No pipeline arguments.")
        for arg in pipeline_names:
            if arg not in available_names:
                LOG.error("`%s` not available in exec.", arg)
        LOG.error(
            "Available objects in exec: %s",
            ", ".join("`{}`".format(name) for name in sorted(available_names)),
        )


if __name__ == "__main__":
    main()
