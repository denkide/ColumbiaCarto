"""Execution code for address reports."""
import argparse
import logging

import arcetl
from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper import dataset


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


# ETLs.


##TODO: Refactor: Do without SQL exec; fix table; email?
def site_address_maint_summary_etl():
    """Run ETL for update to site address maintenance summary report."""
    sql_statements = [
        """
        insert into {report}(
            summary_date,
            total_non_unique_xy,
            total_non_unique_xy10
        )
        select
            summary_date = getdate(),
            total_non_unique_xy = (
                select sum(non_unique.feature_count)
                from (
                    select feature_count = count(*)
                    from {dataset}
                    group by round(shape.STX, 0), round(shape.STY, 0)
                    having count(*) > 1
                ) as non_unique
            ),
            total_non_unique_xy10 = (
                select sum(non_unique_10.feature_count)
                from (
                    select feature_count = count(*)
                    from {dataset}
                    group by round(shape.STX, -1), round(shape.STY, -1)
                    having count(*) > 1
                ) as non_unique_10
            );
        """,
        # Set change values for all report rows.
        """
        with Report_Ordered as (
            select
                row_number() over (order by summary_date) as row_rank,
                summary_date,
                total_non_unique_xy,
                change_non_unique_xy,
                total_non_unique_xy10,
                change_non_unique_xy10
            from {report}
        )
            update report
            set
                change_non_unique_xy =
                    report.total_non_unique_xy - sorted.total_non_unique_xy,
                change_non_unique_xy10 =
                    report.total_non_unique_xy10 - sorted.total_non_unique_xy10
            from Report_Ordered as report
                left join Report_Ordered as sorted
                    on report.row_rank - 1 = sorted.row_rank;
        """,
    ]
    kwargs = {
        "dataset": "Addressing.dbo.SiteAddress_evw",
        "report": "Addressing.dbo.Report_SiteAddress_MaintSummary",
    }
    for sql in sql_statements:
        arcetl.workspace.execute_sql(sql.format(**kwargs), database.ADDRESSING.path)


# Jobs.


MONTHLY_JOB = Job("Address_Reports_Monthly", etls=[site_address_maint_summary_etl])


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
