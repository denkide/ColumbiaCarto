"""Execution code for state ePermitting projects I/O."""
import argparse
import logging
import os
import re

import requests

from etlassist.pipeline import Job, execute_pipeline

from helper import path
from helper import url


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

CSV_HREF_PATTERN = ('Click <a href="(.|\n)*?">here</a> to download CSV file'
                    ' or view as spreadsheet')
FORM_PAYLOAD = {'status': 'Active', 'Bus_Name': '', 'Locale': '',
                'OperType': 'All', 'County': 'All', 'YearExp': 'None',
                'License_Nbr': '', 'csvfile': 'on', 'Submit': 'Submit'}
LP_DEQ_CSV_PATH = os.path.join(path.RLID_DB_STAGING, 'OregonEPermitting',
                               'LicensedProfessionals', 'Input', 'DEQ.csv')


# ETLs.

def lp_deq_etl():
    """Run ETL for Licensed Professional/DEQ CSV file."""
    with requests.Session() as session:
        # Get form page.
        form_response = session.get(url.DEQ_WEB +'wq/onsite/sdssearch.asp')
        # Post form.
        result_response = session.post(
            url=url.DEQ_WEB + 'wq/onsite/sdsresults.asp', params=FORM_PAYLOAD,
            headers={'Referer': form_response.url}
            )
        csv_relpath = re.search(CSV_HREF_PATTERN,
                                result_response.text).group(0).split('"')[1]
        csv_url = requests.compat.urljoin(url.DEQ_WEB, csv_relpath)
        csv_response = session.get(url=csv_url,
                                   headers={'Referer': result_response.url})
    with open(LP_DEQ_CSV_PATH, 'wb') as csvfile:
        csvfile.write(csv_response.content)


# Jobs.

INPUT_HOURLY_JOB = Job('ePermitting_Input_Hourly',
                       etls=(
                           lp_deq_etl,
                           ))


# Execution.

DEFAULT_PIPELINES = (INPUT_HOURLY_JOB,)


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument('pipelines', nargs='*', help="Pipeline(s) to run")
    # Collect pipeline objects.
    if args.parse_args().pipelines:
        pipelines = tuple(globals()[arg] for arg in args.parse_args().pipelines)
    else:
        pipelines = DEFAULT_PIPELINES
    # Execute.
    for pipeline in pipelines:
        execute_pipeline(pipeline)


if __name__ == '__main__':
    main()
