"""Execution code for license usage monitoring.

Feature license names sourced from:
https://desktop.arcgis.com/en/license-manager/latest/feature-names-for-arcgis-options-file.htm
LMStat parameters sourced from:
https://media.3ds.com/support/simulia/public/flexlm108/EndUser/chap7.htm
"""
import argparse
import datetime
import logging
import re
import subprocess

import dateutil.parser

from etlassist.pipeline import Job, execute_pipeline

from helper import database
from helper.model import LicenseArcGISDesktop, LicenseUsage
from helper import path


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

REGEX_PATTERN = {
    "borrowed": re.compile(
        r"""
        ACTIVATED\sLICENSE\(S\)\s
        (?P<user_host>\w*)\s
        ACTIVATION\s
        \((?P<license_version>v[0-9.]*)\)\s
        \((?P<flexnet_host>\w*)
        /(?P<flexnet_port>\d*)\s
        (?P<flexnet_license_handle>\d*)\),\s
        start\s(?P<checkout_time>.*)
        """,
        re.VERBOSE,
    ),
    "checked-out": re.compile(
        r"""
        (?P<user_handle>\S*)\s
        (?P<user_host>\w*)\s
        (?P<user_display>.*(?=\s\(v))\s
        \((?P<license_version>v[0-9.]*)\)\s
        \((?P<flexnet_host>\w*)
        /(?P<flexnet_port>\d*)\s
        (?P<flexnet_license_handle>\d*)\),\s
        start\s(?P<checkout_time>.*)
        """,
        re.VERBOSE,
    ),
}


# Helpers.


def license_usage_info(internal_name):
    """Generate concurrent license usage info.

    Args:
        internal_name (str): Name used internally for license.

    Returns:
        dict: Mapping of license usage attribute name to value.
    """
    call = """"{}" lmstat -c @gisrv100 -f {}""".format(path.LMUTIL, internal_name)
    raw_output = subprocess.check_output(call).decode()
    if "floating license" not in raw_output:
        return

    lines = (
        line.strip()
        for line in raw_output.split("floating license")[1].split("\r\n")
        if line.strip()
    )
    for line in lines:
        usage_type = (
            "borrowed" if line.startswith("ACTIVATED LICENSE(S)") else "checked-out"
        )
        match = REGEX_PATTERN[usage_type].match(line).groupdict()
        usage = {
            "usage_check_time": datetime.datetime.now(),
            # Borrowed licenses have no user handle.
            "user_handle": (
                match["user_handle"].upper() if "user_handle" in match else None
            ),
            "user_host": match["user_host"].upper(),
            "license_internal_name": internal_name,
            "checkout_time": dateutil.parser.parse(match["checkout_time"]),
            "is_borrowed": True if usage_type == "borrowed" else False,
        }
        yield usage


# ETLs.


def license_usage_update():
    """Run update for current license usage."""
    LOG.info("Start: Collect license usage from FlexNet License Manager.")
    session = database.CPA_ADMIN.create_session()
    names = (name for name, in session.query(LicenseArcGISDesktop.internal_name))
    for name in names:
        session.add_all(LicenseUsage(**usage) for usage in license_usage_info(name))
    session.commit()
    session.close()
    LOG.info("End: Collect.")


# Jobs.


FIVE_MINUTE_JOB = Job("License_Usage", etls=[license_usage_update])


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
