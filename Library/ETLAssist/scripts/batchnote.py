"""Execution code for batch notification."""
import argparse
import logging

from etlassist.pipeline import batch_name_id_map, send_batch_notification


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument('batches', nargs='*',
                      help="Batch names to send notification for",
                      choices=batch_name_id_map().keys())
    # Execute.
    for batch in args.parse_args().batches:
        send_batch_notification(batch)


if __name__ == '__main__':
    main()
