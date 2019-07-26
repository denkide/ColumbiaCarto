"""Document processing objects."""
from collections import Counter
import filecmp
import logging
import os
import shutil
import stat
import subprocess
import time

from . import path  # pylint: disable=relative-beyond-top-level


__all__ = [
    "IMAGE_FILE_EXTENSIONS",
    "changed",
    "convert_image_to_pdf",
    "log_state_counts",
    "repository_file_paths",
    "update_document",
    "update_repository",
]
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

IMAGE_FILE_EXTENSIONS = [
    ".bmp",
    ".dcx",
    ".emf",
    ".gif",
    ".jpg",
    ".jpeg",
    ".pcd",
    ".pcx",
    ".pic",
    ".png",
    ".psd",
    ".tga",
    ".tif",
    ".tiff",
    ".wmf",
]


def changed(document_path, cmp_document_path, missing_document_ok=True):
    """Determine if document file has changed between two instances."""
    # file_name = os.path.basename(document_path).lower()
    if not os.path.exists(document_path):
        if missing_document_ok:
            return True

        else:
            raise OSError(
                "Document does not exist at {} (missing_document_ok=False).".format(
                    document_path
                )
            )

    return not filecmp.cmp(document_path, cmp_document_path)


def convert_image_to_pdf(image_path, output_path):
    """Convert image to a PDF."""
    if os.path.splitext(image_path)[1].lower() not in IMAGE_FILE_EXTENSIONS:
        raise ValueError("Image must have image file type.")

    subprocess.check_call(
        "{} -i {} -o {} -g overwrite".format(path.IMAGE2PDF, image_path, output_path)
    )
    # Image2PDF returns before the underlying library's process completes.
    # So we'll need to wait until the PDF shows up in the file system.
    wait_interval, max_wait, wait_time = 0.1, 30.0, 0.0
    while os.path.isfile(output_path) is False:
        if wait_time < max_wait:
            wait_time += wait_interval
            time.sleep(wait_interval)
        else:
            raise IOError("Image2PDF failed to create PDF.")

    converted = True
    return converted


def log_state_counts(counter, documents_type="documents"):
    """Log the counts for each state in the provided counters."""
    if sum(counter.values()) == 0:
        LOG.info("No %s states to log.", documents_type)
    else:
        for state, count in sorted(counter.items()):
            LOG.info("%s %s %s.", count, documents_type, state)


def repository_file_paths(repository_path, file_extensions=None):
    """Generate file paths present in repository."""
    if file_extensions:
        file_extensions = {ext.lower() for ext in file_extensions}
    for dir_path, _, file_names in os.walk(repository_path):
        for file_name in file_names:
            if file_extensions and os.path.splitext(file_name) not in file_extensions:
                continue

            yield os.path.join(dir_path, file_name)


def update_document(source_path, update_path):
    """Update document from source into repository."""
    # Make destination file overwriteable.
    if os.path.exists(update_path):
        os.chmod(update_path, stat.S_IWRITE)
    # Create directory structure (if necessary).
    path.create_directory(
        os.path.dirname(update_path), exist_ok=True, create_parents=True
    )
    try:
        shutil.copy2(source_path, update_path)
    except IOError:
        result_key = "failed to update"
    else:
        os.chmod(update_path, stat.S_IWRITE)
        result_key = "updated"
    LOG.log(
        logging.INFO if result_key == "updated" else logging.WARNING,
        "%s %s at %s.",
        os.path.basename(source_path),
        result_key,
        update_path,
    )
    return result_key


def update_repository(
    root_path,
    source_root_path,
    file_extensions,
    flatten_tree=False,
    create_pdf_copies=False,
):
    """Update document repository replica from source."""
    LOG.info(
        "Start: Update document repository %s from %s.", root_path, source_root_path
    )
    file_extensions = {ext.lower() for ext in file_extensions}
    for repo_path in [root_path, source_root_path]:
        if not os.access(repo_path, os.R_OK):
            raise OSError("Cannot access {}".format(repo_path))

    source_paths = repository_file_paths(source_root_path, file_extensions)
    count = Counter()
    updated_paths = set()
    for source_path in source_paths:
        bin_path = root_path if flatten_tree else root_path.replace(
            source_root_path, root_path
        )
        update_path = os.path.join(bin_path, os.path.basename(source_path))
        # Add directory (if necessary).
        if not os.path.exists(bin_path):
            path.create_directory(bin_path, create_parents=True)
            LOG.info("Created %s", bin_path)
        # Only copy if not-extant or different from source (e.g. older).
        if changed(update_path, source_path):
            result_key = update_document(source_path, update_path)
            count[result_key] += 1
            if result_key == "updated":
                updated_paths.add(update_path)
        file_root, file_extension = os.path.splitext(update_path)
        if create_pdf_copies and file_extension.lower() != ".pdf":
            if file_extension in IMAGE_FILE_EXTENSIONS:
                pdf_path = file_root + ".pdf"
                if convert_image_to_pdf(update_path, pdf_path):
                    count["converted to PDF"] += 1
                    LOG.info("Converted %s to %s.", update_path, pdf_path)
                else:
                    count["failed to convert to PDF"] += 1
                    LOG.info("Failed to convert %s to PDF.", update_path)
            else:
                LOG.warning("File type not convertable to PDF.")
    log_state_counts(count, documents_type="documents")
    LOG.info("End: Update.")
    return updated_paths
