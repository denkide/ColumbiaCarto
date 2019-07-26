"""File system path objects."""
import logging
import os
import subprocess
import zipfile


__all__ = [
    "CPA_WORK_SHARE",
    "IMAGE2PDF",
    "LMUTIL",
    "MAILERSPLUS4",
    "PSCP",
    "archive_directory",
    "create_directory",
    "extract_archive",
    "flattened",
    "folder_file_paths",
]
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


# Network shares.


CPA_WORK_SHARE = r"\\gisrv100.ris5.net\work"


# Repository paths.


# First-level.
ETLASSIST = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir))
LOGFILES = os.path.join(CPA_WORK_SHARE, "Processing", "log")
# Second-level.
RESOURCES = os.path.join(ETLASSIST, "resources")
# Third-level.
SDE_CONNECTIONS = os.path.join(RESOURCES, "sde_connections")
APPS = os.path.join(RESOURCES, "apps")


# Application paths.


IMAGE2PDF = os.path.join(APPS, "Image2PDF\\image2pdf.exe -r EUIEUFBFYUOQVPAT")
LMUTIL = os.path.join(APPS, "lmutil.exe")
if os.environ["COMPUTERNAME"].upper() == "CLWRKGIS":
    MAILERSPLUS4 = "C:\\Program Files (x86)\\Melissa DATA\\MAILERS+4\\mp4.exe"
else:
    MAILERSPLUS4 = None
PSCP = os.path.join(APPS, "pscp.exe")
SEVEN_ZIP = os.path.join(APPS, "7_Zip\\x64\\7za.exe")


def archive_directory(directory_path, archive_path, directory_as_base=False, **kwargs):
    """Create zip archive of files in the given directory.

    The exclude pattern will ignore any directory or file name that includes any
        pattern listed.

    Args:
        directory_path (str): Path of directory to archive.
        archive_path (str): Path of archive to create.
        directory_as_base (bool): Place contents in the base directory within the
            archive if True, do not if False.


    Keyword Args:
        archive_exclude_patterns (iter): Collection of file name patterns to exclude
            from archive.
        encrypt_password (str): Password for an encrypted wrapper archive to place the
            directory archive inside. Default is None (no encryption/wrapper).

    Returns:
        str: Path of archive created.
    """
    kwargs.setdefault("archive_exclude_patterns", [])
    kwargs.setdefault("encrypt_password")
    LOG.info("Start: Create archive of directory %s.", directory_path)
    if directory_as_base:
        directory_root_length = len(os.path.dirname(directory_path)) + 1
    else:
        directory_root_length = len(directory_path) + 1
    archive = zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED)
    with archive:
        for subdirectory_path, _, file_names in os.walk(directory_path):
            if any(
                pattern.lower() in os.path.basename(subdirectory_path).lower()
                for pattern in kwargs["archive_exclude_patterns"]
            ):
                continue

            for file_name in file_names:
                if any(
                    pattern.lower() in file_name.lower()
                    for pattern in kwargs["archive_exclude_patterns"]
                ):
                    continue

                file_path = os.path.join(subdirectory_path, file_name)
                file_archive_path = file_path[directory_root_length:]
                archive.write(file_path, file_archive_path)
    if kwargs["encrypt_password"]:
        out_path = "{}_encrypted{}".format(*os.path.splitext(archive_path))
        # Usage: 7za.exe <command> <archive_name> [<file_names>...] [<switches>...]
        call_string = """{exe} a "{wrapper}" "{archive}" -p"{password}" """.format(
            exe=SEVEN_ZIP,
            wrapper=out_path,
            archive=archive_path,
            password=kwargs["encrypt_password"],
        )
        subprocess.check_call(call_string)
        os.remove(archive_path)
    else:
        out_path = archive_path
    LOG.info("End: Create.")
    return out_path


def create_directory(directory_path, exist_ok=False, create_parents=False):
    """Create directory at given path.

    Args:
        directory_path (str): Path of directory to create.
        exist_ok (bool): Already-existing directories treated as successfully created
            if True, raises an exception if False.
        create_parents (bool): Function will create missing parent directories if True,
            Will not (and raise an exception) if False.

    Returns:
        str: Path of the created directory.
    """
    try:
        os.makedirs(directory_path) if create_parents else os.mkdir(directory_path)
    except WindowsError as error:
        # [Error 183] Cannot create a file when that file already exists: {path}
        if not (exist_ok and error.winerror == 183):
            raise

    return directory_path


def extract_archive(archive_path, extract_path, password=None):
    """Extract files from archive into the extract path.

    Args:
        archive_path (str): Path of archive file.
        extract_path (str): Path of folder to extract into.
        password (str): Password for any encrypted contents.

    Returns:
        bool: True if archived extracted, False otherwise.
    """
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            archive.extractall(extract_path, pwd=password)
    except zipfile.BadZipfile:
        LOG.warning("%s not a valid archive.", archive_path)
        extracted = False
    else:
        extracted = True
    return extracted


def flattened(path, flat_char="_"):
    """Returns "flattened" version of given path, with no separators."""
    for char in [os.sep, ":"]:
        path = path.replace(char, flat_char)
    while flat_char * 2 in path:
        path = path.replace(flat_char * 2, flat_char)
    while path.startswith(flat_char) or path.endswith(flat_char):
        path = path.strip(flat_char)
    return path


def folder_file_paths(folder_path):
    """Generate paths for files in folder.

    Args:
        folder_path (str): Path for folder to list file paths within.

    Yields:
        str: Path of file.
    """
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            yield file_path
