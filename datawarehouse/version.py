from pathlib import Path


def get_version_number():
    # This file must exist in the root of the module, as the following code
    # tries to find the module root so that it can find the VERSION file.
    # project_root/
    #  - module/
    #    - VERSION
    #    - version.py
    version_file = Path(__file__).parent / "VERSION"
    version = version_file.read_text().strip()

    return version


__version__ = get_version_number()
