from setuptools import find_packages, setup

from datawarehouse.version import __version__ as version


with open("requirements/install.txt") as f:
    requirements = f.read().splitlines()


setup(
    name="DataWarehouse",
    version=version,
    description="The Datafeeds data warehouse python interface and implementation.",
    author="Invenia Technical Computing",
    url="https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=requirements,
    include_package_data=True,
)
