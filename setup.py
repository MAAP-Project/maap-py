import os.path
from setuptools import find_packages, setup


# Package data
# ------------
_author = "Jet Propulsion Laboratory"
_author_email = "bsatoriu@jpl.nasa.gov"
_classifiers = [
    "Environment :: Console",
    "Framework :: Pytest",
    "Intended Audience :: Developers",
    "Intended Audience :: Information Technology",
    "Intended Audience :: Science/Research",
    "Topic :: Scientific/Engineering",
    "Development Status :: 3 - Alpha",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3.5",
    "Topic :: Internet :: WWW/HTTP",
    "Topic :: Software Development :: Libraries :: Python Modules",
]
_description = "maapPy Python API"
_download_url = ""
_boto3_version = "1.34.41"
_requirements = [
    "backoff~=2.2",
    f"boto3~={_boto3_version}",
    "ConfigParser~=6.0",
    "importlib_resources~=6.0",
    # We must explicitly specify ipython because mapboxgl requires it, but
    # does not specify it in its own requirements.  This is a bug in mapboxgl
    # that has been fixed, but the fix has not been released even though it was
    # fixed in 2019.  See https://github.com/mapbox/mapboxgl-jupyter/pull/172.
    "ipython==8.11.0",
    "mapboxgl~=0.10",
    "PyYAML~=6.0",
    "requests~=2.31",
    "setuptools~=69.0",
]
_extra_requirements = {
    "dev": [
        f"boto3-stubs[s3]~={_boto3_version}",
        "moto~=4.2",
        "mypy~=1.8",
        "pytest~=7.4",
        "responses~=0.24",
        "types-requests~=2.31",
        "types-PyYAML~=6.0",
    ]
}
_keywords = ["dataset", "granule", "nasa", "MAAP", "CMR"]
_license = "Apache License, Version 2.0"
_long_description = "Python client API for interacting with the NASA MAAP API"
_name = "maap-py"
_namespaces: list[str] = []
_test_suite = ""
_url = "https://github.com/MAAP-Project/maap-py"
_version = "3.1.5"
_zip_safe = False

# Setup Metadata
# --------------


def _read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()


_header = "*" * len(_name) + "\n" + _name + "\n" + "*" * len(_name)
_longDescription = "\n\n".join([_header, _read("README.md")])

with open("doc.txt", "w") as doc:
    doc.write(_longDescription)

setup(
    author=_author,
    author_email=_author_email,
    classifiers=_classifiers,
    description=_description,
    download_url=_download_url,
    include_package_data=True,
    setup_requires=["pytest-runner"],
    install_requires=_requirements,
    extras_require=_extra_requirements,
    keywords=_keywords,
    license=_license,
    long_description=_long_description,
    name=_name,
    namespace_packages=_namespaces,
    packages=find_packages(),
    test_suite=_test_suite,
    url=_url,
    version=_version,
    zip_safe=_zip_safe,
)
