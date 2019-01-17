# Python MAAP Client Library

Python client library that abstracts [MAAP API](https://github.com/MAAP-Project/maap-api) calls including CMR querying, algorithm change management, and HySDS job execution. CMR components in this library are largely derived from the [pyCMR](https://github.com/nasa/pyCMR) library.



## Setup

Run:

    $ python setup.py install

Or

    $ pip install -e .

## Usage

Populate your MAAP base url into a `maap.cfg` file, using [maap.cfg](maap.cfg) as a template.

Then, run:

```python
$ python
>>> from maap.maap import MAAP
>>> maap = MAAP('maap.cfg') 

>>> granules = maap.searchGranule(sitename='lope', instrument='uavsar')
>>> for res in granules:
    print(res.getDownloadUrl())
    res.download()
#results omitted for brevity
```

### Custom CMR 'Additional Attribute' Parameters

Custom parameters may be used to substitute CMR's [additional attributes](ttps://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#g-additional-attribute) to improve usability.
This list of attributes is editable in the [maap.cfg](maap.cfg) `indexed_attributes` setting. E.g.:
- "site_name,Site Name,string" where `site_name` is the parameter, `Site Name` is the CMR attribute name, and `string` is the parameter type.

## Test

```bash
python setup.py test
```