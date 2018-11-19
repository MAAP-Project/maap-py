# Python MAAP Client Library

Python client library that abstracts [MAAP API](https://github.com/MAAP-Project/maap-api) calls including CMR querying, algorithm change management, and HySDS job execution. CMR components in this library are largely derived from the [pyCMR](https://github.com/nasa/pyCMR) library.



## Setup

Run:

    $ python setup.py install

Or

    $ pip install -e .

### Usage

Populate your MAAP base url into a `maap.cfg` file, using [maap.cfg](maap.cfg) as a template.

Then, run:

```python
$ python
>>> from maap.maap import MAAP
>>> maap = MAAP('/path/to/my/maap.cfg') 

>>> results = maap.searchCollection(keyword='precipitation')
>>> print(results[0].keys())
dict_keys(['concept-id', 'revision-id', 'format', 'Collection'])
>>> for res in results:
    print("Short Name: "+ res['Collection']['ShortName'])
    print("Dataset ID: " + res['Collection']['DataSetId'])
    if 'Collection' in res and 'ArchiveCenter' in res['Collection']:
        print("Archive Center: " + res['Collection']['ArchiveCenter'])
    if res['Collection']['Description'] is not None:
        print("Description: " + res['Collection']['Description'])
    if 'BoundingRectangle' in res['Collection']['Spatial']['HorizontalSpatialDomain']['Geometry'] is not None:
        print("Bounding Rectangle: " + str(res['Collection']['Spatial']['HorizontalSpatialDomain']['Geometry']['BoundingRectangle']))
    print("================================================")
#results omitted for brevity

>>> granules = maap.searchGranule(short_name='MOD11A1')
>>> for res in granules:
    print(res.getDownloadUrl())
    res.download()
#results omitted for brevity
```

## Test

```bash
python setup.py test
```