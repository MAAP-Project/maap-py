import logging
import json

import yaml
from yaml import load as yaml_load, dump as yaml_dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
from cwl_utils.parser import load_document_by_uri, cwl_v1_2
import re
import urllib.parse
import os 

logger = logging.getLogger(__name__)


def read_yaml_file(algo_yaml):
    algo_config = dict()
    with open(algo_yaml, 'r') as fr:
        algo_config = yaml_load(fr, Loader=Loader)
    return validate_algorithm_config(algo_config)

def validate_algorithm_config(algo_config):
    return algo_config
