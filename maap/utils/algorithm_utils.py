import logging
import json

import yaml
from yaml import load as yaml_load, dump as yaml_dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


logger = logging.getLogger(__name__)


def read_yaml_file(algo_yaml):
    algo_config = dict()
    with open(algo_yaml, 'r') as fr:
        algo_config = yaml_load(fr, Loader=Loader)
    return validate_algorithm_config(algo_config)

def read_cwl_file(algo_cwl):
    """
    Parse through the CWL file and returns the response as the POST to register a 
    a process for OGC is expecting 
    https://github.com/MAAP-Project/joint-open-api-specs/blob/nasa-adaptation/ogc-api-processes/openapi-template/schemas/processes-core/postProcess.yaml
    """
    try:
        with open(algo_cwl, 'r') as f:
            cwl_data = yaml.safe_load(f)
        print(f"Successfully read and parsed '{algo_cwl}'")
        return parse_cwl_data(cwl_data)
    except FileNotFoundError:
        print(f"Error: The file '{algo_cwl}' was not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing the YAML in '{algo_cwl}': {e}")
        return None

def parse_cwl_data(cwl_data):
    algo_config = dict()
    # TODO implement this and return cwl_data as a dictionary with important variables like
    # the API is expecting 
    return algo_config

def validate_algorithm_config(algo_config):
    return algo_config
