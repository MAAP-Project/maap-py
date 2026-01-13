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

def read_cwl_file(algo_cwl):
    """
    Parse through the CWL file and returns the response as a JSON for the POST to register a 
    a process for OGC is expecting 
    https://github.com/MAAP-Project/joint-open-api-specs/blob/nasa-adaptation/ogc-api-processes/openapi-template/schemas/processes-core/postProcess.yaml
    """
    try:
        with open(algo_cwl, 'r') as f:
            cwl_text = f.read()
        try:
            cwl_obj = load_document_by_uri(algo_cwl, load_all=True)
        except Exception as e:
            print(f"Failed to parse CWL: {e}")
            raise ValueError("CWL file is not in the right format or is invalid.")
        print("graceal1 successfully got cwl object and data")
        print(cwl_obj)
        print(cwl_text)
        return parse_cwl_data(cwl_obj, cwl_text)
    except FileNotFoundError:
        print(f"Error: The file '{algo_cwl}' was not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing the YAML in '{algo_cwl}': {e}")
        return None


def parse_cwl_data(cwl_obj, cwl_text):
    """
    Pass the cwl object for essential arguments
    Assigning of default values is in the API 
    
    :param cwl_obj: Object of CWL file from cwl_utils
    :param cwl_text: Plain text of CWL file 
    """
    workflow = next((obj for obj in cwl_obj if isinstance(obj, cwl_v1_2.Workflow)), None)
    if not workflow:
        raise ValueError("A valid Workflow object must be defined in the CWL file.")

    cwl_id = workflow.id
    version_match = re.search(r"s:version:\s*(\S+)", cwl_text, re.IGNORECASE)
    
    if not version_match or not cwl_id:
        raise ValueError("Required metadata missing: s:version and a top-level id are required.")

    fragment = urllib.parse.urlparse(cwl_id).fragment
    cwl_id = os.path.basename(fragment)
    process_version = version_match.group(1)

    if ":" in process_version:
        raise ValueError("Process version cannot contain a :")

    # Get git information
    github_url = re.search(r"s:codeRepository:\s*(\S+)", cwl_text, re.IGNORECASE)
    github_url = github_url.group(1) if github_url else None
    git_commit_hash = re.search(r"s:commitHash:\s*(\S+)", cwl_text, re.IGNORECASE)
    git_commit_hash = git_commit_hash.group(1) if git_commit_hash else None

    keywords_match = re.search(r"s:keywords:\s*(.*)", cwl_text, re.IGNORECASE)
    keywords = keywords_match.group(1).replace(" ", "") if keywords_match else None

    try:
        author_match = re.search(
            r"s:author:.*?s:name:\s*(\S+)",
            cwl_text,
            re.DOTALL | re.IGNORECASE
        )
        author = author_match.group(1) if author_match else None
    except Exception as e:
        author = None
        print(f"Failed to get author name: {e}")

    # Initialize optional variables
    ram_min = None
    cores_min = None
    base_command = None

    # Find the CommandLineTool run by the first step of the workflow
    if workflow.steps:
        # Get the ID of the tool to run (e.g., '#main')
        tool_id_ref = workflow.steps[0].run
        # The actual ID is the part after the '#'
        tool_id = os.path.basename(tool_id_ref)

        # Find the CommandLineTool object in the parsed CWL graph
        command_line_tool = next((obj for obj in cwl_obj if isinstance(obj, cwl_v1_2.CommandLineTool) and obj.id.endswith(tool_id)), None)
    
        if command_line_tool:
            # Extract the baseCommand directly
            base_command = command_line_tool.baseCommand
    
            # Find the ResourceRequirement to extract ramMin and coresMin
            if command_line_tool.requirements:
                for req in command_line_tool.requirements:
                    if isinstance(req, cwl_v1_2.ResourceRequirement):
                        ram_min = req.ramMin if req.ramMin else ram_min
                        cores_min = req.coresMin if req.coresMin else cores_min
                        break  # Stop after finding the first ResourceRequirement

    # Build dictionary with all extracted variables
    process_config = {
        'id': cwl_id,
        'version': process_version,
        'title': workflow.label,
        'description': workflow.doc,
        'keywords': keywords,
        'raw_text': cwl_text,
        'github_url': github_url,
        'git_commit_hash': git_commit_hash,
        'ram_min': ram_min,
        'cores_min': cores_min,
        'base_command': base_command,
        'author': author
    }

    return process_config

def validate_algorithm_config(algo_config):
    return algo_config
