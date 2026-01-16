# Note: This is not intended to be a unit test but rather a functional test
# that will connect to a backend api to perform actions and report output

import json
import os

from maap.maap import MAAP
import logging
import sys
import time

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S",
    stream=sys.stdout)


def log_decorator(func):
    def wrapper_log(*args, **kwargs):
        logging.info(f"Calling {func.__name__} with args {args} {json.dumps(kwargs)}")
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error("Error calling function {}".format(func.__name__))
            logging.exception(e)
            raise e

    return wrapper_log


@log_decorator
def configure_maap():
    maap = MAAP(maap_host=os.environ.get('MAAP_HOST', 'api.dit.maap-project.org'))
    assert maap is not None
    return maap


@log_decorator
def register_algorithm(maap: MAAP, filepath):
    resp = maap.register_algorithm_from_yaml_file(filepath)
    logging.debug(resp.json())
    assert resp.status_code == 200
    assert resp.json().get('message').get('web_url')


@log_decorator
def list_algorithms(maap: MAAP):
    resp = maap.listAlgorithms()
    logging.debug(resp.json())
    assert resp.status_code == 200
    algorithms = resp.json().get('algorithms')
    for algorithm in algorithms:
        algorithm_id = f"{algorithm.get('type')}:{algorithm.get('version')}"
        describe_algorithm(maap, algorithm_id)
        break


@log_decorator
def describe_algorithm(maap: MAAP, algorithm_id):
    resp = maap.describeAlgorithm(algorithm_id)
    assert resp.status_code == 200


@log_decorator
def submit_job(maap: MAAP, wait_for_completion=False, queue="maap-dps-worker-8gb"):
    # This is assuming maap_functional_test_algo already registered
    # TODO wait for registration to complete successfully before submitting job
    algo_name = "maap_functional_test_algo"
    algo_version = "main"
    describe_algorithm(maap, f"{algo_name}:{algo_version}")
    job_inputs = {
        "input_file": "https://photojournal.jpl.nasa.gov/tiff/PIA00127.tif",
        "output_filename": "output.tif",
        "outsize": "20"
    }
    job = maap.submit_job(identifier="maap_functional_test",
                         algo_id=algo_name,
                         version=algo_version,
                         queue=queue,
                         **job_inputs)
    print(job)
    assert job is not None
    # Sleep for 5s for ES to commit document so that we can query for status or else we might get None sometimes
    time.sleep(5)
    assert str(job.retrieve_attributes().status).lower() in ["accepted", "running", "success"]
    if wait_for_completion:
        job.wait_for_completion()
        assert job.retrieve_result() is not None
    return job


@log_decorator
def delete_algorithm(maap: MAAP, algorithm_id="maap_functional_test_algo:main"):
    resp = maap.deleteAlgorithm(algorithm_id)
    assert resp.status_code == 200


@log_decorator
def cancel_job(maap: MAAP, job_id):
    resp = maap.cancelJob(job_id)
    print(resp)
    assert resp is not None
    assert 'Accepted' in str(resp)

@log_decorator
def add_secret(maap: MAAP, secret_name=None, secret_value=None):
    resp = maap.secrets.add_secret(secret_name, secret_value)
    print(resp)
    assert resp is not None

@log_decorator
def get_secrets(maap: MAAP):
    resp = maap.secrets.get_secrets()
    print(resp)
    assert resp is not None

@log_decorator
def get_secret(maap: MAAP, secret_name=None, secret_value=None):
    resp = maap.secrets.get_secret(secret_name)
    print(resp)
    assert resp is not None
    assert resp == secret_value

@log_decorator
def delete_secret(maap: MAAP, secret_name=None):
    resp = maap.secrets.delete_secret(secret_name)
    print(resp)
    assert resp is not None


def main():
    if os.environ.get('MAAP_PGT') is None:
        print("MAAP_PGT environment variable is not set")
        exit(1)
    maap = configure_maap()
    # register_algorithm(maap, "dps_test_algo_config.yaml")
    # list_algorithms(maap)
    job = submit_job(maap, queue="maap-dps-sandbox")
    cancel_job(maap, job.id)

    # Test secrets management
    secret_name = "test_secret"
    secret_value = "test_value"
    get_secrets(maap)
    add_secret(maap, secret_name, secret_value)
    get_secret(maap, secret_name, secret_value)
    delete_secret(maap, secret_name)

    # submit_job(maap, wait_for_completion=True)
    # delete_algorithm(maap, "maap_functional_test_algo:main")



if __name__ == '__main__':
    main()
