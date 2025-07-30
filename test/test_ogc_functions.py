"""
Test module for OGC functions in maap.py
"""

import pytest
from maap.maap import MAAP


def test_list_processes_ogc():
    """Test list_processes_ogc function calls OGC processes endpoint and returns 200 with JSON"""
    maap = MAAP(maap_host='api.dit.maap-project.org')
    
    response = maap.list_processes_ogc()
    
    # Check that we get a 200 status code
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    
    # Check that response is valid JSON
    try:
        json_data = response.json()
        assert isinstance(json_data, (dict, list)), "Response should be valid JSON (dict or list)"
    except ValueError as e:
        pytest.fail(f"Response is not valid JSON: {e}")


def test_deploy_process_ogc():
    """Test deploy_process_ogc function"""
    pass


def test_get_deployment_status_ogc():
    """Test get_deployment_status_ogc function"""
    pass


def test_describe_process_ogc():
    """Test describe_process_ogc function by getting process list and describing first process"""
    maap = MAAP(maap_host='api.dit.maap-project.org')
    
    # First get the list of processes
    list_response = maap.list_processes_ogc()
    assert list_response.status_code == 200, f"Failed to get process list: {list_response.status_code}"
    
    try:
        processes_data = list_response.json()
    except ValueError as e:
        pytest.fail(f"Process list response is not valid JSON: {e}")
    
    # Check if there are any processes
    if not processes_data or (isinstance(processes_data, dict) and not processes_data.get('processes')):
        pytest.skip("No processes available to test describe_process_ogc")
    
    # Get the first process
    if isinstance(processes_data, dict) and 'processes' in processes_data:
        processes = processes_data['processes']
    else:
        processes = processes_data
    
    if not processes or len(processes) == 0:
        pytest.skip("No processes available to test describe_process_ogc")
    
    first_process = processes[0]
    
    # Find the self link or use process ID
    process_id = None
    if 'links' in first_process:
        for link in first_process['links']:
            if link.get('rel') == 'self':
                href = link.get('href', '')
                # Extract process ID from href like /ogc/processes/3
                if '/ogc/processes/' in href:
                    process_id = href.split('/ogc/processes/')[-1]
                break
    
    # Fall back to process ID field if no self link found
    if not process_id and 'id' in first_process:
        process_id = first_process['id']
    
    if not process_id:
        pytest.skip("Could not determine process ID to test describe_process_ogc")
    
    # Now test the describe_process_ogc function
    describe_response = maap.describe_process_ogc(process_id)
    
    # Check that we get a successful response
    assert describe_response.status_code == 200, f"Expected 200, got {describe_response.status_code}"
    
    # Check that response is valid JSON
    try:
        describe_data = describe_response.json()
        assert isinstance(describe_data, dict), "Describe response should be a JSON object"
    except ValueError as e:
        pytest.fail(f"Describe response is not valid JSON: {e}")
    
    # Verify the URL called contains the process ID
    assert str(process_id) in describe_response.url


def test_update_process_ogc():
    """Test update_process_ogc function"""
    pass


def test_delete_process_ogc():
    """Test delete_process_ogc function"""
    pass


def test_get_process_package_ogc():
    """Test get_process_package_ogc function"""
    maap = MAAP(maap_host='api.dit.maap-project.org')
    
    # First get the list of processes
    list_response = maap.list_processes_ogc()
    assert list_response.status_code == 200, f"Failed to get process list: {list_response.status_code}"
    
    try:
        processes_data = list_response.json()
    except ValueError as e:
        pytest.fail(f"Process list response is not valid JSON: {e}")
    
    # Check if there are any processes
    if not processes_data or (isinstance(processes_data, dict) and not processes_data.get('processes')):
        pytest.skip("No processes available to test describe_process_ogc")
    
    # Get the first process
    if isinstance(processes_data, dict) and 'processes' in processes_data:
        processes = processes_data['processes']
    else:
        processes = processes_data
    
    if not processes or len(processes) == 0:
        pytest.skip("No processes available to test describe_process_ogc")
    
    first_process = processes[0]
    
    # Find the self link or use process ID
    process_id = None
    if 'links' in first_process:
        for link in first_process['links']:
            if link.get('rel') == 'self':
                href = link.get('href', '')
                # Extract process ID from href like /ogc/processes/3
                if '/ogc/processes/' in href:
                    process_id = href.split('/ogc/processes/')[-1]
                break
    
    # Fall back to process ID field if no self link found
    if not process_id and 'id' in first_process:
        process_id = first_process['id']
    
    if not process_id:
        pytest.skip("Could not determine process ID to test describe_process_ogc")
    
    # Now test the package_response function
    package_response = maap.get_process_package_ogc(process_id)
    
    # Check that we get a successful response
    assert package_response.status_code == 200, f"Expected 200, got {package_response.status_code}"
    
    # Check that response is valid JSON
    try:
        package_data = package_response.json()
        assert isinstance(package_data, dict), "Process Package response should be a JSON object"
    except ValueError as e:
        pytest.fail(f"Process package response is not valid JSON: {e}")
    
    # Verify the URL called contains the process ID
    assert str(process_id) in package_response.url


def test_execute_process_ogc():
    """Test execute_process_ogc function"""
    pass


def test_get_job_status_ogc():
    """Test get_job_status_ogc function"""
    pass


def test_cancel_job_ogc():
    """Test cancel_job_ogc function"""
    pass


def test_get_job_results_ogc():
    """Test get_job_results_ogc function"""
    pass


def test_list_jobs_ogc():
    """Test list_jobs_ogc function"""
    pass


def test_get_job_metrics_ogc():
    """Test get_job_metrics_ogc function"""
    pass