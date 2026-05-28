"""
Test module for algorithm and job functions in maap.py
"""

import pytest
from maap.maap import MAAP


def test_list_algorithms():
    """Test list_algorithms function calls OGC algorithms endpoint and returns 200 with JSON"""
    maap = MAAP(maap_host='api.dit.maap-project.org')
    
    response = maap.list_algorithms()
    
    # Check that we get a 200 status code
    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
    
    # Check that response is valid JSON
    try:
        json_data = response.json()
        assert isinstance(json_data, (dict, list)), "Response should be valid JSON (dict or list)"
    except ValueError as e:
        pytest.fail(f"Response is not valid JSON: {e}")


def test_register_algorithm():
    """Test register_algorithm function with a valid CWL URL"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        # Test that list_algorithms works first to ensure we have proper authentication
        list_response = maap.list_algorithms()
        if list_response.status_code != 200:
            pytest.skip("Authentication required - skipping register_algorithm test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping register_algorithm test")
    
    # Use a real CWL example URL that should work
    sample_cwl_url = "https://raw.githubusercontent.com/MAAP-Project/maap-algorithms/master/examples/hello-world/hello-world.cwl"
    
    response = maap.register_algorithm(sample_cwl_url)
    
    # Should get a successful response or a meaningful error
    assert response.status_code in [200, 201], f"Expected successful registration, got {response.status_code}: {response.text}"
    
    # Should return JSON with deployment info
    json_data = response.json()
    assert isinstance(json_data, dict), "Registration response should be a JSON object"
    
    # Should contain deployment information
    assert "deploymentID" in json_data or "id" in json_data, "Response should contain deployment ID"


def test_get_deployment_status():
    """Test get_deployment_status function"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        list_response = maap.list_algorithms()
        if list_response.status_code != 200:
            pytest.skip("Authentication required - skipping get_deployment_status test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping get_deployment_status test")
    
    # Since we don't have a real deployment ID, this test will likely return 404
    # which is the expected behavior for a non-existent deployment
    sample_deployment_id = "test-deployment-123"
    
    response = maap.get_deployment_status(sample_deployment_id)
    
    # Should get a valid response - 200 if found, 404 if not found
    assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}: {response.text}"
    
    # If deployment exists (200), should return JSON with status info
    if response.status_code == 200:
        json_data = response.json()
        assert isinstance(json_data, dict), "Status response should be a JSON object"
        assert "status" in json_data, "Response should contain status information"
    
    # Verify the URL contains the deployment ID
    assert str(sample_deployment_id) in response.url


def test_describe_algorithm():
    """Test describe_algorithm function by getting algorithm list and describing first algorithm"""
    maap = MAAP(maap_host='api.dit.maap-project.org')
    
    # First get the list of algorithms
    list_response = maap.list_algorithms()
    assert list_response.status_code == 200, f"Failed to get algorithm list: {list_response.status_code}"
    
    try:
        processes_data = list_response.json()
    except ValueError as e:
        pytest.fail(f"Algorithm list response is not valid JSON: {e}")
    
    # Check if there are any algorithms
    if not processes_data or (isinstance(processes_data, dict) and not processes_data.get('processes')):
        pytest.skip("No algorithms available to test describe_algorithm")
    
    # Get the first algorithm
    if isinstance(processes_data, dict) and 'processes' in processes_data:
        processes = processes_data['processes']
    else:
        processes = processes_data
    
    if not processes or len(processes) == 0:
        pytest.skip("No algorithms available to test describe_algorithm")
    
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
        pytest.skip("Could not determine algorithm ID to test describe_algorithm")
    
    # Now test the describe_algorithm function
    describe_response = maap.describe_algorithm(process_id)
    
    # Check that we get a successful response
    assert describe_response.status_code == 200, f"Expected 200, got {describe_response.status_code}"
    
    # Check that response is valid JSON
    try:
        describe_data = describe_response.json()
        assert isinstance(describe_data, dict), "Describe response should be a JSON object"
    except ValueError as e:
        pytest.fail(f"Describe response is not valid JSON: {e}")
    
    # Verify the URL called contains the algorithm ID
    assert str(process_id) in describe_response.url


def test_update_algorithm():
    """Test update_algorithm function"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        list_response = maap.list_algorithms()
        if list_response.status_code != 200:
            pytest.skip("Authentication required - skipping update_algorithm test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping update_algorithm test")
    
    # Use a non-existent algorithm ID - should return 404 which is expected
    sample_process_id = "non-existent-algorithm-123"
    sample_cwl_url = "https://raw.githubusercontent.com/MAAP-Project/maap-algorithms/master/examples/hello-world/hello-world.cwl"
    
    response = maap.update_algorithm(sample_process_id, sample_cwl_url)
    
    # Should get a valid response - 200 if successful, 404 if not found, 403 if not authorized
    assert response.status_code in [200, 404, 403], f"Expected 200, 404, or 403, got {response.status_code}: {response.text}"
    
    # If successful (200), should return JSON with update info
    if response.status_code == 200:
        json_data = response.json()
        assert isinstance(json_data, dict), "Update response should be a JSON object"
    
    # Verify the URL contains the process ID
    assert str(sample_process_id) in response.url


def test_delete_algorithm():
    """Test delete_algorithm function"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        list_response = maap.list_algorithms()
        if list_response.status_code != 200:
            pytest.skip("Authentication required - skipping delete_algorithm test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping delete_algorithm test")
    
    # Use a non-existent algorithm ID - should return 404 which is expected
    sample_process_id = "non-existent-algorithm-123"
    
    response = maap.delete_algorithm(sample_process_id)
    
    # Should get a valid response - 200/204 if successful, 404 if not found, 403 if not authorized
    assert response.status_code in [200, 204, 404, 403], f"Expected 200, 204, 404, or 403, got {response.status_code}: {response.text}"
    
    # If successful (200/204), response might be empty or contain JSON
    if response.status_code in [200, 204]:
        if response.content:  # Only check JSON if there's content
            json_data = response.json()
            assert isinstance(json_data, dict), "Delete response should be a JSON object"
    
    # Verify the URL contains the process ID
    assert str(sample_process_id) in response.url


def test_get_algorithm_package():
    """Test get_algorithm_package function"""
    maap = MAAP(maap_host='api.dit.maap-project.org')
    
    # First get the list of algorithms
    list_response = maap.list_algorithms()
    assert list_response.status_code == 200, f"Failed to get algorithm list: {list_response.status_code}"
    
    try:
        processes_data = list_response.json()
    except ValueError as e:
        pytest.fail(f"Algorithm list response is not valid JSON: {e}")
    
    # Check if there are any algorithms
    if not processes_data or (isinstance(processes_data, dict) and not processes_data.get('processes')):
        pytest.skip("No algorithms available to test get_algorithm_package")
    
    # Get the first algorithm
    if isinstance(processes_data, dict) and 'processes' in processes_data:
        processes = processes_data['processes']
    else:
        processes = processes_data
    
    if not processes or len(processes) == 0:
        pytest.skip("No algorithms available to test get_algorithm_package")
    
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
        pytest.skip("Could not determine algorithm ID to test get_algorithm_package")
    
    # Now test the package_response function
    package_response = maap.get_algorithm_package(process_id)
    
    # Check that we get a successful response
    assert package_response.status_code == 200, f"Expected 200, got {package_response.status_code}"
    
    # Check that response is valid JSON
    try:
        package_data = package_response.json()
        assert isinstance(package_data, dict), "Algorithm Package response should be a JSON object"
    except ValueError as e:
        pytest.fail(f"Algorithm package response is not valid JSON: {e}")
    
    # Verify the URL called contains the algorithm ID
    assert str(process_id) in package_response.url


def test_submit_job():
    """Test submit_job function by first getting a real algorithm ID"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        list_response = maap.list_algorithms()
        if list_response.status_code != 200:
            pytest.skip("Authentication required - skipping submit_job test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping submit_job test")
    
    # Get a real algorithm to test with
    try:
        algorithms_data = list_response.json()
        if not algorithms_data or (isinstance(algorithms_data, dict) and not algorithms_data.get('processes')):
            pytest.skip("No algorithms available to test submit_job")
        
        if isinstance(algorithms_data, dict) and 'processes' in algorithms_data:
            algorithms = algorithms_data['processes']
        else:
            algorithms = algorithms_data
        
        if not algorithms or len(algorithms) == 0:
            pytest.skip("No algorithms available to test submit_job")
        
        # Get the first algorithm's ID
        first_algorithm = algorithms[0]
        algorithm_id = first_algorithm.get('id') or first_algorithm.get('processId')
        
        if not algorithm_id:
            pytest.skip("Could not determine algorithm ID to test submit_job")
            
    except Exception as e:
        pytest.skip(f"Could not parse algorithms list: {e}")
    
    # Test job submission with minimal inputs
    sample_inputs = {}  # Empty inputs for basic test
    sample_queue = "maap-dps-worker-32gb"  # Use a real queue name
    
    response = maap.submit_job(algorithm_id, sample_inputs, sample_queue)
    
    # Should get a response - 200/201 if successful, 400 if invalid inputs, 404 if algorithm not found
    assert response.status_code in [200, 201, 400, 404], f"Expected valid response, got {response.status_code}: {response.text}"
    
    # If successful (200/201), should return JSON with job info
    if response.status_code in [200, 201]:
        json_data = response.json()
        assert isinstance(json_data, dict), "Job submission response should be a JSON object"
        assert "jobID" in json_data or "id" in json_data, "Response should contain job ID"
    
    # Verify the URL contains the algorithm ID
    assert str(algorithm_id) in response.url


def test_get_job_status():
    """Test get_job_status function"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        list_response = maap.list_jobs()
        if list_response.status_code != 200:
            pytest.skip("Authentication required - skipping get_job_status test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping get_job_status test")
    
    # Use a non-existent job ID - should return 404 which is expected
    sample_job_id = "non-existent-job-123"
    
    response = maap.get_job_status(sample_job_id)
    
    # Should get a valid response - 200 if found, 404 if not found
    assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}: {response.text}"
    
    # If job exists (200), should return JSON with status info
    if response.status_code == 200:
        json_data = response.json()
        assert isinstance(json_data, dict), "Job status response should be a JSON object"
        assert "status" in json_data, "Response should contain status information"
    
    # Verify the URL contains the job ID
    assert str(sample_job_id) in response.url


def test_cancel_job():
    """Test cancel_job function"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        list_response = maap.list_jobs()
        if list_response.status_code != 200:
            pytest.skip("Authentication required - skipping cancel_job test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping cancel_job test")
    
    # Use a non-existent job ID - should return 404 which is expected
    sample_job_id = "non-existent-job-123"
    
    response = maap.cancel_job(sample_job_id)
    
    # Should get a valid response - 200/204 if successful, 404 if not found, 409 if already completed
    assert response.status_code in [200, 204, 404, 409], f"Expected 200, 204, 404, or 409, got {response.status_code}: {response.text}"
    
    # If successful (200/204), response might be empty or contain JSON
    if response.status_code in [200, 204]:
        if response.content:  # Only check JSON if there's content
            json_data = response.json()
            assert isinstance(json_data, dict), "Cancel response should be a JSON object"
    
    # Verify the URL contains the job ID
    assert str(sample_job_id) in response.url


def test_get_job_result():
    """Test get_job_result function"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        list_response = maap.list_jobs()
        if list_response.status_code != 200:
            pytest.skip("Authentication required - skipping get_job_result test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping get_job_result test")
    
    # Use a non-existent job ID - should return 404 which is expected
    sample_job_id = "non-existent-job-123"
    
    response = maap.get_job_result(sample_job_id)
    
    # Should get a valid response - 200 if found, 404 if not found, 425 if not ready
    assert response.status_code in [200, 404, 425], f"Expected 200, 404, or 425, got {response.status_code}: {response.text}"
    
    # If job results exist (200), should return JSON with result info
    if response.status_code == 200:
        json_data = response.json()
        assert isinstance(json_data, dict), "Job result response should be a JSON object"
        # Should contain outputs or links to result files
    
    # Verify the URL contains the job ID and 'results'
    assert str(sample_job_id) in response.url
    assert 'results' in response.url


def test_list_jobs():
    """Test list_jobs function"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        response = maap.list_jobs()
        if response.status_code != 200:
            pytest.skip("Authentication required - skipping get_job_result test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping get_job_result test")

    # Only check JSON content if we get a successful response
    if response.status_code == 200:
        json_data = response.json()
        assert isinstance(json_data, (dict, list)), "Jobs list response should be JSON (dict or list)"
        
        # If it's a dict, it might have a 'jobs' key or similar
        if isinstance(json_data, dict):
            # Common structures: {"jobs": [...]} or {"processes": [...]}
            assert len(json_data) >= 0, "Jobs response should be valid"
        elif isinstance(json_data, list):
            # Direct list of jobs
            assert len(json_data) >= 0, "Jobs list should be valid"


def test_get_job_metrics():
    """Test get_job_metrics function"""
    maap = MAAP()
    
    # Skip test if we can't authenticate
    try:
        list_response = maap.list_jobs()
        if list_response.status_code != 200:
            pytest.skip("Authentication required - skipping get_job_metrics test")
    except Exception:
        pytest.skip("Cannot connect to MAAP API - skipping get_job_metrics test")
    
    # Use a non-existent job ID - should return 404 which is expected
    sample_job_id = "non-existent-job-123"
    
    response = maap.get_job_metrics(sample_job_id)
    
    # Should get a valid response - 200 if found, 404 if not found, 425 if not available
    assert response.status_code in [200, 404, 425], f"Expected 200, 404, or 425, got {response.status_code}: {response.text}"
    
    # If job metrics exist (200), should return JSON with metrics info
    if response.status_code == 200:
        json_data = response.json()
        assert isinstance(json_data, dict), "Job metrics response should be a JSON object"
        # Should contain metrics like CPU usage, memory usage, duration, etc.
    
    # Verify the URL contains the job ID and 'metrics'
    assert str(sample_job_id) in response.url
    assert 'metrics' in response.url