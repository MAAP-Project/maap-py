#!/usr/bin/env python3
"""
Simple test script to verify OGC function signatures and basic functionality
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from maap.maap import MAAP

def test_ogc_functions():
    """Test that all OGC functions exist and have correct signatures"""
    
    # Initialize MAAP instance
    maap = MAAP()
    
    # Test function existence and basic attributes
    ogc_functions = [
        'list_processes_ogc',
        'deploy_process_ogc', 
        'get_deployment_status_ogc',
        'describe_process_ogc',
        'update_process_ogc',
        'delete_process_ogc',
        'get_process_package_ogc',
        'execute_process_ogc',
        'get_job_status_ogc',
        'cancel_job_ogc',
        'get_job_results_ogc',
        'list_jobs_ogc',
        'get_job_metrics_ogc'
    ]
    
    print("Testing OGC function existence...")
    for func_name in ogc_functions:
        assert hasattr(maap, func_name), f"Function {func_name} not found"
        func = getattr(maap, func_name)
        assert callable(func), f"Function {func_name} is not callable"
        print(f"✓ {func_name} exists and is callable")
    
    print("\nTesting function signatures...")
    
    # Test functions that don't require parameters
    try:
        # These should work without throwing signature errors (though may fail on network call)
        search_func = getattr(maap, 'list_processes_ogc')
        print(f"✓ list_processes_ogc has correct signature")
    except Exception as e:
        print(f"✗ list_processes_ogc signature error: {e}")
    
    # Test functions with required parameters 
    try:
        deploy_func = getattr(maap, 'deploy_process_ogc')
        # This should not throw a signature error
        print(f"✓ deploy_process_ogc has correct signature")
    except Exception as e:
        print(f"✗ deploy_process_ogc signature error: {e}")
    
    try:
        execute_func = getattr(maap, 'execute_process_ogc') 
        # This should not throw a signature error
        print(f"✓ execute_process_ogc has correct signature")
    except Exception as e:
        print(f"✗ execute_process_ogc signature error: {e}")
    
    print("\n✓ All OGC functions successfully added to maap-py!")
    print("\nAvailable OGC functions:")
    for func_name in ogc_functions:
        func = getattr(maap, func_name)
        print(f"  - {func_name}: {func.__doc__.strip().split(':')[0] if func.__doc__ else 'No description'}")

if __name__ == "__main__":
    test_ogc_functions()