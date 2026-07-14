#!/usr/bin/env pytest

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from htcondor_cli.snake import Status

@pytest.fixture
def mock_schedd():
    """
    Patch htcondor2.Schedd and yield the mock instance so tests can configure
    schedd.act()'s return value/side effect and assert on calls.
    """
    with patch("htcondor2.Schedd") as mock_schedd_class:
        mock_schedd_instance = MagicMock()
        mock_schedd_class.return_value = mock_schedd_instance
        yield mock_schedd_instance

class TestSnakemakeStatus:
    """Test the status class"""

    def test_status_without_mgmt_id():
        """Test behavior when mgmt_id is not provided"""
        with pytest.raise(SystemExit):
            Status(logger=None, mgmt_id=None)
        mock_schedd.act.assert_not_called()
    
    def test_get_mgmt_info_schedd_exception():
        """Test that exception is caught appropriately if calling schedd failed"""
        mock_schedd.act.side_effect = RuntimeError("Schedd connection failed")

        # skipping __init__ as we are testing only one helper method
        status = Status.__new__(Status)
        res = status._get_mgmt_job_info("2604")

        assert res is None
        captured = capsys.readouterr() # built in pytest fixture
        assert "Could not query schedd" in captured.out

    def test_get_mgmt_info_no_jobs_found():

        pass

    def test_status_pointer_file_not_found():
        pass
    
    def test_status_metadata_file_not_found():
        pass
    def test_status_malformed_metadata_json():
        pass
    
    def test_status_valid_full_flow():
        pass