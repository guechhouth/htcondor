#!/usr/bin/env pytest

import pytest
from unittest.mock import patch, MagicMock
from htcondor_cli.snake import Status
import json

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

    def test_status_without_mgmt_id(self, mock_schedd):
        """Test behavior when mgmt_id is not provided"""
        with pytest.raises(SystemExit):
            Status(logger=None, mgmt_id=None)
        mock_schedd.query.assert_not_called()
    
    def test_get_mgmt_info_schedd_exception(self, mock_schedd):
        """Test that exception is caught appropriately if calling schedd failed"""
        mock_schedd.query.side_effect = RuntimeError("Schedd connection failed")
        mock_logger = MagicMock()

        # skipping __init__ as we are testing only one helper method
        status = Status.__new__(Status)
        status.logger = mock_logger # set directly
        res = status._get_mgmt_job_info("2604")

        assert res is None
        mock_logger.warning.assert_called_once() # could not query schedd

    def test_get_mgmt_info_no_jobs_found(self, mock_schedd):
        """Test job that does not exist"""
        mock_schedd.query.return_value = []

        status = Status.__new__(Status)
        res = status._get_mgmt_job_info("9999")

        assert res is None
    
    def test_status_no_metadata_pointer_on_job_ad(self, mock_schedd, capsys):
        """Test behavior when the management job ad has no HTCondorSnakeMetadata attribute"""
        mock_schedd.query.return_value = [{"JobStatus": 2}]
        mock_logger = MagicMock()

        with pytest.raises(SystemExit):
            Status(logger=mock_logger, mgmt_id="2604")

        mock_logger.error.assert_called_once() # No metadata file path found

    def test_status_metadata_file_not_found(self, mock_schedd, tmp_path):
        """Test metadata file not found even though the job ad has a pointer"""
        mock_logger = MagicMock()
        jobdir = tmp_path / "logs"
        jobdir.mkdir()
        metadata_path = jobdir / "snakemake-metadata-2604.json"
        mock_schedd.query.return_value = [{"HTCondorSnakeMetadata": str(metadata_path)}]

        with pytest.raises(SystemExit):
            Status(logger=mock_logger, mgmt_id="2604")

        mock_logger.error.assert_called_once() # Metadata not found

    def test_status_malformed_metadata_json(self, mock_schedd, tmp_path):
        """Test metadata file containing invalid JSON"""
        mock_logger = MagicMock()
        jobdir = tmp_path / "logs"
        jobdir.mkdir()
        metadata_path = jobdir / "snakemake-metadata-2604.json"
        metadata_path.write_text("{not a valid json}")
        mock_schedd.query.return_value = [{"HTCondorSnakeMetadata": str(metadata_path)}]

        Status(logger=mock_logger, mgmt_id="2604")

        mock_logger.error.assert_called_once() # could not get status for job

    def test_status_valid_full_flow(self, mock_schedd, tmp_path):
        """Test when normal working behavior"""
        jobdir = tmp_path / "logs"
        jobdir.mkdir()
        metadata = {"dag_nodes": 3, "jobs": {}}
        metadata_path = jobdir / "snakemake-metadata-2604.json"
        metadata_path.write_text(json.dumps(metadata))
        mock_schedd.query.return_value = [{"HTCondorSnakeMetadata": str(metadata_path)}]

        with patch.object(Status, "_show_status") as mock_show:
            Status(logger=None, mgmt_id="2604")

        mock_show.assert_called_once()
        args, kwargs = mock_show.call_args
        assert args[0] == "2604"
        assert args[1] == metadata
