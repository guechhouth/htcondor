#!/usr/bin/env pytest

import pytest
from pathlib import Path
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
    
    def test_get_mgmt_info_schedd_exception(self, mock_schedd, capsys):
        """Test that exception is caught appropriately if calling schedd failed"""
        mock_schedd.query.side_effect = RuntimeError("Schedd connection failed")

        # skipping __init__ as we are testing only one helper method
        status = Status.__new__(Status)
        res = status._get_mgmt_job_info("2604")

        assert res is None
        captured = capsys.readouterr() # built in pytest fixture
        assert "Could not query schedd" in captured.out

    def test_get_mgmt_info_no_jobs_found(self, mock_schedd):
        """Test job that does not exist"""
        mock_schedd.query.return_value = []

        status = Status.__new__(Status)
        res = status._get_mgmt_job_info("9999")

        assert res is None

    def test_status_pointer_file_not_found(self, mock_schedd, capsys, tmp_path):
        """Test pointer file missing"""
        mock_schedd.query.return_value = []

        with patch("os.getcwd", return_value=str(tmp_path)):
            with pytest.raises(SystemExit):
                Status(logger=None, mgmt_id="2604")
        
        captured = capsys.readouterr()
        assert "No workflow pointer found" in captured.out
    
    def test_status_metadata_file_not_found(self, mock_schedd, capsys, tmp_path):
        """Test metadata file not found although the pointer exist"""
        mock_schedd.query.return_value = []
        pointer_dir = tmp_path/".snakemake"/"htcondor"
        pointer_dir.mkdir(parents=True)
        jobdir = tmp_path/"logs"
        jobdir.mkdir()
        (pointer_dir/"snakemake-htcondor-2604.json").write_text(json.dumps({"jobdir": str(jobdir)}))

        with patch("os.getcwd", return_value=str(tmp_path)):
            with pytest.raises(SystemExit):
                Status(logger=None, mgmt_id="2604")
        
        captured = capsys.readouterr()
        assert "Metadata not found" in captured.out
        
    def test_status_malformed_metadata_json(self, mock_schedd, capsys, tmp_path):
        """Test metadata file containing invalid JSON"""
        mock_schedd.query.return_value = []
        pointer_dir = tmp_path/".snakemake"/"htcondor"
        pointer_dir.mkdir(parents=True)
        jobdir = tmp_path/"logs"
        jobdir.mkdir()
        (pointer_dir/"snakemake-htcondor-2604.json").write_text(json.dumps({"jobdir": str(jobdir)}))

        (jobdir/"snakemake-metadata-2604.json").write_text("{not a valid json}")

        with patch("os.getcwd", return_value=str(tmp_path)):
            Status(logger=None, mgmt_id="2604")

        captured = capsys.readouterr()
        assert "Could not get status" in captured.out
    
    def test_status_valid_full_flow(self, mock_schedd, tmp_path):
        """Test when normal working behavior"""
        mock_schedd.query.return_value = []
        pointer_dir = tmp_path / ".snakemake" / "htcondor"
        pointer_dir.mkdir(parents=True)
        jobdir = tmp_path / "logs"
        jobdir.mkdir()
        (pointer_dir / "snakemake-htcondor-2604.json").write_text(
            json.dumps({"jobdir": str(jobdir)})
        )
        metadata = {"dag_nodes": 3, "jobs": {}}
        (jobdir / "snakemake-metadata-2604.json").write_text(json.dumps(metadata))

        with patch("os.getcwd", return_value=str(tmp_path)):
            with patch.object(Status, "_show_status") as mock_show:
                Status(logger=None, mgmt_id="2604")

        mock_show.assert_called_once()
        args, kwargs = mock_show.call_args
        assert args[0] == "2604"
        assert args[1] == metadata
