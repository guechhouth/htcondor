#!/usr/bin/env pytest

import pytest
from unittest.mock import patch, MagicMock
from htcondor_cli.snake import Status
import json

@pytest.fixture
def mock_schedd():
    """
    Patch htcondor2.Schedd and yield the mock instance so tests can configure
    schedd.query()'s return value/side effect and assert on calls.
    """
    with patch("htcondor2.Schedd") as mock_schedd_class:
        mock_schedd_instance = MagicMock()
        mock_schedd_class.return_value = mock_schedd_instance
        yield mock_schedd_instance

class TestSnakemakeStatus:
    """Test Non-UI related implementation for status class"""

    def test_path_as_identifier(self, mock_schedd, tmp_path):
        """Test that valid metadata file path provided can be read"""
        metadata = {
            "started_at": 1793204568.0,
            "total_nodes": 2,
            "executable_nodes": 1,
            "jobs": {},
        }
        metadata_path = tmp_path / "snakemake-metadata.json"
        metadata_path.write_text(json.dumps(metadata))

        with patch.object(Status, "_display_workflow_status") as mock_show:
            Status(logger=None, identifier=str(metadata_path))

        mock_schedd.query.assert_not_called() # a path never goes through the schedd
        mock_show.assert_called_once()
        args, kwargs = mock_show.call_args
        assert args[0] == str(metadata_path)
        assert args[1] == metadata
        assert args[2] is None # no management job ad for a path

    def test_identifier_is_directory(self, mock_schedd, tmp_path):
        """When the path provided is a directory, it can't be accepted"""
        mock_logger = MagicMock()

        with pytest.raises(SystemExit) as exc:
            Status(logger=mock_logger, identifier=str(tmp_path))

        assert exc.value.code == 1
        mock_schedd.query.assert_not_called()
        mock_logger.error.assert_called_once()
        assert "is a directory" in mock_logger.error.call_args[0][0]

    def test_identifier_is_required(self):
        """A missing identifier is rejected before querying HTCondor."""
        with pytest.raises(SystemExit) as exc:
            Status(logger=None, identifier=None)

        assert exc.value.code == 1
    def test_get_mgmt_info_schedd_exception(self, mock_schedd):
        """That exception is caught appropriately if calling schedd failed"""
        mock_schedd.query.side_effect = RuntimeError("Schedd connection failed")
        mock_logger = MagicMock()

        # skipping __init__ as we are testing only one helper method
        status = Status.__new__(Status)
        status.logger = mock_logger # set directly
        res = status._get_mgmt_job_info("2604")

        assert res is None
        mock_logger.warning.assert_called_once() # could not query schedd

    def test_status_no_job_in_queue(self, mock_schedd):
        """A ClusterId with no job in the queue exits and suggests passing the metadata path"""
        mock_schedd.query.return_value = []
        mock_logger = MagicMock()

        with pytest.raises(SystemExit):
            Status(logger=mock_logger, identifier="9999")

        mock_logger.error.assert_called_once()
        assert "pass the path to its metadata file" in mock_logger.error.call_args[0][0]

    def test_status_no_metadata_on_job_ad(self, mock_schedd):
        """Test behavior when the management job ad has no HTCondorSnakeMetadata attribute"""
        mock_schedd.query.return_value = [{"JobStatus": 2}]
        mock_logger = MagicMock()

        with pytest.raises(SystemExit):
            Status(logger=mock_logger, identifier="2604")

        mock_logger.error.assert_called_once() # job has no metadata path yet

    def test_status_metadata_file_not_found(self, mock_schedd, tmp_path):
        """Test metadata file not found even though the job ad has a pointer"""
        mock_logger = MagicMock()
        jobdir = tmp_path / "logs"
        jobdir.mkdir()
        metadata_path = jobdir / "snakemake-metadata-2604.json"
        mock_schedd.query.return_value = [{"HTCondorSnakeMetadata": str(metadata_path)}]

        with pytest.raises(SystemExit):
            Status(logger=mock_logger, identifier="2604")

        mock_logger.error.assert_called_once() # Metadata not found

    def test_status_malformed_metadata_json(self, mock_schedd, tmp_path):
        """Test metadata file containing invalid JSON"""
        mock_logger = MagicMock()
        jobdir = tmp_path / "logs"
        jobdir.mkdir()
        metadata_path = jobdir / "snakemake-metadata-2604.json"
        metadata_path.write_text("{not a valid json}")
        mock_schedd.query.return_value = [{"HTCondorSnakeMetadata": str(metadata_path)}]

        with pytest.raises(SystemExit):
            Status(logger=mock_logger, identifier="2604")

        mock_logger.error.assert_called_once() # could not read metadata

    def test_status_valid_full_flow(self, mock_schedd, tmp_path):
        """Test when normal working behavior"""
        jobdir = tmp_path / "logs"
        jobdir.mkdir()
        metadata = {
            "mgmt_job_id": 2604,
            "mgmt_job_name": "snakemake-mgmt-2604",
            "started_at": 123456.12,
            "total_nodes": 3,
            "executable_nodes": 2,
            "jobs": {}
        }
        metadata_path = jobdir / "snakemake-metadata-2604.json"
        metadata_path.write_text(json.dumps(metadata))
        job_ad = {"JobStatus": 2,"HTCondorSnakeMetadata": str(metadata_path)}
        mock_schedd.query.return_value = [job_ad]

        with patch.object(Status, "_display_workflow_status") as mock_show:
            Status(logger=None, identifier="2604")

        assert "HTCondorSnakeMetadata" in mock_schedd.query.call_args.kwargs["projection"]

        mock_show.assert_called_once()
        args, kwargs = mock_show.call_args
        assert args[0] == "2604"
        assert args[1] == metadata
        assert args[2] == job_ad

class TestdisplayWorkflowStatus:
    """Test the summary printed by _display_workflow_status for different metadata contents"""

    def test_display_counts_each_status(self, capsys):
        """Records in different states are each counted and listed in the summary"""
        metadata = {
            "started_at": 1793204568.0,
            "total_nodes": 7,
            "executable_nodes": 6,
            "jobs": {
                "101:1": {"cluster_id": 101, "jobid": 1, "status": "idle", "exit_code": None, "exit_signal": None},
                "102:2": {"cluster_id": 102, "jobid": 2, "status": "running", "exit_code": None, "exit_signal": None},
                "103:3": {"cluster_id": 103, "jobid": 3, "status": "running", "exit_code": None, "exit_signal": None},
                "104:4": {"cluster_id": 104, "jobid": 4, "status": "completed", "exit_code": 0, "exit_signal": None},
                "105:5": {"cluster_id": 105, "jobid": 5, "status": "held", "exit_code": None, "exit_signal": None},
                "106:6": {"cluster_id": 106, "jobid": 6, "status": "removed", "exit_code": None, "exit_signal": None},
            },
        }

        status = Status.__new__(Status)
        status._display_workflow_status("logs/snakemake-metadata.json", metadata, mgmt_job_ad=None)

        out = capsys.readouterr().out
        assert "Workflow has submitted 6 job(s), of which:" in out
        assert "        1 is submitted and waiting for resources." in out
        assert "        2 are running." in out
        assert "        1 is completed." in out
        assert "        1 is held." in out

    def test_display_completed_successfully(self, capsys):
        """All executable nodes completed and no job ad prints 'completed successfully' and 100%"""
        metadata = {
            "started_at": 1793204568.0,
            "total_nodes": 3,
            "executable_nodes": 2,
            "jobs": {
                "101:1": {"cluster_id": 101, "jobid": 1, "status": "completed", "exit_code": 0, "exit_signal": None},
                "102:2": {"cluster_id": 102, "jobid": 2, "status": "completed", "exit_code": 0, "exit_signal": None},
            },
        }

        # skipping __init__ as we are testing only the display method
        ## management is done running
        status = Status.__new__(Status)
        status._display_workflow_status("logs/snakemake-metadata.json", metadata, mgmt_job_ad=None)

        out = capsys.readouterr().out
        assert "Workflow has completed successfully." in out
        assert "Workflow is 100% complete." in out
        assert "running normally" not in out

    def test_display_not_complete_while_mgmt_job_in_queue(self, capsys):
        """All nodes completed but the job ad is still present should not print 'completed successfully'"""
        metadata = {
            "started_at": 1793204568.0,
            "total_nodes": 3,
            "executable_nodes": 2,
            "jobs": {
                "101:1": {"cluster_id": 101, "jobid": 1, "status": "completed", "exit_code": 0, "exit_signal": None},
                "102:2": {"cluster_id": 102, "jobid": 2, "status": "completed", "exit_code": 0, "exit_signal": None},
            },
        }
        # skipping __init__ as we are testing only the display method
        ## JobStatus: 2 = management is still running
        status = Status.__new__(Status)
        status._display_workflow_status("logs/snakemake-metadata.json", metadata, mgmt_job_ad={"JobStatus": 2})

        out = capsys.readouterr().out
        assert "Workflow has completed successfully." not in out
        assert "Workflow is running normally." in out

    def test_display_removed_jobs(self, capsys):
        """Removed jobs produce a failed workflow health status."""
        metadata = {
            "executable_nodes": 1,
            "jobs": {
                "101:1": {
                    "cluster_id": 101,
                    "status": "removed",
                    "exit_code": None,
                    "exit_signal": None,
                },
            },
        }

        status = Status.__new__(Status)
        status._display_workflow_status("metadata.json", metadata, mgmt_job_ad={"JobStatus": 2})

        out = capsys.readouterr().out
        assert "Some jobs have been removed." in out
        assert "Workflow is running normally." not in out

    def test_display_completed_management_job(self, capsys):
        """A completed management job reports completion when the workflow is not fully successful."""
        metadata = {
            "executable_nodes": 1,
            "jobs": {
                "101:1": {
                    "cluster_id": 101,
                    "status": "running",
                    "exit_code": None,
                    "exit_signal": None,
                },
            },
        }

        status = Status.__new__(Status)
        status._display_workflow_status("metadata.json", metadata, mgmt_job_ad={"JobStatus": 4})

        out = capsys.readouterr().out
        assert "Workflow has completed." in out
        assert "Workflow is running normally." not in out

    def test_display_held_takes_priority_over_removed(self, capsys):
        """With both held and removed records, the health line is 'Workflow has held jobs.'"""
        metadata = {
            "started_at": 1793204568.0,
            "total_nodes": 3,
            "executable_nodes": 2,
            "jobs": {
                "101:1": {"cluster_id": 101, "jobid": 1, "status": "removed", "exit_code": None, "exit_signal": None},
                "102:2": {"cluster_id": 102, "jobid": 2, "status": "held", "exit_code": None, "exit_signal": None},
            },
        }

        # skipping __init__ as we are testing only the display method
        status = Status.__new__(Status)
        status._display_workflow_status("logs/snakemake-metadata.json", metadata, mgmt_job_ad={"JobStatus": 2})

        out = capsys.readouterr().out
        assert "Workflow has held jobs." in out
        assert "Some jobs have been removed." not in out # held job is prioritized

    def test_display_grouped_jobs_wording(self, capsys):
        """Several node records sharing one cluster_id print 'submitted 1 HTCondor job(s), representing N node(s)'"""
        metadata = {
            "started_at": 1793204568.0,
            "total_nodes": 4,
            "executable_nodes": 3,
            "jobs": {
                "101:1": {"cluster_id": 101, "jobid": 1, "grouped": True, "status": "running", "exit_code": None, "exit_signal": None},
                "101:2": {"cluster_id": 101, "jobid": 2, "grouped": True,  "status": "running", "exit_code": None, "exit_signal": None},
                "101:3": {"cluster_id": 101, "jobid": 3, "grouped": True, "status": "running", "exit_code": None, "exit_signal": None},
            },
        }

        # skipping __init__ as we are testing only the display method
        status = Status.__new__(Status)
        status._display_workflow_status("logs/snakemake-metadata.json", metadata, mgmt_job_ad={"JobStatus": 2})

        out = capsys.readouterr().out
        # This is a group job so only one job is submitted, but there should be three nodes
        assert "Workflow has submitted 1 HTCondor job(s), representing 3 node(s)" in out

    def test_display_progress_uses_executable_nodes(self, capsys):
        """Progress is computed from executable_nodes, not total_nodes (which includes target and local rules)"""
        # 6 DAG nodes, of which 4 are executable and only 2 are submitted so far
        metadata = {
            "started_at": 1793204568.0,
            "total_nodes": 6,
            "executable_nodes": 4,
            "jobs": {
                "101:1": {"cluster_id": 101, "jobid": 1, "status": "completed", "exit_code": 0, "exit_signal": None},
                "102:2": {"cluster_id": 102, "jobid": 2, "status": "running", "exit_code": None, "exit_signal": None},
            },
        }

        status = Status.__new__(Status)
        status._display_workflow_status("logs/snakemake-metadata.json", metadata, mgmt_job_ad=None)

        out = capsys.readouterr().out
        assert "DAG contains 4 total executable node(s), of which:" in out
        assert "2 are waiting on other nodes to finish." in out
        assert "Workflow is 25.0% complete." in out # only 1/4 job is completed
