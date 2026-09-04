#!/usr/bin/env pytest

import pytest
import signal
from unittest.mock import patch, MagicMock
from htcondor_cli.snake import Halt

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

class TestSnakemakeHalt:
    """Test halt logic and error handlings for htcondor snake halt"""

    def test_halt_mgmt_running(self, mock_schedd):
        """Halting a valid running Snakemake management job"""
        mgmt_id = "2604"
        target_pid = 54321
        mock_logger = MagicMock()
        mock_schedd.query.return_value = [
            {"JobStatus": 2, "SnakeMgmtPID": target_pid}
        ]

        with patch("os.kill") as mock_kill:
            Halt(logger=mock_logger, mgmt_id=mgmt_id)

            # Verify the query asked for the right cluster and fields
            mock_schedd.query.assert_called_once()
            _, kwargs = mock_schedd.query.call_args
            assert f"ClusterId == {mgmt_id}" in kwargs["constraint"]
            assert "JobStatus" in kwargs["projection"]
            assert "SnakeMgmtPID" in kwargs["projection"]

            # Verify SIGTERM was sent to the PID from the running job
            mock_kill.assert_called_once_with(target_pid, signal.SIGTERM)


    def test_halt_without_mgmt_id(self, capsys):
        """Test behavior when mgmt id is not provided"""
        with pytest.raises(SystemExit) as exc_info:
            Halt(logger=None, mgmt_id=None)

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "management job ID is required" in captured.out


    def test_halt_with_invalid_mgmt_id(self, capsys):
        """Test behavior when mgmt id is provided but not a valid input"""
        with pytest.raises(SystemExit) as exc_info:
            Halt(logger=None, mgmt_id="not-an-int")
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Management job ID must be an integer." in captured.out

    def test_halt_non_existant_job(self, mock_schedd):
        """Test when there is no workflow running"""
        mgmt_id = "2604"
        mock_logger = MagicMock()
        mock_schedd.query.return_value = []

        with pytest.raises(SystemExit) as exc_info:
            Halt(logger=mock_logger, mgmt_id=mgmt_id)

        assert exc_info.value.code == 1
        mock_logger.error.assert_called_once() # No management job found with JobID 2604."
        assert "No management job found with JobID" in mock_logger.error.call_args[0][0]

    def test_halt_schedd_exception(self, mock_schedd):
        """Schedd errors are caught, logged, and turned into a SystemExit(1)"""
        mock_logger = MagicMock()
        mock_schedd.query.side_effect = RuntimeError("Schedd connection failed")

        with pytest.raises(SystemExit) as exc_info:
            Halt(logger=mock_logger, mgmt_id="2604")

        assert exc_info.value.code == 1
        mock_logger.error.assert_called_once()
        assert "Could not halt the management job" in mock_logger.error.call_args[0][0]

    def test_halt_process_already_killed_or_no_pid(self, mock_schedd):
        """os.kill raises ProcessLookupError: PID from the ad is stale"""
        mgmt_id = "2604"
        target_pid = 54321
        mock_logger = MagicMock()
        mock_schedd.query.return_value = [
            {"JobStatus":2, "SnakeMgmtPID": target_pid}
        ]

        with patch("os.kill", side_effect=ProcessLookupError) as mock_kill:
            with pytest.raises(SystemExit) as exc_info:
                Halt(logger=mock_logger, mgmt_id=mgmt_id)

        mock_kill.assert_called_once_with(target_pid, signal.SIGTERM)
        assert exc_info.value.code == 1
        mock_logger.warning.assert_called_once()
        assert "no longer running" in mock_logger.warning.call_args[0][0]

    def test_halt_job_exists_but_not_running(self, mock_schedd):
        """Job is found but JobStatus != 2 (e.g. idle/held/completed), so there is nothing to halt"""
        mgmt_id = "2604"
        target_pid = 54321
        mock_logger = MagicMock()
        mock_schedd.query.return_value = [
            {"JobStatus":1, "SnakeMgmtPID": target_pid}
        ]

        with pytest.raises(SystemExit) as exc_info:
            Halt(logger=mock_logger, mgmt_id=mgmt_id)
        
        assert exc_info.value.code == 1
        mock_logger.warning.assert_called_once()
        assert "nothing to halt" in mock_logger.warning.call_args[0][0]

    def test_halt_running_job_missing_pid(self, mock_schedd):
        """JobStatus == 2 but SnakeMgmtPID is absent/None, so there is nothing to halt"""
        mgmt_id = "2604"
        mock_logger = MagicMock()
        mock_schedd.query.return_value = [
            {"JobStatus":2, "SnakeMgmtPID": None}
        ]

        with pytest.raises(SystemExit) as exc_info:
            Halt(logger=mock_logger, mgmt_id=mgmt_id)
        
        assert exc_info.value.code == 1
        mock_logger.warning.assert_called_once()
        assert "nothing to halt" in mock_logger.warning.call_args[0][0]


    def test_halt_multiple_mgmt_jobs_first_running_wins(self, mock_schedd):
        """when query returns multiple jobs for the cluster, only the first JobStatus == 2 match is used"""
        mgmt_id = "2604"
        first_running_pid = 11111
        second_running_pid = 22222
        mock_logger = MagicMock()
        mock_schedd.query.return_value = [
            {"JobStatus": 1, "SnakeMgmtPID": None},                # not running, skipped
            {"JobStatus": 2, "SnakeMgmtPID": first_running_pid},   # first running match
            {"JobStatus": 2, "SnakeMgmtPID": second_running_pid},  # never reached, loop breaks before this
        ]

        with patch("os.kill") as mock_kill:
            Halt(logger=mock_logger, mgmt_id=mgmt_id)

        # assert_called_once_with also proves it was never called with second_running_pid
        mock_kill.assert_called_once_with(first_running_pid, signal.SIGTERM)
