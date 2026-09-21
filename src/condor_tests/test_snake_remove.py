#!/usr/bin/env pytest

import signal
import pytest
from unittest.mock import patch, MagicMock
from htcondor_cli.snake import Remove, JSM_HTC_SNAKE_SUBMIT


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


class TestSnakemakeRemove:
    """Test removing logic for htcondor snake remove"""

    @pytest.fixture(autouse=True)
    def mock_executor_plugin_installed(self):
        """
        Submit() requires snakemake_executor_plugin_htcondor.
        pretend it's installed so these tests exercise argument handling instead
        of the environment's package set.
        """
        with patch("htcondor_cli.snake.importlib.util.find_spec", return_value=MagicMock()):
            yield

    def test_remove_mgmt_running_fast(self, mock_schedd):
        """Fast (SIGINT) removal method for a valid workflow that is submitted"""
        mgmt_id = "2604"
        mock_logger = MagicMock()
        mock_schedd.act.return_value = {"TotalSuccess": 1, "TotalError": 0}

        Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=True, peaceful=False)

        mock_schedd.act.assert_called_once()
        args, kwargs = mock_schedd.act.call_args
        constraint = args[1]
        assert f"ClusterId == {mgmt_id}" in constraint
        assert f"JobSubmitMethod == {JSM_HTC_SNAKE_SUBMIT}" in constraint
        assert f"SnakeManagerJobId == {mgmt_id}" in constraint
        assert kwargs.get("reason")
        mock_logger.info.assert_called_once()

    def test_remove_mgmt_running_fast_error(self, mock_schedd):
        """Schedd.act() reports an error"""
        mgmt_id = "2604"
        mock_logger = MagicMock()
        mock_schedd.act.return_value = {"TotalSuccess": 0, "TotalError": 1}

        Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=True, peaceful=False)

        mock_schedd.act.assert_called_once()
        mock_logger.warning.assert_called_once_with(
            f"Failed to remove job {mgmt_id}: schedd reported 1 error(s)."
        )

    def test_remove_mgmt_running_peaceful(self, mock_schedd):
        """Peaceful (SIGTERM) removal method for a valid workflow that is submitted"""
        mgmt_id = "2604"
        target_pid = 12345
        mock_logger = MagicMock()
        mock_schedd.query.return_value = [
            {"JobStatus": 2, "SnakeMgmtPID": target_pid},
        ]

        with patch("htcondor_cli.snake.os.kill") as mock_kill:
            Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=False, peaceful=True)

        mock_schedd.query.assert_called_once()
        _, kwargs = mock_schedd.query.call_args
        assert kwargs["constraint"] == f"ClusterId == {mgmt_id} && JobSubmitMethod == {JSM_HTC_SNAKE_SUBMIT}"
        assert kwargs["projection"] == ["JobStatus", "SnakeMgmtPID"]

        mock_kill.assert_called_once_with(target_pid, signal.SIGTERM)
        mock_logger.info.assert_called_once()

    def test_peaceful_stale_pid(self, mock_schedd):
        """os.kill raises ProcessLookupError: PID from the ad is stale"""
        mgmt_id = "2604"
        target_pid = 54321
        mock_logger = MagicMock()
        mock_schedd.query.return_value = [
            {"JobStatus":2, "SnakeMgmtPID": target_pid}
        ]

        with patch("htcondor_cli.snake.os.kill", side_effect=ProcessLookupError) as mock_kill:
            with pytest.raises(SystemExit) as exc_info:
                Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=False, peaceful=True)

        mock_kill.assert_called_once_with(target_pid, signal.SIGTERM)
        assert exc_info.value.code == 1
        mock_logger.warning.assert_called_once()
        assert "no longer running" in mock_logger.warning.call_args[0][0]

    def test_running_job_missing_pid(self, mock_schedd):
        """JobStatus == 2 but SnakeMgmtPID is absent/None, so there is nothing to halt"""
        mgmt_id = "2604"
        mock_logger = MagicMock()
        mock_schedd.query.return_value = [
            {"JobStatus":2, "SnakeMgmtPID": None}
        ]

        with pytest.raises(SystemExit) as exc_info:
            Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=False, peaceful=True)
        
        assert exc_info.value.code == 1
        mock_logger.warning.assert_called_once()
        assert f"Management job {mgmt_id} is not currently running." in mock_logger.warning.call_args[0][0]

    def test_job_exists_but_not_running(self, mock_schedd):
        """Job is found but JobStatus != 2 (e.g. idle/held/completed), so there is nothing to halt"""
        mgmt_id = "2604"
        target_pid = 54321
        mock_logger = MagicMock()
        mock_schedd.query.return_value = [
            {"JobStatus":1, "SnakeMgmtPID": target_pid}
        ]

        with pytest.raises(SystemExit) as exc_info:
            Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=False, peaceful=True)
        
        assert exc_info.value.code == 1
        mock_logger.warning.assert_called_once()
        assert f"Management job {mgmt_id} is not currently running." in mock_logger.warning.call_args[0][0]


    def test_remove_without_mgmt_id(self, mock_schedd):
        """Test behavior when mgmt id is not provided"""
        with pytest.raises(SystemExit):
            Remove(logger=None, mgmt_id=None, fast=True, peaceful=False)
        mock_schedd.act.assert_not_called()

    def test_remove_mgmt_id_not_integer(self, mock_schedd):
        """Test behavior when mgmt id provided is not expected integer"""
        with pytest.raises(SystemExit):
            Remove(logger=None, mgmt_id="not-an-int", fast=False, peaceful=True)
        mock_schedd.act.assert_not_called()

    def test_remove_non_existent_job(self, mock_schedd):
        """Test running the command when no workflow is running"""
        mock_logger = MagicMock()
        mock_schedd.act.return_value = {"TotalSuccess": 0, "TotalError": 0}

        Remove(logger=mock_logger, mgmt_id="9999", fast=True, peaceful=False)

        mock_schedd.act.assert_called_once()
        mock_logger.info.assert_called_once()

    def test_remove_schedd_exception(self, mock_schedd):
        """Test that a schedd failure propagates instead of being swallowed"""
        mock_schedd.act.side_effect = RuntimeError("Schedd connection failed")

        with pytest.raises(SystemExit):
            Remove(logger=MagicMock(), mgmt_id="2604", fast=True, peaceful=False)

    def test_remove_peaceful_then_fast(self, mock_schedd):
        """Peaceful removal, then the user escalates to fast removal on the same job"""
        mgmt_id = "2604"
        mock_logger = MagicMock()

        mock_schedd.query.return_value = [{"JobStatus": 2, "SnakeMgmtPID": 12345}]
        with patch("htcondor_cli.snake.os.kill") as mock_kill:
            Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=False, peaceful=True)
        mock_kill.assert_called_once_with(12345, signal.SIGTERM)

        mock_schedd.act.return_value = {"TotalSuccess": 1, "TotalError": 0}
        Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=True, peaceful=False)

        mock_schedd.query.assert_called_once()
        mock_schedd.act.assert_called_once()
        assert mock_logger.info.call_count == 2

    def test_remove_fast_then_peaceful(self, mock_schedd):
        """Fast removal, then peaceful removal attempted on the now-gone job"""
        mgmt_id = "2604"
        mock_logger = MagicMock()

        mock_schedd.act.return_value = {"TotalSuccess": 1, "TotalError": 0}
        Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=True, peaceful=False)

        # The job was already removed from the queue, so the follow-up query
        # finds nothing.
        mock_schedd.query.return_value = []
        with patch("htcondor_cli.snake.os.kill") as mock_kill:
            with pytest.raises(SystemExit):
                Remove(logger=mock_logger, mgmt_id=mgmt_id, fast=False, peaceful=True)

        mock_schedd.act.assert_called_once()
        mock_schedd.query.assert_called_once()
        mock_kill.assert_not_called()
        mock_logger.info.assert_called_once()
        mock_logger.error.assert_called_once_with(
            f"No management job found with JobID {mgmt_id}."
        )

    def test_no_removal_method(self, mock_schedd):
        """Neither of the removal methods are specified. fast option should be applied."""
        mgmt_id = "2604"
        mock_logger = MagicMock()

        mock_schedd.act.return_value = {"TotalSuccess": 1, "TotalError": 0}
        Remove(logger=mock_logger, mgmt_id=mgmt_id)

        mock_schedd.act.assert_called_once()
        args, kwargs = mock_schedd.act.call_args
        constraint = args[1]
        assert f"ClusterId == {mgmt_id}" in constraint
        assert f"JobSubmitMethod == {JSM_HTC_SNAKE_SUBMIT}" in constraint
        assert f"SnakeManagerJobId == {mgmt_id}" in constraint
        assert kwargs.get("reason")
        mock_logger.info.assert_called_once()
