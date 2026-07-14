#!/usr/bin/env pytest

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

    def test_remove_mgmt_running(self, mock_schedd):
        """Valid snake-submitted mgmt job"""
        mock_schedd.act.return_value = {"TotalSuccess": 1, "TotalError": 0}

        Remove(logger=None, mgmt_id="2604")

        mock_schedd.act.assert_called_once()
        args, kwargs = mock_schedd.act.call_args
        constraint = args[1]
        assert "ClusterId == 2604" in constraint
        assert f"JobSubmitMethod == {JSM_HTC_SNAKE_SUBMIT}" in constraint
        assert kwargs.get("reason")

    def test_remove_without_mgmt_id(self, mock_schedd):
        """Test behavior when mgmt id is not provided"""
        with pytest.raises(SystemExit):
            Remove(logger=None, mgmt_id=None)
        mock_schedd.act.assert_not_called()

    def test_remove_mgmt_id_not_integer(self, mock_schedd):
        """Test behavior when mgmt id provided is not expected integer"""
        with pytest.raises(SystemExit):
            Remove(logger=None, mgmt_id="not-an-int")
        mock_schedd.act.assert_not_called()

    def test_remove_non_existent_job(self, mock_schedd):
        """Test running the command when no workflow is running"""
        mock_schedd.act.return_value = {"TotalSuccess": 0, "TotalError": 0}

        Remove(logger=None, mgmt_id="9999")

        mock_schedd.act.assert_called_once()

    def test_remove_schedd_exception(self, mock_schedd):
        """Test that exception is caught appropriately if calling schedd failed"""
        mock_schedd.act.side_effect = RuntimeError("Schedd connection failed")

        with pytest.raises(SystemExit):
            Remove(logger=None, mgmt_id="2604")

    def test_remove_sub_job(self, mock_schedd):
        """Test when the id provided is the sub job and not the management job.

        The JobSubmitMethod constraint excludes it, so schedd.act() should reports no
        successes even though the ClusterId itself exists.
        """
        mock_schedd.act.return_value = {"TotalSuccess": 0, "TotalError": 0}

        Remove(logger=None, mgmt_id="2605")

        mock_schedd.act.assert_called_once()
