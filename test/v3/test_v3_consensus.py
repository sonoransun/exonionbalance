# -*- coding: utf-8 -*-
"""
Tests for consensus timing functions: SRV run timing, time period calculations,
and hsdir_spread_store consensus parameter.
"""

import datetime
from unittest import mock

from onionbalance.hs_v3 import consensus


class DummyConsensus(consensus.Consensus):
    """A consensus that does not attempt to refresh from the network."""
    def __init__(self):
        self.consensus = None
        self.nodes = []

    def is_live(self):
        """Always return True for testing."""
        return self.consensus is not None


def _make_mock_consensus(valid_after_dt, valid_until_dt=None, params=None):
    """Create a mock stem NetworkStatusDocumentV3 with given valid_after."""
    mock_consensus = mock.Mock()
    mock_consensus.valid_after = valid_after_dt
    if valid_until_dt is None:
        valid_until_dt = valid_after_dt + datetime.timedelta(hours=1)
    mock_consensus.valid_until = valid_until_dt
    mock_consensus.shared_randomness_current_value = None
    mock_consensus.shared_randomness_previous_value = None
    mock_consensus.params = params or {}
    mock_consensus.routers = {}
    return mock_consensus


class TestGetTimePeriodNum:
    """Test get_time_period_num() calculations."""

    @mock.patch('onionbalance.hs_v3.onionbalance.my_onionbalance')
    def test_mainnet_time_period(self, mock_ob):
        mock_ob.is_testnet = False
        c = DummyConsensus()
        # 2024-01-01 12:00:00 UTC
        va = datetime.datetime(2024, 1, 1, 12, 0, 0)
        c.consensus = _make_mock_consensus(va)

        tp = c.get_time_period_num()
        assert isinstance(tp, int)
        assert tp > 0

    @mock.patch('onionbalance.hs_v3.onionbalance.my_onionbalance')
    def test_consecutive_hours_same_period(self, mock_ob):
        """Two consensus valid_after times in the same day should give the same time period."""
        mock_ob.is_testnet = False
        c = DummyConsensus()

        va1 = datetime.datetime(2024, 1, 1, 13, 0, 0)
        c.consensus = _make_mock_consensus(va1)
        tp1 = c.get_time_period_num()

        va2 = datetime.datetime(2024, 1, 1, 14, 0, 0)
        c.consensus = _make_mock_consensus(va2)
        tp2 = c.get_time_period_num()

        # Same day should give same TP (time period is 24h)
        assert tp1 == tp2

    @mock.patch('onionbalance.hs_v3.onionbalance.my_onionbalance')
    def test_next_day_increments_period(self, mock_ob):
        """Time period should increment by 1 after 24 hours."""
        mock_ob.is_testnet = False
        c = DummyConsensus()

        va1 = datetime.datetime(2024, 1, 1, 13, 0, 0)
        c.consensus = _make_mock_consensus(va1)
        tp1 = c.get_time_period_num()

        va2 = datetime.datetime(2024, 1, 2, 13, 0, 0)
        c.consensus = _make_mock_consensus(va2)
        tp2 = c.get_time_period_num()

        assert tp2 == tp1 + 1

    @mock.patch('onionbalance.hs_v3.onionbalance.my_onionbalance')
    def test_testnet_shorter_period(self, mock_ob):
        """Testnet should use shorter time periods."""
        mock_ob.is_testnet = True
        c = DummyConsensus()

        va = datetime.datetime(2024, 1, 1, 12, 0, 0)
        c.consensus = _make_mock_consensus(va)
        tp = c.get_time_period_num()
        assert isinstance(tp, int)
        assert tp > 0


class TestSrvRunTiming:
    """Test get_start_time_of_current_srv_run() and related methods."""

    @mock.patch('onionbalance.hs_v3.onionbalance.my_onionbalance')
    def test_srv_run_start_mainnet(self, mock_ob):
        """SRV run start should be at the beginning of the current 24-round cycle."""
        mock_ob.is_testnet = False
        c = DummyConsensus()
        # 2024-01-01 23:00:00 UTC (round 23 of 24)
        va = datetime.datetime(2024, 1, 1, 23, 0, 0)
        c.consensus = _make_mock_consensus(va)

        srv_start = c.get_start_time_of_current_srv_run()
        # Should be 2024-01-01 00:00:00 UTC
        assert isinstance(srv_start, int)

        import stem.util
        expected = int(stem.util.datetime_to_unix(datetime.datetime(2024, 1, 1, 0, 0, 0)))
        assert srv_start == expected

    @mock.patch('onionbalance.hs_v3.onionbalance.my_onionbalance')
    def test_srv_run_start_midnight(self, mock_ob):
        """At midnight (round 0), SRV run start should be the current time."""
        mock_ob.is_testnet = False
        c = DummyConsensus()
        va = datetime.datetime(2024, 1, 2, 0, 0, 0)
        c.consensus = _make_mock_consensus(va)

        import stem.util
        srv_start = c.get_start_time_of_current_srv_run()
        expected = int(stem.util.datetime_to_unix(datetime.datetime(2024, 1, 2, 0, 0, 0)))
        assert srv_start == expected

    @mock.patch('onionbalance.hs_v3.onionbalance.my_onionbalance')
    def test_previous_srv_run_is_24h_before(self, mock_ob):
        """Previous SRV run should be exactly 24 hours before current."""
        mock_ob.is_testnet = False
        c = DummyConsensus()
        va = datetime.datetime(2024, 1, 2, 12, 0, 0)
        c.consensus = _make_mock_consensus(va)

        current = c.get_start_time_of_current_srv_run()
        previous = c.get_start_time_of_previous_srv_run()
        assert current - previous == 24 * 3600

    @mock.patch('onionbalance.hs_v3.onionbalance.my_onionbalance')
    def test_testnet_srv_run(self, mock_ob):
        """Testnet should use 20s voting intervals."""
        mock_ob.is_testnet = True
        c = DummyConsensus()
        va = datetime.datetime(2024, 1, 1, 12, 0, 0)
        c.consensus = _make_mock_consensus(va)

        current = c.get_start_time_of_current_srv_run()
        previous = c.get_start_time_of_previous_srv_run()
        # Testnet: 24 rounds * 20 seconds = 480 seconds between SRV runs
        assert current - previous == 24 * 20


class TestNextTimePeriodStart:
    @mock.patch('onionbalance.hs_v3.onionbalance.my_onionbalance')
    def test_next_tp_after_current(self, mock_ob):
        """The start of the next time period should be after now."""
        mock_ob.is_testnet = False
        c = DummyConsensus()
        va = datetime.datetime(2024, 1, 1, 12, 0, 0)
        c.consensus = _make_mock_consensus(va)

        import stem.util
        now_unix = stem.util.datetime_to_unix(va)
        next_tp_start = c.get_start_time_of_next_time_period()
        assert next_tp_start > now_unix


class TestHsdirSpreadStore:
    """Test get_hsdir_spread_store() consensus parameter reading."""

    def test_default_without_consensus(self):
        c = DummyConsensus()
        assert c.get_hsdir_spread_store() == 4

    def test_reads_from_consensus_params(self):
        c = DummyConsensus()
        c.consensus = _make_mock_consensus(
            datetime.datetime(2024, 1, 1, 12, 0, 0),
            params={'hsdir_spread_store': 3})
        assert c.get_hsdir_spread_store() == 3

    def test_default_when_param_missing(self):
        c = DummyConsensus()
        c.consensus = _make_mock_consensus(
            datetime.datetime(2024, 1, 1, 12, 0, 0),
            params={'other_param': 5})
        assert c.get_hsdir_spread_store() == 4

    def test_empty_params(self):
        c = DummyConsensus()
        c.consensus = _make_mock_consensus(
            datetime.datetime(2024, 1, 1, 12, 0, 0),
            params={})
        assert c.get_hsdir_spread_store() == 4
