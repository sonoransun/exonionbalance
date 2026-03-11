# -*- coding: utf-8 -*-
"""
Tests for the BerkeleyDB persistent store.
"""

import os
import shutil
import tempfile
import time

import pytest

from onionbalance.hs_v3.store import OBStore, NullStore, EventType, HAS_BDB


bdb_required = pytest.mark.skipif(not HAS_BDB,
                                  reason="berkeleydb package not installed")


@pytest.fixture
def store_dir():
    """Create a temporary directory for the test store."""
    d = tempfile.mkdtemp(prefix='ob_store_test_')
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def store(store_dir):
    """Create and return an OBStore instance."""
    s = OBStore(store_dir)
    yield s
    s.close()


@bdb_required
class TestStoreOpenClose:
    def test_open_creates_directory(self, store_dir):
        sub_dir = os.path.join(store_dir, 'subdir')
        s = OBStore(sub_dir)
        assert s.enabled
        assert os.path.isdir(sub_dir)
        s.close()

    def test_close_sets_disabled(self, store):
        assert store.enabled
        store.close()
        assert not store.enabled

    def test_double_close_is_safe(self, store):
        store.close()
        store.close()  # should not raise


class TestNullStore:
    """NullStore tests don't require berkeleydb."""
    def test_null_store_methods_are_noops(self):
        ns = NullStore()
        assert not ns.enabled
        ns.record_descriptor_event(EventType.FETCH_REQUESTED, 'addr')
        ns.record_intro_point_change('inst', 'svc', 0, 3)
        assert ns.load_instance_state('addr') is None
        assert ns.load_service_state('addr') is None
        assert ns.count_events_since(EventType.FETCH_RECEIVED, 0) == 0
        assert ns.query_intro_change_rate() == 0.0
        assert ns.query_fetch_failure_rate() == 0.0
        assert ns.query_publish_success_rate() == 1.0
        assert ns.get_total_signal_count() == 0
        assert ns.get_first_event_timestamp() is None
        ns.cleanup_old_data()
        ns.close()


@bdb_required
class TestDescriptorEvents:
    def test_record_and_count(self, store):
        now = time.time()
        store.record_descriptor_event(
            EventType.FETCH_RECEIVED,
            service_address='svc1.onion',
            instance_address='inst1.onion',
            descriptor_size=1234)
        store.record_descriptor_event(
            EventType.FETCH_RECEIVED,
            service_address='svc1.onion',
            instance_address='inst2.onion')

        count = store.count_events_since(EventType.FETCH_RECEIVED, now - 1)
        assert count == 2

    def test_count_filters_by_type(self, store):
        store.record_descriptor_event(
            EventType.FETCH_RECEIVED, service_address='svc1.onion')
        store.record_descriptor_event(
            EventType.FETCH_FAILED, service_address='svc1.onion')

        now = time.time()
        received = store.count_events_since(EventType.FETCH_RECEIVED, now - 10)
        failed = store.count_events_since(EventType.FETCH_FAILED, now - 10)
        assert received >= 1
        assert failed >= 1

    def test_total_signal_count(self, store):
        assert store.get_total_signal_count() == 0
        store.record_descriptor_event(
            EventType.FETCH_REQUESTED, service_address='svc1.onion')
        assert store.get_total_signal_count() >= 1

    def test_first_event_timestamp(self, store):
        assert store.get_first_event_timestamp() is None
        before = time.time()
        store.record_descriptor_event(
            EventType.FETCH_REQUESTED, service_address='svc1.onion')
        ts = store.get_first_event_timestamp()
        assert ts is not None
        assert ts >= before - 1


@bdb_required
class TestIntroPointChanges:
    def test_record_and_query_rate(self, store):
        store.record_intro_point_change(
            'inst1.onion', 'svc1.onion', 3, 4)
        store.record_intro_point_change(
            'inst1.onion', 'svc1.onion', 4, 3)

        rate = store.query_intro_change_rate(window_seconds=3600)
        assert rate > 0


@bdb_required
class TestFetchFailureRate:
    def test_no_events_returns_zero(self, store):
        assert store.query_fetch_failure_rate() == 0.0

    def test_all_success_returns_zero(self, store):
        for _ in range(5):
            store.record_descriptor_event(
                EventType.FETCH_RECEIVED, service_address='svc1.onion')
        assert store.query_fetch_failure_rate() == 0.0

    def test_mixed_events(self, store):
        for _ in range(3):
            store.record_descriptor_event(
                EventType.FETCH_RECEIVED, service_address='svc1.onion')
        store.record_descriptor_event(
            EventType.FETCH_FAILED, service_address='svc1.onion')

        rate = store.query_fetch_failure_rate(window_seconds=3600)
        assert 0.0 < rate < 1.0


@bdb_required
class TestPublishSuccessRate:
    def test_no_events_returns_one(self, store):
        assert store.query_publish_success_rate() == 1.0

    def test_all_success(self, store):
        for _ in range(3):
            store.record_descriptor_event(
                EventType.PUBLISH_UPLOADED, service_address='svc1.onion')
        assert store.query_publish_success_rate() == 1.0


@bdb_required
class TestInstanceState:
    def test_save_and_load(self, store):
        from types import SimpleNamespace
        import datetime

        instance = SimpleNamespace(
            onion_address='inst1.onion',
            intro_set_modified_timestamp=datetime.datetime(2024, 1, 1, 12, 0, 0),
            descriptor=None)

        store.save_instance_state(instance, 'svc1.onion')
        state = store.load_instance_state('inst1.onion')
        assert state is not None
        assert state['instance_address'] == 'inst1.onion'
        assert state['service_address'] == 'svc1.onion'

    def test_load_nonexistent_returns_none(self, store):
        assert store.load_instance_state('nonexistent.onion') is None


@bdb_required
class TestServiceState:
    def test_save_and_load(self, store):
        from types import SimpleNamespace

        service = SimpleNamespace(
            onion_address='svc1.onion',
            first_descriptor=None,
            second_descriptor=None)

        store.save_service_state(service)
        state = store.load_service_state('svc1.onion')
        assert state is not None
        assert state['service_address'] == 'svc1.onion'
        assert state['first_descriptor'] is None

    def test_load_nonexistent_returns_none(self, store):
        assert store.load_service_state('nonexistent.onion') is None


@bdb_required
class TestCleanup:
    def test_cleanup_removes_old_events(self, store):
        # Record an event
        store.record_descriptor_event(
            EventType.FETCH_RECEIVED, service_address='svc1.onion')

        # Should not remove recent events
        store.cleanup_old_data()
        assert store.get_total_signal_count() >= 1
