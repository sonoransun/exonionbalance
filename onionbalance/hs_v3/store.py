# -*- coding: utf-8 -*-
"""
BerkeleyDB-backed persistent storage for onionbalance signal/event data.

Provides event logging, intro point change tracking, consensus event recording,
and instance/service state persistence for restart recovery.

Gracefully degrades to no-ops if the berkeleydb package is not installed.
"""

import json
import os
import struct
import time

from onionbalance.common import log

logger = log.get_logger()

try:
    import berkeleydb.db as bdb
    HAS_BDB = True
except ImportError:
    HAS_BDB = False


# Event type byte values
class EventType:
    FETCH_REQUESTED = 0x01
    FETCH_RECEIVED = 0x02
    FETCH_FAILED = 0x03
    PUBLISH_ATTEMPTED = 0x10
    PUBLISH_UPLOADED = 0x11
    PUBLISH_FAILED = 0x12
    CONSENSUS_ARRIVED = 0x20


# Default configuration
DEFAULT_STORE_DIR = os.path.expanduser('~/.onionbalance/ob_store')
STORE_DIR_ENV_VAR = 'ONIONBALANCE_STORE_DIR'
DB_CACHE_SIZE_BYTES = 16 * 1024 * 1024  # 16 MB
EVENT_RETENTION_SECONDS = 30 * 24 * 3600  # 30 days
STATE_SAVE_INTERVAL = 60  # seconds


def _make_event_key(timestamp_ns, event_type_byte):
    """Pack a primary key for event databases: 8B timestamp + 1B type + 4B nonce."""
    nonce = os.urandom(4)
    return struct.pack('>QB', timestamp_ns, event_type_byte) + nonce


def _extract_json_field(data, field):
    """Extract a field from JSON-encoded data, returning bytes or None."""
    try:
        record = json.loads(data.decode('utf-8'))
        value = record.get(field)
        if value is not None:
            return value.encode('utf-8')
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        pass
    return None


class OBStore:
    """
    BerkeleyDB-backed persistent store for onionbalance operational data.

    If BerkeleyDB is unavailable, all methods become no-ops.
    """

    def __init__(self, db_dir=None):
        self.enabled = False

        if not HAS_BDB:
            logger.info("berkeleydb package not available; persistent store disabled")
            return

        if db_dir is None:
            db_dir = os.environ.get(STORE_DIR_ENV_VAR, DEFAULT_STORE_DIR)

        self.db_dir = db_dir

        try:
            os.makedirs(self.db_dir, mode=0o700, exist_ok=True)
            self._open_environment()
            self._open_databases()
            self.enabled = True
            logger.info("Persistent store initialized at %s", self.db_dir)
        except Exception:
            logger.warning("Failed to initialize persistent store", exc_info=True)

    def _open_environment(self):
        """Open the BerkeleyDB environment."""
        self.env = bdb.DBEnv()
        self.env.set_cachesize(0, DB_CACHE_SIZE_BYTES, 1)
        self.env.set_flags(bdb.DB_LOG_AUTO_REMOVE, 1)
        self.env.open(
            self.db_dir,
            bdb.DB_CREATE | bdb.DB_INIT_MPOOL | bdb.DB_INIT_LOG
            | bdb.DB_INIT_TXN | bdb.DB_INIT_LOCK | bdb.DB_RECOVER
        )

    def _open_db(self, name, allow_dups=False):
        """Open a single database within the environment."""
        db = bdb.DB(self.env)
        if allow_dups:
            db.set_flags(bdb.DB_DUPSORT)
        db.open(name + '.db', None, bdb.DB_BTREE, bdb.DB_CREATE | bdb.DB_AUTO_COMMIT)
        return db

    def _open_secondary(self, primary_db, name, key_callback):
        """Open a secondary index associated with a primary database."""
        sec_db = bdb.DB(self.env)
        sec_db.set_flags(bdb.DB_DUPSORT)
        sec_db.open(name + '.db', None, bdb.DB_BTREE, bdb.DB_CREATE | bdb.DB_AUTO_COMMIT)
        primary_db.associate(sec_db, key_callback, bdb.DB_CREATE)
        return sec_db

    def _open_databases(self):
        """Open all primary databases and secondary indices."""
        # Primary databases
        self.descriptor_events_db = self._open_db('descriptor_events')
        self.intro_changes_db = self._open_db('intro_changes')
        self.consensus_events_db = self._open_db('consensus_events')
        self.instance_state_db = self._open_db('instance_state')
        self.service_state_db = self._open_db('service_state')

        # Secondary indices for descriptor_events
        self.desc_events_by_service = self._open_secondary(
            self.descriptor_events_db, 'desc_events_by_service',
            lambda pkey, pdata: _extract_json_field(pdata, 'service_address'))
        self.desc_events_by_instance = self._open_secondary(
            self.descriptor_events_db, 'desc_events_by_instance',
            lambda pkey, pdata: _extract_json_field(pdata, 'instance_address'))
        self.desc_events_by_type = self._open_secondary(
            self.descriptor_events_db, 'desc_events_by_type',
            lambda pkey, pdata: struct.pack('B', pkey[8]) if len(pkey) >= 9 else None)

        # Secondary indices for intro_changes
        self.intro_changes_by_instance = self._open_secondary(
            self.intro_changes_db, 'intro_changes_by_instance',
            lambda pkey, pdata: _extract_json_field(pdata, 'instance_address'))

        # Secondary index for instance_state
        self.instance_state_by_service = self._open_secondary(
            self.instance_state_db, 'instance_state_by_service',
            lambda pkey, pdata: _extract_json_field(pdata, 'service_address'))

        # Track all databases and secondary indices for cleanup
        self._secondary_dbs = [
            self.desc_events_by_service, self.desc_events_by_instance,
            self.desc_events_by_type, self.intro_changes_by_instance,
            self.instance_state_by_service,
        ]
        self._primary_dbs = [
            self.descriptor_events_db, self.intro_changes_db,
            self.consensus_events_db, self.instance_state_db,
            self.service_state_db,
        ]

    def close(self):
        """Close all databases and the environment."""
        if not self.enabled:
            return
        try:
            for db in self._secondary_dbs:
                db.close()
            for db in self._primary_dbs:
                db.close()
            self.env.close()
            logger.info("Persistent store closed")
        except Exception:
            logger.warning("Error closing persistent store", exc_info=True)
        self.enabled = False

    # -------------------------------------------------------------------------
    # Event recording
    # -------------------------------------------------------------------------

    def record_descriptor_event(self, event_type, service_address,
                                instance_address=None, hsdir_fingerprint=None,
                                descriptor_size=None, intro_point_count=None,
                                is_first_desc=None, error_reason=None):
        """Record a descriptor fetch or publish event."""
        if not self.enabled:
            return

        now = time.time()
        key = _make_event_key(int(now * 1e9), event_type)
        value = {
            'ts': now,
            'event_type': event_type,
            'service_address': service_address or '',
            'instance_address': instance_address or '',
            'hsdir_fingerprint': hsdir_fingerprint or '',
            'descriptor_size': descriptor_size,
            'intro_point_count': intro_point_count,
            'is_first_desc': is_first_desc,
            'error_reason': error_reason,
        }

        try:
            txn = self.env.txn_begin()
            try:
                self.descriptor_events_db.put(
                    key, json.dumps(value).encode('utf-8'), txn=txn)
                txn.commit()
            except Exception:
                txn.abort()
                raise
        except Exception:
            logger.debug("Failed to record descriptor event", exc_info=True)

    def record_intro_point_change(self, instance_address, service_address,
                                  old_key_count, new_key_count,
                                  old_onion_keys=None, new_onion_keys=None):
        """Record an introduction point set change for an instance."""
        if not self.enabled:
            return

        import hashlib
        now = time.time()
        addr_hash = hashlib.sha256(instance_address.encode('utf-8')).digest()[:8]
        key = struct.pack('>Q', int(now * 1e9)) + addr_hash
        value = {
            'ts': now,
            'instance_address': instance_address,
            'service_address': service_address,
            'previous_intro_count': old_key_count,
            'new_intro_count': new_key_count,
        }

        try:
            txn = self.env.txn_begin()
            try:
                self.intro_changes_db.put(
                    key, json.dumps(value).encode('utf-8'), txn=txn)
                txn.commit()
            except Exception:
                txn.abort()
                raise
        except Exception:
            logger.debug("Failed to record intro point change", exc_info=True)

    def record_consensus_event(self, consensus_obj):
        """Record a consensus arrival event."""
        if not self.enabled:
            return

        try:
            import stem.util
            valid_after = consensus_obj.consensus.valid_after
            valid_after_ts = int(stem.util.datetime_to_unix(valid_after))
            valid_until_ts = int(stem.util.datetime_to_unix(consensus_obj.consensus.valid_until))
        except (AttributeError, TypeError):
            return

        key = struct.pack('>Q', valid_after_ts)
        value = {
            'ts_received': time.time(),
            'valid_after': valid_after_ts,
            'valid_until': valid_until_ts,
            'is_live': consensus_obj.is_live(),
            'node_count': len(consensus_obj.nodes) if consensus_obj.nodes else 0,
        }

        try:
            txn = self.env.txn_begin()
            try:
                self.consensus_events_db.put(
                    key, json.dumps(value).encode('utf-8'), txn=txn)
                txn.commit()
            except Exception:
                txn.abort()
                raise
        except Exception:
            logger.debug("Failed to record consensus event", exc_info=True)

    # -------------------------------------------------------------------------
    # State persistence (for restart recovery)
    # -------------------------------------------------------------------------

    def save_instance_state(self, instance, service_address=''):
        """Save the current state of an instance for restart recovery."""
        if not self.enabled:
            return

        key = instance.onion_address.encode('utf-8')
        value = {
            'instance_address': instance.onion_address,
            'service_address': service_address,
            'intro_set_modified_ts': (
                instance.intro_set_modified_timestamp.timestamp()
                if instance.intro_set_modified_timestamp else None),
            'descriptor_received_ts': (
                instance.descriptor.received_ts.timestamp()
                if instance.descriptor and hasattr(instance.descriptor, 'received_ts')
                else None),
            'intro_point_count': (
                len(instance.descriptor.get_intro_points())
                if instance.descriptor else 0),
        }

        try:
            txn = self.env.txn_begin()
            try:
                self.instance_state_db.put(
                    key, json.dumps(value).encode('utf-8'), txn=txn)
                txn.commit()
            except Exception:
                txn.abort()
                raise
        except Exception:
            logger.debug("Failed to save instance state", exc_info=True)

    def load_instance_state(self, instance_address):
        """Load the last-known state of an instance. Returns dict or None."""
        if not self.enabled:
            return None

        try:
            key = instance_address.encode('utf-8')
            data = self.instance_state_db.get(key)
            if data:
                return json.loads(data.decode('utf-8'))
        except Exception:
            logger.debug("Failed to load instance state", exc_info=True)
        return None

    def save_service_state(self, service):
        """Save the current state of a service for restart recovery."""
        if not self.enabled:
            return

        key = service.onion_address.encode('utf-8')

        def _desc_meta(desc):
            if desc is None:
                return None
            return {
                'last_publish_attempt_ts': (
                    desc.last_publish_attempt_ts.timestamp()
                    if desc.last_publish_attempt_ts else None),
                'last_upload_ts': (
                    desc.last_upload_ts.timestamp()
                    if desc.last_upload_ts else None),
                'responsible_hsdirs': desc.responsible_hsdirs or [],
                'intro_point_count': len(desc.get_intro_points()),
            }

        value = {
            'service_address': service.onion_address,
            'first_descriptor': _desc_meta(service.first_descriptor),
            'second_descriptor': _desc_meta(service.second_descriptor),
        }

        try:
            txn = self.env.txn_begin()
            try:
                self.service_state_db.put(
                    key, json.dumps(value).encode('utf-8'), txn=txn)
                txn.commit()
            except Exception:
                txn.abort()
                raise
        except Exception:
            logger.debug("Failed to save service state", exc_info=True)

    def load_service_state(self, service_address):
        """Load the last-known state of a service. Returns dict or None."""
        if not self.enabled:
            return None

        try:
            key = service_address.encode('utf-8')
            data = self.service_state_db.get(key)
            if data:
                return json.loads(data.decode('utf-8'))
        except Exception:
            logger.debug("Failed to load service state", exc_info=True)
        return None

    # -------------------------------------------------------------------------
    # Query methods for signal analysis
    # -------------------------------------------------------------------------

    def count_events_since(self, event_type, since_ts):
        """Count descriptor events of a given type since a timestamp."""
        if not self.enabled:
            return 0

        count = 0
        since_ns = int(since_ts * 1e9)

        try:
            cursor = self.desc_events_by_type.cursor()
            try:
                type_key = struct.pack('B', event_type)
                record = cursor.set_range(type_key)
                while record:
                    sec_key, pkey, pdata = cursor.pget_current()
                    if sec_key != type_key:
                        break
                    # Extract timestamp from primary key
                    if len(pkey) >= 8:
                        ts_ns = struct.unpack('>Q', pkey[:8])[0]
                        if ts_ns >= since_ns:
                            count += 1
                    record = cursor.next_dup()
            finally:
                cursor.close()
        except Exception:
            logger.debug("Failed to count events", exc_info=True)

        return count

    def query_intro_change_rate(self, window_seconds=3600):
        """
        Return the number of intro point changes per minute over the window.
        """
        if not self.enabled:
            return 0.0

        now = time.time()
        since_ns = int((now - window_seconds) * 1e9)
        count = 0

        try:
            cursor = self.intro_changes_db.cursor()
            try:
                start_key = struct.pack('>Q', since_ns)
                record = cursor.set_range(start_key)
                while record:
                    count += 1
                    record = cursor.next()
            finally:
                cursor.close()
        except Exception:
            logger.debug("Failed to query intro change rate", exc_info=True)

        minutes = window_seconds / 60.0
        return count / minutes if minutes > 0 else 0.0

    def query_fetch_failure_rate(self, window_seconds=1800):
        """
        Return the ratio of fetch failures to total fetch events over the window.
        """
        if not self.enabled:
            return 0.0

        now = time.time()
        since_ts = now - window_seconds
        total = self.count_events_since(EventType.FETCH_RECEIVED, since_ts) + \
            self.count_events_since(EventType.FETCH_FAILED, since_ts)
        failures = self.count_events_since(EventType.FETCH_FAILED, since_ts)

        if total == 0:
            return 0.0
        return failures / total

    def query_publish_success_rate(self, window_seconds=1800):
        """
        Return the ratio of successful publishes to total publish events.
        """
        if not self.enabled:
            return 1.0

        now = time.time()
        since_ts = now - window_seconds
        successes = self.count_events_since(EventType.PUBLISH_UPLOADED, since_ts)
        failures = self.count_events_since(EventType.PUBLISH_FAILED, since_ts)
        total = successes + failures

        if total == 0:
            return 1.0
        return successes / total

    def get_total_signal_count(self):
        """Return the total number of descriptor events recorded."""
        if not self.enabled:
            return 0

        try:
            stat = self.descriptor_events_db.stat()
            return stat.get('ndata', 0)
        except Exception:
            return 0

    def get_first_event_timestamp(self):
        """Return the timestamp of the first recorded event, or None."""
        if not self.enabled:
            return None

        try:
            cursor = self.descriptor_events_db.cursor()
            try:
                record = cursor.first()
                if record:
                    key, _ = record
                    if len(key) >= 8:
                        ts_ns = struct.unpack('>Q', key[:8])[0]
                        return ts_ns / 1e9
            finally:
                cursor.close()
        except Exception:
            pass
        return None

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------

    def cleanup_old_data(self):
        """Delete event records older than the retention period."""
        if not self.enabled:
            return

        cutoff = time.time() - EVENT_RETENTION_SECONDS
        cutoff_ns = int(cutoff * 1e9)
        cutoff_key = struct.pack('>Q', cutoff_ns)

        deleted = 0
        for db in [self.descriptor_events_db, self.intro_changes_db, self.consensus_events_db]:
            try:
                txn = self.env.txn_begin()
                try:
                    cursor = db.cursor(txn)
                    try:
                        record = cursor.first()
                        while record:
                            key, _ = record
                            if key[:8] < cutoff_key[:8]:
                                cursor.delete()
                                deleted += 1
                                record = cursor.next()
                            else:
                                break
                    finally:
                        cursor.close()
                    txn.commit()
                except Exception:
                    txn.abort()
                    raise
            except Exception:
                logger.debug("Failed to clean up old data from a database", exc_info=True)

        if deleted > 0:
            logger.info("Cleaned up %d old event records from persistent store", deleted)

    def save_all_state(self, services):
        """Save state for all services and their instances."""
        if not self.enabled:
            return

        for service in services:
            self.save_service_state(service)
            for instance in service.instances:
                self.save_instance_state(instance, service.onion_address)


class NullStore:
    """No-op store used when BerkeleyDB is not available."""

    enabled = False

    def close(self):
        pass

    def record_descriptor_event(self, *args, **kwargs):
        pass

    def record_intro_point_change(self, *args, **kwargs):
        pass

    def record_consensus_event(self, *args, **kwargs):
        pass

    def save_instance_state(self, *args, **kwargs):
        pass

    def load_instance_state(self, *args, **kwargs):
        return None

    def save_service_state(self, *args, **kwargs):
        pass

    def load_service_state(self, *args, **kwargs):
        return None

    def count_events_since(self, *args, **kwargs):
        return 0

    def query_intro_change_rate(self, *args, **kwargs):
        return 0.0

    def query_fetch_failure_rate(self, *args, **kwargs):
        return 0.0

    def query_publish_success_rate(self, *args, **kwargs):
        return 1.0

    def get_total_signal_count(self):
        return 0

    def get_first_event_timestamp(self):
        return None

    def cleanup_old_data(self):
        pass

    def save_all_state(self, *args, **kwargs):
        pass
