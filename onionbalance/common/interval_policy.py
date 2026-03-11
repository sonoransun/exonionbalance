# -*- coding: utf-8 -*-
"""
Dynamic interval policies for adaptive scheduling.

Policies compute recommended scheduling intervals based on persistent signal
data from the OBStore, applying EWMA smoothing and min/max clamping to
prevent oscillation.
"""

import time

from onionbalance.common import log
from onionbalance.hs_v3 import params

logger = log.get_logger()


class IntervalPolicy:
    """
    Base class for interval computation policies.

    Subclasses override compute_interval() to define the raw target interval
    based on metrics from the store. get_interval() applies EWMA smoothing
    and clamping.
    """

    def __init__(self, base_interval, min_interval, max_interval,
                 smoothing_factor=0.3):
        self.base_interval = base_interval
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.smoothing_factor = smoothing_factor
        self._current_interval = base_interval
        self._metrics_cache = None
        self._metrics_cache_ts = 0.0
        self._metrics_cache_ttl = 30.0  # seconds

    def compute_interval(self, store):
        """
        Compute the raw recommended interval from store metrics.
        Subclasses must override this.
        """
        return self.base_interval

    def _is_startup_phase(self, store):
        """Return True if we are in the startup phase."""
        from onionbalance.hs_v3.onionbalance import my_onionbalance
        if my_onionbalance.is_testnet:
            startup_duration = params.STARTUP_DURATION_TESTNET
            min_signals = params.MIN_SIGNALS_FOR_STABLE_TESTNET
        else:
            startup_duration = params.STARTUP_DURATION
            min_signals = params.MIN_SIGNALS_FOR_STABLE

        if store.get_total_signal_count() < min_signals:
            return True

        first_ts = store.get_first_event_timestamp()
        if first_ts and (time.time() - first_ts) < startup_duration:
            return True

        return False

    def _near_consensus_rotation(self):
        """Return True if we are near a consensus TP rotation boundary."""
        try:
            from onionbalance.hs_v3.onionbalance import my_onionbalance
            if not my_onionbalance.consensus.is_live():
                return False

            if my_onionbalance.is_testnet:
                window = params.CONSENSUS_PROXIMITY_WINDOW_TESTNET
            else:
                window = params.CONSENSUS_PROXIMITY_WINDOW

            next_tp_start = my_onionbalance.consensus.get_start_time_of_next_time_period()
            time_to_rotation = next_tp_start - time.time()
            return 0 < time_to_rotation < window
        except (AssertionError, AttributeError, TypeError):
            return False

    def get_interval(self, store):
        """
        Get the smoothed, clamped interval.

        1. Compute raw target from store metrics
        2. Apply EWMA smoothing (anti-oscillation)
        3. Clamp to [min_interval, max_interval]
        """
        raw_target = self.compute_interval(store)

        # EWMA smoothing: new = alpha * target + (1 - alpha) * current
        smoothed = (self.smoothing_factor * raw_target
                    + (1 - self.smoothing_factor) * self._current_interval)

        # Clamp
        clamped = max(self.min_interval, min(self.max_interval, smoothed))
        self._current_interval = clamped

        logger.debug("IntervalPolicy %s: raw=%.1f smoothed=%.1f clamped=%.1f",
                     self.__class__.__name__, raw_target, smoothed, clamped)

        return clamped


class FetchIntervalPolicy(IntervalPolicy):
    """
    Adjusts fetch frequency based on:
    - Startup phase: use min_interval (aggressive)
    - Intro point instability: decrease interval (fetch more)
    - Stable instances: increase interval (save resources)
    - Fetch failures: backoff
    - Near consensus rotation: increase activity
    """

    def compute_interval(self, store):
        if self._is_startup_phase(store):
            return self.min_interval

        target = self.base_interval

        # Intro point instability
        change_rate = store.query_intro_change_rate()
        if change_rate > 0.5:
            instability_factor = max(0.5, 1.0 - (change_rate - 0.5) / 3.0)
            target *= instability_factor
        elif change_rate < 0.1:
            target *= 1.5

        # Fetch failures: backoff
        failure_rate = store.query_fetch_failure_rate()
        if failure_rate > 0.5:
            backoff_factor = 1.0 + failure_rate
            target *= backoff_factor

        # Near consensus rotation: be more aggressive
        if self._near_consensus_rotation():
            target *= 0.5

        return target


class PublishIntervalPolicy(IntervalPolicy):
    """
    Adjusts publish check frequency based on:
    - Startup phase: use min_interval
    - Publish failures: retry faster initially, then backoff
    - Intro point changes: check more often
    - High stability: relax
    - Near consensus rotation: increase activity
    """

    def compute_interval(self, store):
        if self._is_startup_phase(store):
            return self.min_interval

        target = self.base_interval

        # Publish failures
        publish_success_rate = store.query_publish_success_rate()
        failure_rate = 1.0 - publish_success_rate
        if failure_rate > 0.3:
            # Count recent consecutive failures
            recent_failures = store.count_events_since(
                0x12,  # PUBLISH_FAILED
                time.time() - 1800)
            if recent_failures > 3:
                target = min(self.base_interval * (2 ** (recent_failures - 3)),
                             self.max_interval)
            else:
                target *= 0.5

        # Intro point changes
        change_rate = store.query_intro_change_rate()
        if change_rate > 0.3:
            target *= max(0.5, 1.0 - change_rate / 4.0)

        # High stability: relax
        if failure_rate < 0.1 and change_rate < 0.1:
            target *= 1.3

        # Near consensus rotation
        if self._near_consensus_rotation():
            target *= 0.6

        return target
