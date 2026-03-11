import logging
import os
import sys

from onionbalance.common import scheduler
from onionbalance.common import log
from onionbalance.common import signalhandler

from onionbalance.hs_v3 import params
from onionbalance.hs_v3 import onionbalance

logger = log.get_logger()


def status_socket_location(config_data):
    location = os.environ.get('ONIONBALANCE_STATUS_SOCKET_LOCATION', '')
    if location == '':
        location = config_data.get('status-socket-location')
    return location


def main(args):
    """
    This is the entry point of v3 functionality.

    Initialize onionbalance, schedule future jobs and let the scheduler do its thing.
    """

    # Override the log level if specified on the command line.
    if args.verbosity:
        params.DEFAULT_LOG_LEVEL = args.verbosity.upper()
    logger.setLevel(logging.__dict__[params.DEFAULT_LOG_LEVEL.upper()])

    # Get the global onionbalance singleton
    my_onionbalance = onionbalance.my_onionbalance

    try:
        my_onionbalance.init_subsystems(args)
    except onionbalance.ConfigError as err:
        logger.error("%s", err)
        sys.exit(1)

    from onionbalance.hs_v3 import status
    status_socket = None
    if status_socket_location(my_onionbalance.config_data) is not None:
        status_socket = status.StatusSocket(status_socket_location(my_onionbalance.config_data), my_onionbalance)
    signalhandler.SignalHandler('v3', my_onionbalance.controller.controller, status_socket)

    # Schedule descriptor fetch and upload events
    init_scheduler(my_onionbalance)

    # Begin main loop to poll for HS descriptors
    scheduler.run_forever()

    return 0


def init_scheduler(my_onionbalance):
    from onionbalance.common.interval_policy import FetchIntervalPolicy, PublishIntervalPolicy

    scheduler.jobs = []

    store = my_onionbalance.store

    if store.enabled:
        # Use dynamic scheduling with persistent signal analysis
        if my_onionbalance.is_testnet:
            fetch_policy = FetchIntervalPolicy(
                base_interval=params.FETCH_DESCRIPTOR_FREQUENCY_TESTNET,
                min_interval=params.FETCH_INTERVAL_MIN_TESTNET,
                max_interval=params.FETCH_INTERVAL_MAX_TESTNET,
                smoothing_factor=params.SMOOTHING_FACTOR)
            publish_policy = PublishIntervalPolicy(
                base_interval=params.PUBLISH_DESCRIPTOR_CHECK_FREQUENCY_TESTNET,
                min_interval=params.PUBLISH_INTERVAL_MIN_TESTNET,
                max_interval=params.PUBLISH_INTERVAL_MAX_TESTNET,
                smoothing_factor=params.SMOOTHING_FACTOR)
        else:
            fetch_policy = FetchIntervalPolicy(
                base_interval=params.FETCH_DESCRIPTOR_FREQUENCY,
                min_interval=params.FETCH_INTERVAL_MIN,
                max_interval=params.FETCH_INTERVAL_MAX,
                smoothing_factor=params.SMOOTHING_FACTOR)
            publish_policy = PublishIntervalPolicy(
                base_interval=params.PUBLISH_DESCRIPTOR_CHECK_FREQUENCY,
                min_interval=params.PUBLISH_INTERVAL_MIN,
                max_interval=params.PUBLISH_INTERVAL_MAX,
                smoothing_factor=params.SMOOTHING_FACTOR)

        scheduler.add_dynamic_job(fetch_policy, store,
                                  my_onionbalance.fetch_instance_descriptors)
        scheduler.add_dynamic_job(publish_policy, store,
                                  my_onionbalance.publish_all_descriptors)

        # Periodic store cleanup (daily)
        scheduler.add_job(86400, store.cleanup_old_data)
        # Periodic state save (every 60 seconds)
        scheduler.add_job(60, store.save_all_state, my_onionbalance.services)
    else:
        # Fallback to fixed intervals when store is unavailable
        if my_onionbalance.is_testnet:
            scheduler.add_job(params.FETCH_DESCRIPTOR_FREQUENCY_TESTNET,
                              my_onionbalance.fetch_instance_descriptors)
            scheduler.add_job(params.PUBLISH_DESCRIPTOR_CHECK_FREQUENCY_TESTNET,
                              my_onionbalance.publish_all_descriptors)
        else:
            scheduler.add_job(params.FETCH_DESCRIPTOR_FREQUENCY,
                              my_onionbalance.fetch_instance_descriptors)
            scheduler.add_job(params.PUBLISH_DESCRIPTOR_CHECK_FREQUENCY,
                              my_onionbalance.publish_all_descriptors)

    # Dedicated job for keeping Tor active (prevents dormancy)
    if my_onionbalance.is_testnet:
        scheduler.add_job(params.FETCH_DESCRIPTOR_FREQUENCY_TESTNET,
                          my_onionbalance.controller.mark_tor_as_active)
    else:
        scheduler.add_job(params.FETCH_DESCRIPTOR_FREQUENCY,
                          my_onionbalance.controller.mark_tor_as_active)

    # Run initial fetch of HS instance descriptors
    scheduler.run_all(delay_seconds=params.INITIAL_CALLBACK_DELAY)
