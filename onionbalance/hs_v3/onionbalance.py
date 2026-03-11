import os
import sys

import stem
from stem.descriptor.hidden_service import HiddenServiceDescriptorV3

import onionbalance.common.instance

from onionbalance.common import log

from onionbalance.common import util
from onionbalance.hs_v3 import manager

from onionbalance.hs_v3 import stem_controller
from onionbalance.hs_v3 import service as ob_service
from onionbalance.hs_v3 import consensus as ob_consensus
from onionbalance.hs_v3.store import OBStore, NullStore, EventType

logger = log.get_logger()


class Onionbalance(object):
    """
    Onionbalance singleton that represents this onionbalance runtime.

    Contains various objects that are useful to other onionbalance modules so
    this is imported from all over the codebase.
    """

    def __init__(self):
        # This is kept minimal so that it's quick (it's executed at program
        # launch because of the onionbalance singleton). The actual init work
        # happens in init_subsystems()

        # True if this onionbalance operates in a testnet (e.g. chutney)
        self.is_testnet = False

        # Persistent store (initialized in init_subsystems)
        self.store = NullStore()

    def init_subsystems(self, args):
        """
        Initialize subsystems (this is resource intensive)
        """
        self.args = args
        self.config_path = os.path.abspath(self.args.config)
        self.config_data = self.load_config_file()
        self.is_testnet = args.is_testnet

        if self.is_testnet:
            logger.warning("Onionbalance configured on a testnet!")

        # Create stem controller and connect to the Tor network
        self.controller = stem_controller.StemController(address=args.ip, port=args.port, socket=args.socket)
        self.consensus = ob_consensus.Consensus()

        # Initialize persistent store
        store_dir = os.environ.get('ONIONBALANCE_STORE_DIR',
                                   self.config_data.get('store-dir'))
        try:
            self.store = OBStore(store_dir)
        except Exception:
            logger.warning("Failed to initialize persistent store, using null store")
            self.store = NullStore()

        # Initialize our service
        self.services = self.initialize_services_from_config_data()

        # Restore state from persistent store if available
        self._restore_state_from_store()

        # Catch interesting events (like receiving descriptors etc.)
        self.controller.add_event_listeners()

        logger.warning("Onionbalance initialized (stem version: %s) (tor version: %s)!",
                       stem.__version__, self.controller.controller.get_version())
        logger.warning("=" * 80)

    def initialize_services_from_config_data(self):
        services = []
        try:
            for service in self.config_data['services']:
                services.append(ob_service.OnionbalanceService(service, self.config_path))
        except ob_service.BadServiceInit:
            sys.exit(1)

        return services

    def load_config_file(self):
        config_data = util.read_config_data_from_file(self.config_path)
        logger.debug("Onionbalance config data: %s", config_data)

        # Do some basic validation
        if "services" not in config_data:
            raise ConfigError("Config file is bad. 'services' is missing. Did you make it with onionbalance-config?")

        # More validation
        for service in config_data["services"]:
            if "key" not in service:
                raise ConfigError("Config file is bad. 'key' is missing. Did you make it with onionbalance-config?")

            if "instances" not in service:
                raise ConfigError("Config file is bad. 'instances' is missing. Did you make it with "
                                  "onionbalance-config?")

            if not service["instances"]:
                raise ConfigError("Config file is bad. No backend instances are set. Onionbalance needs at least 1.")

            for instance in service["instances"]:
                if "address" not in instance:
                    raise ConfigError("Config file is wrong. 'address' missing from instance.")

                if not instance["address"]:
                    raise ConfigError("Config file is bad. Address field is not set.")

                # Validate that the onion address is legit
                try:
                    _ = HiddenServiceDescriptorV3.identity_key_from_address(instance["address"])
                except ValueError:
                    raise ConfigError("Cannot load instance with address: '{}'".format(instance["address"]))

        return config_data

    def _restore_state_from_store(self):
        """Restore instance and service state from persistent store on startup."""
        for service in self.services:
            for instance in service.instances:
                state = self.store.load_instance_state(instance.onion_address)  # pylint: disable=assignment-from-none
                if state:
                    logger.info("Restored state for instance %s from persistent store",
                                instance.onion_address)

    def fetch_instance_descriptors(self):
        logger.info("[*] fetch_instance_descriptors() called [*]")

        if not self.consensus.is_live():
            logger.warning("No live consensus. Waiting before fetching descriptors...")
            return

        all_instances = self._get_all_instances()

        # Record fetch request events
        for instance in all_instances:
            try:
                service_addr = self._get_service_for_instance(instance)
                self.store.record_descriptor_event(
                    EventType.FETCH_REQUESTED,
                    service_address=service_addr,
                    instance_address=instance.onion_address)
            except Exception:
                pass

        onionbalance.common.instance.helper_fetch_all_instance_descriptors(self.controller.controller,
                                                                           all_instances)

    def _get_service_for_instance(self, instance):
        """Return the service address that owns this instance, or empty string."""
        for service in self.services:
            for inst in service.instances:
                if inst.onion_address == instance.onion_address:
                    return service.onion_address
        return ''

    def handle_new_desc_content_event(self, desc_content_event):
        """
        Parse HS_DESC_CONTENT response events for descriptor content

        Update the HS instance object with the data from the new descriptor.
        """
        onion_address = desc_content_event.address
        logger.debug("Received descriptor for %s.onion from %s",
                     onion_address, desc_content_event.directory)

        #  Check that the HSDir returned a descriptor that is not empty
        descriptor_text = str(desc_content_event.descriptor).encode('utf-8')

        # HSDirs provide a HS_DESC_CONTENT response with either one or two
        # CRLF lines when they do not have a matching descriptor. Using
        # len() < 5 should ensure all empty HS_DESC_CONTENT events are matched.
        if len(descriptor_text) < 5:
            logger.debug("Empty descriptor received for %s.onion", onion_address)
            return None

        # OK this descriptor seems plausible: Find the instances that this
        # descriptor belongs to:
        for instance in self._get_all_instances():
            if instance.onion_address == onion_address:
                instance.register_descriptor(descriptor_text, onion_address)
                # Record successful fetch and save instance state
                try:
                    service_addr = self._get_service_for_instance(instance)
                    self.store.record_descriptor_event(
                        EventType.FETCH_RECEIVED,
                        service_address=service_addr,
                        instance_address=instance.onion_address,
                        descriptor_size=len(descriptor_text))
                    self.store.save_instance_state(instance, service_addr)
                except Exception:
                    pass

    def publish_all_descriptors(self):
        """
        For each service attempt to publish all descriptors
        """
        logger.info("[*] publish_all_descriptors() called [*]")

        if not self.consensus.is_live():
            logger.info("No live consensus. Waiting before publishing descriptors...")
            return

        for service in self.services:
            service.publish_descriptors()

    def _get_all_instances(self):
        """
        Get all instances for all services
        """
        instances = []

        for service in self.services:
            instances.extend(service.instances)

        return instances

    def handle_new_status_event(self, status_event):
        """
        Parse Tor status events such as "STATUS_GENERAL"
        """
        # pylint: disable=no-member
        if status_event.action == "CONSENSUS_ARRIVED":
            logger.info("Received new consensus!")
            self.consensus.refresh()
            try:
                self.store.record_consensus_event(self.consensus)
            except Exception:
                pass
            # Call all callbacks in case we just got a live consensus
            my_onionbalance.publish_all_descriptors()
            my_onionbalance.fetch_instance_descriptors()

    def _address_is_instance(self, onion_address):
        """
        Return True if 'onion_address' is one of our instances.
        """
        for service in self.services:
            for instance in service.instances:
                if instance.has_onion_address(onion_address):
                    return True
        return False

    def _address_is_frontend(self, onion_address):
        for service in self.services:
            if service.has_onion_address(onion_address):
                return True
        return False

    def handle_new_desc_event(self, desc_event):
        """
        Parse HS_DESC response events
        """
        action = desc_event.action

        if action == "RECEIVED":
            pass  # We already log in HS_DESC_CONTENT so no need to do it here too
        elif action == "UPLOADED":
            logger.debug("Successfully uploaded descriptor for %s to %s", desc_event.address, desc_event.directory)
            try:
                self.store.record_descriptor_event(
                    EventType.PUBLISH_UPLOADED,
                    service_address=desc_event.address,
                    hsdir_fingerprint=desc_event.directory)
            except Exception:
                pass
        elif action == "FAILED":
            if self._address_is_instance(desc_event.address):
                logger.info("Descriptor fetch failed for instance %s from %s (%s)",
                            desc_event.address, desc_event.directory, desc_event.reason)
                try:
                    self.store.record_descriptor_event(
                        EventType.FETCH_FAILED,
                        service_address='',
                        instance_address=desc_event.address,
                        error_reason=desc_event.reason)
                except Exception:
                    pass
            elif self._address_is_frontend(desc_event.address):
                logger.warning("Descriptor upload failed for frontend %s to %s (%s)",
                               desc_event.address, desc_event.directory, desc_event.reason)
                try:
                    self.store.record_descriptor_event(
                        EventType.PUBLISH_FAILED,
                        service_address=desc_event.address,
                        error_reason=desc_event.reason)
                except Exception:
                    pass
            else:
                logger.warning("Descriptor action failed for unknown service %s to %s (%s)",
                               desc_event.address, desc_event.directory, desc_event.reason)
        elif action == "REQUESTED":
            logger.debug("Requested descriptor for %s from %s...", desc_event.address, desc_event.directory)
        else:
            pass

    def reload_config(self):
        """
        Reload configuration and reset job scheduler
        """

        try:
            self.init_subsystems(self.args)
            manager.init_scheduler(self)
        except ConfigError as err:
            logger.error("%s", err)
            sys.exit(1)


class ConfigError(Exception):
    pass


my_onionbalance = Onionbalance()
