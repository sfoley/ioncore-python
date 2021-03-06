#!/usr/bin/env python

"""
@file ion/services/coi/state_repository.py
@author Michael Meisinger
@brief service for exchanging and persisting service instance state
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

class StateRepositoryService(ServiceProcess):
    """Repository for service state service interface. Service state is
    information shared between many processes.
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='state_repository', version='0.1.0', dependencies=[])

    def op_define_state(self, content, headers, msg):
        """Service operation: Create a new state object (session) or update
        an existing one by replacing
        """

    def op_update_state(self, content, headers, msg):
        """Service operation: Provide an incremental update to the service state.
        """

    def op_retrieve_state(self, content, headers, msg):
        """Service operation: TBD
        """

# Spawn of the process using the module name
factory = ProcessFactory(StateRepositoryService)
