# Python Capability Container start script.
# Starts container with Java Services.

import logging
from twisted.internet import defer

from ion.agents.instrumentagents.simulators.sim_SBE49 import Simulator
from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
from ion.core import ioninit
from ion.core import bootstrap
from ion.util.config import Config
from ion.resources.sa_resource_descriptions import InstrumentResource, DataProductResource
from ion.services.sa.instrument_registry import InstrumentRegistryClient
from ion.services.sa.data_product_registry import DataProductRegistryClient

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('ion.core.bootstrap')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('ccagent_cfg', CONF)
demo_procs = [
    {'name':'agent_registry','module':'ion.services.coi.agent_registry','class':'ResourceRegistryService'},

    {'name':'instrument_registry','module':'ion.services.sa.instrument_registry','class':''},
    {'name':'data_product_registry','module':'ion.services.sa.data_product_registry','class':''},
    {'name':'instrument_management','module':'ion.services.sa.instrument_management','class':''},
    {'name':'service_registry','module':'ion.services.coi.service_registry','class':''},
]

INSTRUMENT_ID  = "123"


@defer.inlineCallbacks
def main():
    """
    Initializes container
    """
    logging.info("ION CONTAINER initializing... [LCA Java Integration Demo]")

    processes = []
    processes.extend(agent_procs)
    processes.extend(demo_procs)

    # Start the processes
    sup = yield bootstrap.bootstrap(None, processes)

    irc = InstrumentRegistryClient(proc=sup)

    ir1 = InstrumentResource.create_new_resource()
    ir1.name = "Demo_CTD_1"
    ir1.model = "SBE49"
    ir1.serial_num = "12345"
    ir1.fw_version = "1.334"
    ir1.manufactorer = "SeaBird"
    ir1 = yield irc.register_instrument_instance(ir1)
    ir1_ref = ir1.reference(head=True)

    ir2 = InstrumentResource.create_new_resource()
    ir2.name = "Demo_CTD_2"
    ir2.model = "SBE49_sim"
    ir2.serial_num = "23456"
    ir2.fw_version = "1.335"
    ir2.manufactorer = "SeaBird"
    ir2 = yield irc.register_instrument_instance(ir2)

    ir3 = InstrumentResource.create_new_resource()
    ir3.name = "Demo_ADCP_1"
    ir3.model = "WHSentinelADCP"
    ir3.serial_num = "34567"
    ir3.fw_version = "1.0.2"
    ir3.manufactorer = "Teledyne"
    ir3 = yield irc.register_instrument_instance(ir3)
    
    ir4 = InstrumentResource.create_new_resource()
    ir4.name = "Demo_ADCP_2"
    ir4.model = "WHSentinelADCP_sim"
    ir4.serial_num = "45678"
    ir4.fw_version = "1.0.4"
    ir4.manufactorer = "Teledyne"
    ir4 = yield irc.register_instrument_instance(ir4)

    dprc = DataProductRegistryClient(proc=sup)

    dp1 = DataProductResource.create_new_resource()
    dp1.instrument_ref = ir1_ref
    dp1.name = "Demo_Data_Product_1"
    dp1.dataformat = "binary"
    dp1 = yield dprc.register_data_product(dp1)


main()
