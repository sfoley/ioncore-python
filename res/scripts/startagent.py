# Python Capability Container start script.
# Starts an instrument agent/driver combination for one instrance of a sensor (simulator).

import logging
from twisted.internet import defer

from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient

from ion.core import ioninit
from ion.core import bootstrap
from ion.services.dm.distribution.pubsub_service import DataPubsubClient
from ion.util.config import Config
import ion.util.procutils as pu

from ion.agents.instrumentagents.simulators.sim_SBE49 import Simulator
from ion.agents.instrumentagents.simulators.Simulator_constants import NO_PORT_NUMBER_FOUND

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('ion.core.bootstrap')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('ccagent_cfg', CONF)

service_procs = [
    {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
    {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'},
    {'name':'agent_registry','module':'ion.services.coi.agent_registry','class':'ResourceRegistryService'}
    ]

# Startup arguments
INSTRUMENT_ID = None
INSTRUMENT_IPADDR = None
INSTRUMENT_IPPORT = None
INSTRUMENT_IPCMDPORT = None
INSTRUMENT_NAME = None
SIMULATE = None

def eval_start_arguments():
    global INSTRUMENT_ID
    global INSTRUMENT_IPADDR
    global INSTRUMENT_IPPORT
    global INSTRUMENT_IPCMDPORT
    global INSTRUMENT_NAME
    global SIMULATE
    
    INSTRUMENT_ID = ioninit.cont_args.get('instId', '123')
    INSTRUMENT_IPADDR = ioninit.cont_args.get('instIP', 'localhost')
    INSTRUMENT_IPPORT = int(ioninit.cont_args.get('instPort', 9000))
    INSTRUMENT_IPCMDPORT = int(ioninit.cont_args.get('instCmdPort', 9001))
    INSTRUMENT_NAME = ioninit.cont_args.get('instName', None)
    SIMULATE = ioninit.cont_args.get('simulate', None)
    print "##### Use instrument ID: " + str(INSTRUMENT_ID)
    print "##### Use instrument IP: " + str(INSTRUMENT_IPADDR)
    print "##### Use instrument Port: " + str(INSTRUMENT_IPPORT)
    print "##### Use instrument CmdPort: " + str(INSTRUMENT_IPCMDPORT)
    print "##### Use instrument Name: " + str(INSTRUMENT_NAME)
    print "##### Use simulator: " + str(SIMULATE)

@defer.inlineCallbacks
def main():
    """
    Initializes container
    """
    logging.info("ION CONTAINER initializing... [Start an Instrument Agent]")

    processes = []
    processes.extend(agent_procs)
    processes.extend(service_procs)

    # Start the processes
    sup = yield bootstrap.bootstrap(None, processes)

    eval_start_arguments()
    
    if INSTRUMENT_NAME == None:
        print 'ERROR: no instrument name (instName=xyz) supplied'
        return

    if SIMULATE != None:
        try:
            exec ('from ion.agents.instrumentagents.simulators.sim_{0} import Simulator'.format(INSTRUMENT_NAME))
        except:
            print 'ERROR while trying to import Simulator from module sim_{0}'.format(INSTRUMENT_NAME)
            return
        simulator = Simulator(INSTRUMENT_ID, INSTRUMENT_IPPORT)
        Ports = simulator.start()
        if Ports[0] == NO_PORT_NUMBER_FOUND:
           print "Can't start simulator: no port available"
           sys.exit(1)
        print 'started sim_%s simulator at %s:%s,%s' %(INSTRUMENT_NAME, INSTRUMENT_IPADDR, Ports[0], Ports[1])
    else:
        Ports = [INSTRUMENT_IPPORT, INSTRUMENT_IPCMDPORT]
         
    ia_procs = [
        {'name':INSTRUMENT_NAME+'IA','module':'ion.agents.instrumentagents.'+INSTRUMENT_NAME+'_IA','class':INSTRUMENT_NAME+'InstrumentAgent',
         'spawnargs':{'instrument-id':INSTRUMENT_ID,'driver-args':{'ipaddr':INSTRUMENT_IPADDR,'ipport':Ports[0],'ipportCmd':Ports[1]}}}
    ]
    yield bootstrap.spawn_processes(ia_procs, sup=sup)

    ia_pid = sup.get_child_id(INSTRUMENT_NAME+'IA')
    iaclient = InstrumentAgentClient(proc=sup, target=ia_pid)
    yield iaclient.register_resource(INSTRUMENT_ID)

main()
