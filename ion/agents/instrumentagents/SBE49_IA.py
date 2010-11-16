#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE49_IA.py
@author Steve Foley
@brief CI interface for SeaBird SBE-49 CTD
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import SBE49_constants as const
from ion.agents.instrumentagents import instrument_agent as IA
from ion.agents.instrumentagents.instrument_agent import InstrumentAgent

from ion.core.process.process import ProcessFactory, ProcessDesc
from ion.resources.dm_resource_descriptions import PubSubTopicResource, PublisherResource

# Gotta have this AFTER the "static" variables above
from ion.agents.instrumentagents.SBE49_driver import SBE49InstrumentDriverClient

class SBE49InstrumentAgent(InstrumentAgent):
    """
    Sea-Bird 49 specific instrument driver
    Inherits basic get, set, getStatus, getCapabilities, etc. from parent
    """    
    @defer.inlineCallbacks
    def plc_init(self):
        """
        Initialize instrument driver when this process is started.
        """
        InstrumentAgent.plc_init(self)
        self.instrument_id = self.spawn_args.get('instrument-id', '123')
        
        self.driver_args = self.spawn_args.get('driver-args', {})
        
        log.info("INIT agent for instrument ID: %s" % (self.instrument_id))

        self.driver_args['instrument-id'] = self.instrument_id
        pd = ProcessDesc(**{'name':'SBE49Driver',
                          'module':'ion.agents.instrumentagents.SBE49_driver',
                          'class':'SBE49InstrumentDriver',
                          'spawnargs':self.driver_args})
        log.debug("*** spawning driver from IA")
        driver_id = yield self.spawn_child(pd)
        log.debug("*** spawned driver")
        self.driver_client = SBE49InstrumentDriverClient(proc=self,
                                                         target=driver_id)
        log.debug("*** Starting base topics")
        # set base topics        
        self.output_topics = {"Device":"OutputDevice" + self.instrument_id}
        self.event_topics = {"Device":"EventDevice" + self.instrument_id}
        self.state_topics = {"Device":"StateDevice" + self.instrument_id}
        
        # register topics
        log.debug("*** creating topics")
        self.output_topics["Device"] = PubSubTopicResource.create("OutputDevice" + self.instrument_id, "")
        self.event_topics["Device"] = PubSubTopicResource.create("EventDevice" + self.instrument_id, "")
        self.state_topics["Device"] = PubSubTopicResource.create("StateDevice" + self.instrument_id, "")

        log.debug("*** creating publisher")
        """
        for name in self.output_topics:
            self.output_topics[name] = yield self.dpsc.define_topic(name)
            log.info('Defined Topic: '+str(self.output_topics[name]))
            ### Do we even need the publisher stuff below?
            #Create and register self.sup as a publisher
            publisher = PublisherResource.create('Test Publisher', self.sup,
                                                 self.output_topics[name],
                                                 'DataObject')
            publisher = yield self.dpsc.define_publisher(publisher)
            log.info('Defined Publisher: ' + str(publisher))
        ### Repeat above for event and state topics when the details are worked out
        """
        yield

    #@defer.inlineCallbacks
    #def plc_terminate(self):
    #    yield self.pd.shutdown()


    @staticmethod
    def __translator(input):
        """
        A function (to be returned upon request) that will translate the
        very raw data from the instrument into the common archive format
        """
        return input

    @defer.inlineCallbacks
    def op_get_translator(self, content, headers, msg):
        """
        Return the translator function that will convert the very raw format
        of the instrument into a common OOI repository-ready format
        """
        yield self.reply_err(msg, "Not Implemented!")
#        yield self.reply_ok(msg, self.__translator)

    @defer.inlineCallbacks
    def op_get_capabilities(self, content, headers, msg):
        """
        Obtain a list of capabilities that this instrument has. This is
        simply a command and parameter list at this point
        """
        yield self.reply(msg, 'get_capabilities',
                         {IA.instrument_commands: const.instrument_commands,
                          IA.instrument_parameters: const.instrument_parameters,
                          IA.ci_commands: const.ci_commands,
                          IA.ci_parameters: const.ci_parameters}, {})

# Spawn of the process using the module name
factory = ProcessFactory(SBE49InstrumentAgent)
