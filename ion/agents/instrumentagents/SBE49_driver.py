#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE49_driver.py
@author Steve Foley
@author Dave Everett
@brief Driver code for SeaBird SBE-49 CTD
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer, reactor

"""
from ion.data.dataobject import LCStates as LCS
from ion.core.process.service_process import ServiceProcess
"""

# DHE: testing the miros HSM
import miros
from instrument_hsm import InstrumentHsm

from ion.agents.instrumentagents.instrument_connection import InstrumentConnection
from twisted.internet.protocol import ClientCreator

from collections import deque

from ion.core.process.process import Process
from ion.data.dataobject import ResourceReference
from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource, DAPMessageObject
from ion.services.dm.distribution.pubsub_service import DataPubsubClient

from ion.agents.instrumentagents.instrument_agent import InstrumentDriver, InstrumentAgentClient
from ion.agents.instrumentagents.instrument_agent import InstrumentDriverClient
from ion.agents.instrumentagents.SBE49_constants import instrument_commands
from ion.agents.instrumentagents.SBE49_constants import instrument_prompts

import ion.util.procutils as pu
from threading import Timer

from ion.core.process.process import ProcessFactory

#
# DHE: need to do something like: instCmdTranslator = SBE49_InstCommandXlator()
#

RESPONSE_TIMEOUT = 2   # 2 second timeout for response from instrument

class SBE49_instCommandXlator():
    commands = {
        'ds' : 'ds',
        'getsample' : 'ts',
        'baud' : 'baud',
        'start' : 'startnow',
        'stop' : 'stop',
    }

    def translate(self, command):
        return(self.commands[command])

class SBE49InstrumentHsm(InstrumentHsm):

    hsmEvents = [
        'eventConfigured',
        'eventDisconnectComplete',
        'eventCommandReceived',
        'eventConnectionComplete',
        'eventDisconnectReceived',
        'eventDataReceived',
        'eventPromptReceived',
        'eventResponseTimeout'
    ]

    def sendEvent(self, event):
        if event in self.hsmEvents:
            self.onEvent(event)
        else:
            log.critical("Invalid Event: %s" %event)
        

class SBE49InstrumentDriver(InstrumentDriver):
    """
    Maybe some day these values are looked up from a registry of common
        controlled vocabulary
    """

    def __init__(self, *args, **kwargs):
        self.connected = False
        self.instrument = None
        self.command = None
        self.setConnected(False)
        self.dataQueue = []
        self.cmdQueue = []
        self.proto = None
    
        self.instCmdXlator = SBE49_instCommandXlator()
        
        #
        # DHE: trying to objectize the hsm stuff...
        #
        
        #self.Testhsm = instrument_hsm.InstrumentHsm()
        #self.hsm = InstrumentHsm()
        self.hsm = SBE49InstrumentHsm()
        
        #
        # DHE: Testing miros FSM.
        #
        #self.hsm = miros.Hsm()

        # ---------------------------------------------------------------------------
        #                  name                                              parent's
        #                  of                     event                      event
        #                  state                  handler                    handler
        # ---------------------------------------------------------------------------
        self.hsm.addState ( "stateBase",           self.stateBase,           None)
        self.hsm.addState ( "stateUnconfigured",   self.stateUnconfigured,   self.stateBase)
        self.hsm.addState ( "stateConfigured",     self.stateConfigured,     self.stateBase)
        self.hsm.addState ( "stateDisconnecting",  self.stateDisconnecting,  self.stateConfigured)
        self.hsm.addState ( "stateDisconnected",   self.stateDisconnected,   self.stateConfigured)
        self.hsm.addState ( "stateConnecting",     self.stateConnecting,     self.stateConfigured)
        self.hsm.addState ( "stateConnected",      self.stateConnected,      self.stateConfigured)
        self.hsm.addState ( "statePrompted",       self.statePrompted,       self.stateConnected)
        self.hsm.addState ( "stateDisconnecting",  self.stateDisconnecting,  self.stateConfigured)
    
        """
        A translation dictionary to translate from the commands being sent
        from the agent to the actual command understood by the instrument.
        """
        self.sbeParmCommands = {
            "baudrate" : "Baud",
            "outputformat" : "outputformat"
        }

        self.__instrument_parameters = {
            "baudrate": 9600,
            "outputformat": 0,
            "outputsal": "Y",
            "outputsv": "Y",
            "navg": 0,
            "mincondfreq": 0,
            "pumpdelay": 0,
            "tadvance": 0.0625,
            "alpha": 0.03,
            "tau": 7.0,
            "autorun": "Y",
            "tcaldate": "1/1/01",
            "ta0": 0.0,
            "ta1": 0.0,
            "ta2": 0.0,
            "ta3": 0.0,
            "toffset": 0.0,
            "ccaldate": "1/1/01",
            "cg": 0.0,
            "ch": 0.0,
            "ci": 0.0,
            "cj": 0.0,
            "cpcor": 0.0,
            "ctcor": 0.0,
            "cslope": 0.0,
            "pcaldate": "1/1/01",
            "prange": 100.0,
            "poffset": 0.0,
            "pa0": 0.0,
            "pa1": 0.0,
            "pa2": 0.0,
            "ptempa0": 0.0,
            "ptempa1": 0.0,
            "ptempa2": 0.0,
            "ptca0": 0.0,
            "ptca1": 0.0,
            "ptca2": 0.0,
            "ptcb0": 0.0,
            "ptcb1": 0.0,
            "ptcb2": 0.0
        }

        InstrumentDriver.__init__(self, *args, **kwargs)

    def stateBase(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateBase state")
        if caller.tEvt['sType'] == "init":
            # display event
            log.debug("stateBase-%s;" %(caller.tEvt['sType']))
            caller.stateStart(self.stateUnconfigured)
            return 0
        elif caller.tEvt['sType'] == "entry":
            # display event, do nothing 
            log.debug("stateBase-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.debug("stateBase-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("stateBase state: ignoring event %s;" %(caller.tEvt['sType']))
            return 0
        else:
            log.error("stateBase state: unhandled event %s;" %(caller.tEvt['sType']))
            return 0
        return caller.tEvt['sType']

    def stateUnconfigured(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateUnconfigured state")
        if caller.tEvt['sType'] == "init":
            # display event
            log.debug("stateUnconfigured-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "entry":
            # display event, do nothing 
            log.debug("stateUnconfigured-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.debug("stateUnconfigured-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventConfigured":
            log.debug("stateUnconfigured-%s;" %(caller.tEvt['sType']))
            log.info("!!!!!! transitioning to stateConfigured! stateUnconfigured-%s;" %(caller.tEvt['sType']))
            self.hsm.stateTran(self.stateConfigured)
            return 0
        return caller.tEvt['sType']

    def stateConfigured(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateConfigured state")
        if caller.tEvt['sType'] == "init":
            log.info("stateConfigured-%s;" %(caller.tEvt['sType']))
            caller.stateStart(self.stateDisconnected)
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConfigured-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConfigured-%s;" %(caller.tEvt['sType']))
            return 0
        return caller.tEvt['sType']

    def stateDisconnecting(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateDisconnecting state")
        if caller.tEvt['sType'] == "init":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            if (self.proto):
                log.debug("disconnecting from instrument")
                self.proto.transport.loseConnection()
            else:
                log.debug("no proto instance: cannot disconnect")
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectComplete":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateConnected state
            #
            caller.stateTran(self.stateDisconnected)
            return 0
        return caller.tEvt['sType']

    def stateDisconnected(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateDisconnected state")
        if caller.tEvt['sType'] == "init":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventConnectionComplete":
            # TODO: can this happen?????
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            # Send crlf, then transition to stateConnecting
            self.sendCmd(instrument_prompts.PROMPT_INST)
            #self.sendCmd("\r\n")
            caller.stateTran(self.stateConnecting)
            return 0
        # A command has been received from the agent
        # Move to stateConnecting
        #
        # Maybe this can't happen...try removing and testing...
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateConnecting state
            #
            caller.stateTran(self.stateConnecting)
            return 0
        return caller.tEvt['sType']

    def stateConnecting(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateConnecting state")
        if caller.tEvt['sType'] == "init":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            self.getConnected()
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventConnectionComplete":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            #
            # DHE: Don't transition to the stateConnected state
            # until we get the prompt.
            #
            self.sendCmd(instrument_prompts.PROMPT_INST)
            # move to stateConnected
            caller.stateTran(self.stateConnected)
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateDisconnecting state
            #
            caller.stateTran(self.stateDisconnecting)
            return 0
        # Don't think I should get this here...candidate for deletion
        elif caller.tEvt['sType'] == "eventPromptReceived":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            caller.stateTran(self.stateConnected)
            return 0
        return caller.tEvt['sType']

    def stateConnected(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateConnected state")
        if caller.tEvt['sType'] == "init":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            """
            @todo Need a queue of commands from which to pull commands
            """
            # Should we send the prompt here?
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            # We got a command from the agent; need to get the prompt
            # before sending
            self.sendCmd(instrument_prompts.PROMPT_INST)
            return 0
        elif caller.tEvt['sType'] == "eventDataReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))

            # send this up to the agent to publish.
            data = self.dequeueData()            
            log.debug("stateConnected() Calling publish.")
            self.publish("StateChange", "Device", data)
            if 'S>' in data:
                caller.stateTran(self.statePrompted)
            else:
                log.debug("Did not receive prompt")
            return 0
        elif caller.tEvt['sType'] == "eventPromptReceived":
            #
            # Transition to the statePrompted state
            #
            caller.stateTran(self.statePrompted)
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateDisconnecting state
            #
            caller.stateTran(self.stateDisconnecting)
            return 0
        elif caller.tEvt['sType'] == "eventResponseTimeout":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            self.ProcessWakeupResponseTimeout()
            return 0
        return caller.tEvt['sType']

    def statePrompted(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In statePrompted state")
        if caller.tEvt['sType'] == "init":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            """
            @todo Need a queue of commands from which to pull commands
            """
            if self.cmdPending():
                self.sendCmd(self.dequeueCmd())
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            if self.cmdPending():
                self.sendCmd(self.dequeueCmd())
            else:
                log.error("statePrompted: No command to send!")
            return 0
        elif caller.tEvt['sType'] == "eventDataReceived":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))

            # send this up to the agent to publish.
            data = self.dequeueData()            
            log.debug("statePrompted() Calling publish.")
            self.publish("StateChange", "Device", data)
            # TODO
            # Use CONSTANT STRING HERE
            # What if we go back to connected every time??? Don't think this
            # will work for autonomous mode...need to think about that.
            # Might need to have a separate state for auto mode.
            caller.stateTran(self.stateConnected)
            #if 'S>' not in data:
            #    caller.stateTran(self.stateConnected)
            return 0
        elif caller.tEvt['sType'] == "eventPromptReceived":
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateDisconnecting state
            #
            caller.stateTran(self.stateDisconnecting)
            return 0
        elif caller.tEvt['sType'] == "eventResponseTimeout":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            self.ProcessCmdResponseTimeout()
            caller.stateTran(self.stateConnected)
            return 0
        return caller.tEvt['sType']

    def plc_init(self):
        log.debug("SBE49InstrumentDriver.plc_init: spawn_args: %s" %str(self.spawn_args))
        self.instrument_id = self.spawn_args.get('instrument-id', '123')
        self.instrument_ipaddr = self.spawn_args.get('ipaddr', "localhost")
        self.instrument_ipport = self.spawn_args.get('ipport', 9000)
        #self.instrument_ipaddr = self.spawn_args.get('ipaddr', "137.110.112.119")
        #self.instrument_ipport = self.spawn_args.get('ipport', 4001)
        
        # DHE Testing HSM
        log.debug("!!!!!!!!!!!!!!!!!! Calling onStart!")
        self.hsm.onStart(self.stateUnconfigured)

        yield self._configure_driver(self.spawn_args)

        log.info("INIT DRIVER for instrument ID=%s, ipaddr=%s, port=%s" % (
            self.instrument_id, self.instrument_ipaddr, self.instrument_ipport))

#        self.iaclient = InstrumentAgentClient(proc=self, target=self.proc_supid)

#        log.debug("Instrument driver initialized")

        log.debug("!!!!!!!!!!! Sending configured event");
        #self.hsm.onEvent('configured');
        self.hsm.sendEvent('eventConfigured');

    @defer.inlineCallbacks
    def plc_terminate(self):
        yield self.op_disconnect(None, None, None)

    def isConnected(self):
        return self.connected

    def setConnected(self, value):
        self.connected = value;

    def setAgentService(self, agent):
        self.agent = agent
        
    def enqueueData(self, data):
        self.dataQueue.append(data)

    def dequeueData(self):
        data = self.dataQueue.popleft()
        #log.debug("dequeueCmd: dequeueing command: %s" %data)
        return data
        
    def enqueueCmd(self, cmd):
        log.debug("enqueueCmd: enqueueing command: %s" %cmd)
        self.cmdQueue.append(cmd)

    def dequeueCmd(self):
        cmd = self.cmdQueue.popleft()
        #log.debug("dequeueCmd: dequeueing command: %s" %cmd)
        return cmd

    def cmdPending(self):
        if len(self.cmdQueue) > 0:
            return True
        else:
            return False

    def TimeoutCallback(self):
        log.info("TimeoutCallback()")
        if self.TimeOut != None:
            self.TimeOut = None
            self.hsm.sendEvent('eventResponseTimeout');
            
    def ProcessCmdResponseTimeout(self):
        log.error("No response from instrument for cammand \'%s\'" % self.cmdQueue[0])
        self.cmdQueue = []
        self.publish("No response from instrument for cammand \'%s\'".format(self.cmdQueue[0]), self.publish_to)

    def ProcessWakeupResponseTimeout(self):
        log.error("No response from instrument for wakeup")
        self.cmdQueue = []
        self.publish("No response from instrument for wakeup", self.publish_to)
      
    def sendCmd(self, cmd):
        log.debug("Sending Command: %s" %cmd)
        self.instrument.transport.write(cmd)
        # TODO: What if self.TimeOut exists and is running?  
        self.TimeOut = Timer(RESPONSE_TIMEOUT, self.TimeoutCallback)
        self.TimeOut.start()
        
    @defer.inlineCallbacks
    def getConnected(self):
        """
        @brief A method to get connected to the instrument device server via
        a TCP/IP device server.  We need to come up with a more flexible way of
        doing this; like getting a connection object that abstracts the details
        of the protocol. Not sure how easy that would be with Twisted and
        Python.

        @retval None.
        """

        cc = ClientCreator(reactor, InstrumentConnection, self)
        log.info("Driver connecting to instrument ipaddr: %s, ipport: %s" %(self.instrument_ipaddr, self.instrument_ipport))
        self.proto = yield cc.connectTCP(self.instrument_ipaddr, int(self.instrument_ipport))
        log.info("Driver connected to instrument")

    def gotConnected(self, instrument):
        """
        @brief This method is called when a connection has been made to the
        instrument device server.  The instrument protocol object is passed
        as a parameter, and a reference to it is saved.  Call setConnected
        with True argument.
        @param reference to instrument protocol object.
        @retval none
        """
        log.debug("gotConnected!!!")

        self.instrument = instrument

        #
        # Don't need this anymore
        #
        self.setConnected(True)
        
        #self.hsm.onEvent('eventConnectionComplete')
        self.hsm.sendEvent('eventConnectionComplete')


    def gotDisconnected(self, instrument):
        """
        @brief This method is called when a connection to the instrument
        device server has been lost.  The instrument protocol object is passed
        as a parameter.  Call setConnected with False argument.
        @param reference to instrument protocol object.
        @retval none
        """
        log.debug("gotDisconnected!!!")

        #self.hsm.onEvent('eventDisconnectComplete')
        self.hsm.sendEvent('eventDisconnectComplete')

        self.proto = None

    def gotData(self, data):
        """
        @brief The instrument protocol object has received data from the
        instrument. 
        @param data
        @retval none
        """
        
        #
        # TODO: currently doing this here, but this will change to
        # where it is discovered that the command is valid
        #
        if self.TimeOut != None:
             self.TimeOut.cancel()
             self.TimeOut = None
        
        if data == instrument_prompts.INST_PROMPT or \
              data == instrument_prompts.INST_SLEEPY_PROMPT:
            log.debug("gotPrompt()")
            #self.hsm.onEvent('eventPromptReceived')
            self.hsm.sendEvent('eventPromptReceived')
        elif data == instrument_prompts.INST_CONFUSED:
            log.info("Seabird doesn't understand command.")
        else:
            log.debug("gotData() %s." % (data))
            self.enqueueData(data)
            #self.hsm.onEvent('eventDataReceived')
            self.hsm.sendEvent('eventDataReceived')
        
    def gotPrompt(self, instrument):
        log.debug("gotPrompt()")
        #self.hsm.onEvent('eventPromptReceived')
        self.hsm.sendEvent('eventPromptReceived')


    @defer.inlineCallbacks    
    def publish(self, topic, transducer, data):
        """
        Collect some publishable information to hand back to the agent for
        publishing
        @param topic The type of topic that is to be published to. Should be one
        of "StateChange", "ConfigChange", "Error", or "Data".
        @param transducer The transducer name to indicate which queue to publish on
        @param data The message to be published
        """
        log.debug("driver publish()")
        # send it to our supervisor
        yield self.send(self.proc_supid, publish, {"Type":topic, "Transducer":transducer, "Value":data})

    @defer.inlineCallbacks
    def op_initialize(self, content, headers, msg):
        log.debug('In driver initialize')

        yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def op_disconnect(self, content, headers, msg):
        log.debug("in Instrument Driver op_disconnect!")
        #self.hsm.onEvent('eventDisconnectReceived')
        self.hsm.sendEvent('eventDisconnectReceived')
        if msg:
            yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def op_fetch_params(self, content, headers, msg):
        """
        Operate in instrument protocol to get parameter
        @todo Write the code to interface this with something
        """
        assert(isinstance(content, (list, tuple)))
        result = {}
        for param in content:
            result[param] = self.__instrument_parameters[param]
        yield self.reply_ok(msg, result)

    @defer.inlineCallbacks
    def op_set_params(self, content, headers, msg):
        """
        Operate in instrument protocol to set a parameter. Current semantics
        are that, if there is a problem, fail as soon as possible. This may
        leave partial settings made in the device.
        @param content A dict of all the parameters and values to set
        @todo Make this an all-or-nothing and/or rollback-able transaction
            list?
        """

        assert(isinstance(content, dict))
        log.debug("op_set_params content: %s, keys: %s" %(str(content), str(content.keys)))

        error_msg = None
        for param in content.keys():
            if (param not in self.__instrument_parameters):
                # Getting rid of this
                #yield self.reply_err(msg, "Could not set %s" % param)
                error_msg = "Could not set " + str(param)
                log.error(error_msg)
                # NEED TO BREAK OUT HERE: don't send multiple responses
                break;
            else:
                self.__instrument_parameters[param] = content[param]
                if param in self.sbeParmCommands:
                    log.info("current param is: %s" %str(param))
                    command = self.sbeParmCommands[param] + "=" + str(content[param])
                    """
                    Send the command received event.  This should kick off the
                    appropriate sequence of events to get the command sent.
                    """
                    self.enqueueCmd(command)
                    #self.hsm.onEvent('eventCommandReceived')
                    self.hsm.sendEvent('eventCommandReceived')
                else:
                    error_msg = str(param) + " is not a settable parameter"
                    #log.error("%s is not a settable parameter" % str(param))
                    log.error(error_msg)
                    
        if error_msg:
            yield self.reply_err(msg, error_msg)
        else:            
            yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute the given command structure (first element command, rest
        of the elements are arguments)
        @todo actually do something
        """
        assert(isinstance(content, (tuple, list)))

        log.debug("op_execute content: %s" % content)

        if ((content == ()) or (content == [])):
            yield self.reply_err(msg, "Empty command")
            return
        agentCommands = []
        for command in content:
            if command not in instrument_commands:
                log.error("Invalid Command: %s" % command)
                yield self.reply_err(msg, "Invalid Command")
            else:
                log.debug("op_execute translating command: %s" % command)
                instCommand = self.instCmdXlator.translate(command)
                log.debug("op_execute would send command: %s to instrument" % instCommand)
                instCommand += instrument_prompts.PROMPT_INST

                self.enqueueCmd(instCommand)

                """
                Send the command received event.  This should kick off the
                appropriate sequence of events to get the command sent.
                """
                #self.hsm.onEvent('eventCommandReceived')
                self.hsm.sendEvent('eventCommandReceived')

                agentCommands.append(command)
                yield self.reply_ok(msg, agentCommands)


    @defer.inlineCallbacks
    def op_get_status(self, content, headers, msg):
        """
        Return the non-parameter and non-lifecycle status of the instrument.
        This may include a snippit of config or important summary of what the
        instrument may be doing...or even something else.
        @param args a list of arguments that may be given for the status
            retreival.
        @return Return a tuple of (status_code, dict)
        @todo Remove this? Is it even used?
        """
        yield self.reply_ok(msg, "a-ok")

    @defer.inlineCallbacks
    def op_configure_driver(self, content, headers, msg):
        """
        This method takes a dict of settings that the driver understands as
        configuration of the driver itself (ie 'target_ip', 'port', etc.). This
        is the bootstrap information for the driver and includes enough
        information for the driver to start communicating with the instrument.
        @param content A dict with parameters for the driver
        @todo Actually make this stub do something
        """
        assert(isinstance(content, dict))
        
        log.info("!!!!!! op_configure_driver!")
        yield self._configure_driver(content)
        # Do something here, then adjust test case
        yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def _configure_driver(self, params):
        """
        Configures driver params either on startup or on command
        """
        #log.info("!!!!! _configure_driver!")
        log.info("!!!!! _configure_driver! params: %s" %str(params))
        
        if 'ipaddr' in params:
            self.instrument_ipaddr = params['ipaddr']
        else:
            log.debug("%%%%%%%% No ipaddr in params: defaulting to: %s" %self.instrument_ipaddr)
            
        if 'ipport' in params:
            self.instrument_ipport = params['ipport']
        else:
            log.debug("%%%%%%%% No ipport in params: defaulting to: %s" %self.instrument_ipport)

class SBE49InstrumentDriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """


# Spawn of the process using the module name
factory = ProcessFactory(SBE49InstrumentDriver)
