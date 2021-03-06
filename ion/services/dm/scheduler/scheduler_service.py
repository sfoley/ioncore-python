#!/usr/bin/env python

"""
@file ion/services/dm/scheduler/scheduler_service.py
@date 9/21/10
@author Paul Hubbard
@package ion.services.dm.scheduler.service Implementation of the scheduler
"""
import sys
import time
from ion.core.data.store import IndexStore, Query
from ion.core.exception import ApplicationError
from ion.core.object.gpb_wrapper import StructureElement

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer, reactor
from uuid import uuid4

from ion.core.data import cassandra, store
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils
from ion.services.dm.distribution.events import ScheduleEventPublisher

from ion.core.data.storage_configuration_utility import STORAGE_PROVIDER, PERSISTENT_ARCHIVE, get_cassandra_configuration

from ion.util.iontime import IonTime

import ion.util.procutils as pu

# get configuration
from ion.core import ioninit
CONF = ioninit.config(__name__)

# constants from https://confluence.oceanobservatories.org/display/syseng/Scheduler+Events
# import these and use them to schedule your events, they should be in the "desired origin" field
SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE="1001"
SCHEDULE_TYPE_DSC_RSYNC = '1002'

ADDTASK_REQ_TYPE  = object_utils.create_type_identifier(object_id=2601, version=1)
"""
message AddTaskRequest {
    enum _MessageTypeIdentifier {
      _ID = 2601;
      _VERSION = 1;
    }

    // desired_origin is where the event notification will originate from
    //   this is not required to be sent... one will be generated if not
    // interval is seconds between messages
    // payload is ref to some GPB

    optional string desired_origin              = 1;
    optional uint64 interval_seconds            = 2;
    optional sint64 start_time                  = 3;        // format:UNIX epoch, in ms, can be unset, will use current time
    optional sint64 end_time                    = 4;        // format:UNIX epoch, in ms, can be unset
    optional string user_id                     = 5;
    optional net.ooici.core.link.CASRef payload = 6;
}

"""

ADDTASK_RSP_TYPE  = object_utils.create_type_identifier(object_id=2602, version=1)
"""
message AddTaskResponse {
    enum _MessageTypeIdentifier {
      _ID = 2602;
      _VERSION = 1;
    }

    // the string guid
    // the origin  is where the event notifications will come from

    optional string task_id = 1;
    optional string origin  = 2;
}

"""


RMTASK_REQ_TYPE   = object_utils.create_type_identifier(object_id=2603, version=1)
"""
message RmTaskRequest {
    enum _MessageTypeIdentifier {
      _ID = 2603;
      _VERSION = 1;
    }

    // task id is GUID
    optional string task_id = 1;

}
"""

RMTASK_RSP_TYPE   = object_utils.create_type_identifier(object_id=2604, version=1)


QUERYTASK_REQ_TYPE   = object_utils.create_type_identifier(object_id=2605, version=1)
"""
message QueryTaskRequest {
    enum _MessageTypeIdentifier {
      _ID = 2605;
      _VERSION = 1;
    }

    optional string task_regex = 1;

}
"""

QUERYTASK_RSP_TYPE   = object_utils.create_type_identifier(object_id=2606, version=1)
"""
message QueryTaskResponse {
    enum _MessageTypeIdentifier {
      _ID = 2606;
      _VERSION = 1;
    }

    // can be an empty list
    repeated string task_ids = 1;

}
"""

class SchedulerError(ApplicationError):
    """
    Raised when invalid params are passed to an op on the scheduler.
    """
    pass


class SchedulerService(ServiceProcess):
    """
    First pass at a message-based cron service, where you register a send-to address,
    interval and payload, and the scheduler will message you when the timer expires.
    @note this will be subsumed into CEI at some point; consider this a prototype.
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='scheduler',
                                          version='0.1.1',
                                          dependencies=['attributestore'])

    INDICES = ['task_id',
               'desired_origin',
               'interval_seconds',
               'payload',
               'user_id',
               'constant',
               'start_time',
               'end_time'
               ]

    COLUMN_FAMILY = "scheduler"

    class SchedulerIndexStore(IndexStore):
        """
        Specifically derived IndexStore for scheduler use.
        We do NOT want to use class variables for storage, we want fresh copies
        on every instance.
        """
        def __init__(self, *args, **kwargs):
            self.kvs = {}
            self.indices = {}

            IndexStore.__init__(self, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        ServiceProcess.__init__(self, *args, **kwargs)

        index_store_class_name = self.spawn_args.get('index_store_class', CONF.getValue('index_store_class', default=None))
        if index_store_class_name is not None:
            self.index_store_class = pu.get_class(index_store_class_name)
        else:
            self.index_store_class = self.SchedulerIndexStore

        assert store.IIndexStore.implementedBy(self.index_store_class), \
            'The back end class for the index store passed to the scheduler service does not implement the required IIndexStore interface.'

        self._storage_conf = get_cassandra_configuration()
        self._storage_provider = self._storage_conf[STORAGE_PROVIDER]
        
        self._username = self.spawn_args.get("cassandra_username", CONF.getValue("cassandra_username", default=None))
        self._password = self.spawn_args.get("cassandra_password", CONF.getValue("cassandra_password", default=None))
        self._column_family  = self.spawn_args.get("column_family", CONF.getValue("column_family", default=None))    
        
        # Get the configuration for cassandra - may or may not be used depending on the backend class
        #self._storage_conf = get_cassandra_configuration()

        self.mc = MessageClient(proc=self)

        # maps task_ids to IDelayedCall objects, popped off when callback is called, used to cancel tasks
        self._callback_tasks = {}

        # will move pub through the lifecycle states with the service
        self.pub = ScheduleEventPublisher(process=self)
        self.add_life_cycle_object(self.pub)

    @defer.inlineCallbacks
    def slc_init(self):
        if issubclass(self.index_store_class, cassandra.CassandraIndexedStore):
           
            log.info("Instantiating CassandraStore")
            keyspace = self._storage_conf[PERSISTENT_ARCHIVE]['name']
            
            self.scheduled_events = self.index_store_class(self._username, self._password, self._storage_provider, keyspace, self._column_family)
            
            
            yield self.scheduled_events.initialize()
            yield self.scheduled_events.activate()
            
            yield self.register_life_cycle_object(self.scheduled_events)
            log.info("Done with instantiating the Cassandra store")
        
        else:
            self.scheduled_events = self.index_store_class(self, indices=self.INDICES)

        log.info('SLC_INIT Association Service: index store class - %s' % self.index_store_class)

    @defer.inlineCallbacks
    def slc_activate(self):
        # get all items from the store
        query = Query()
        query.add_predicate_eq('constant', '1')
        rows = yield self.scheduled_events.query(query)

        for task_id, tdef in rows.iteritems():
            log.debug("slc_activate: scheduling %s" % task_id)

            # could be None
            try:
                start_time = int(tdef['start_time'])
            except ValueError:
                start_time = None

            self._schedule_event(start_time, int(tdef['interval_seconds']), task_id)
        
        
        
    def slc_terminate(self):
        """
        Called before terminate, this is a good place to tear down the AS and jobs.
        @todo iterate over the list
        foreach task in op_query:
          rm_task(task)
        """
        for k, v in self._callback_tasks.iteritems():
            if v.active():
                v.cancel()

    def _schedule_event(self, starttime, interval, task_id, query_result=None):
        """
        Helper method to schedule and record a callback in the service.
        Used by op_add_task and on startup.

        @param  starttime       The time to start the callbacks. This is used with the interval to calculate the
                                first callback. If None is specified, will use now. Note: the first callback to
                                occur will not happen immediatly, it will be after the first interval has elapsed,
                                whether starttime is specified or not. This parameter should be specified in UNIX
                                epoch format, in ms. You will have to convert the output from time.time() in Python, or
                                use the IonTime utility class.
        @param  interval        The interval to trigger scheduler events, in seconds.
        @param  task_id         The task_id to trigger.
        @param  query_result    Internal param passed on from service activation which contains the query result already
                                instead of making _send_and_reschedule go get it again.
        """
        assert interval and task_id and interval > 0
        curtime = IonTime().time_ms
        starttime = starttime or curtime

        # determine first callback time
        diff = curtime - starttime
        if diff > 0:
            # we started a while ago, so just find what is remaining of the interval from now
            lefttimems = diff % (interval * 1000)
            calctime = interval - int(lefttimems / 1000)
        else:
            # start time is in THE FUTURE
            calctime = 0 - int(diff/1000) + interval

        log.debug("_schedule_event: calculated next callback time of %d" % calctime)

        ccl = reactor.callLater(calctime, self._send_and_reschedule, task_id, query_result=query_result)
        self._callback_tasks[task_id] = ccl

    @defer.inlineCallbacks
    def op_add_task(self, content, headers, msg):
        """
        @brief Add a new task to the crontab. Interval is in seconds.
        @param content Message payload, must be a GPB #2601
        @param headers Ignored here
        @param msg Ignored here
        @retval reply_ok or reply_err
        """
        try:
            task_id         = content.task_id or str(uuid4())
            msg_interval    = content.interval_seconds
            desired_origin  = content.desired_origin
            if content.IsFieldSet('start_time'):
                starttime = content.start_time

                # need to sanity check this input
                starttime_sec = int(starttime / 1000.0)
                oneyearahead_sec = int(time.time()) + 31536000

                if starttime_sec < -sys.maxint-1 or starttime_sec > sys.maxint:
                    raise SchedulerError("start_time %d out of allowable range (%d to %d)" % (starttime, (-sys.maxint-1)*1000, sys.maxint*1000), content.ResponseCodes.BAD_REQUEST)

                # now make sure start time + interval is in the same range
                if starttime_sec + msg_interval < -sys.maxint-1 or starttime_sec + msg_interval > sys.maxint:
                    raise SchedulerError("start_time + interval %d out of allowable range (%d to %d)" % (starttime + msg_interval, (-sys.maxint-1)*1000, sys.maxint*1000), content.ResponseCodes.BAD_REQUEST)

                if starttime_sec > oneyearahead_sec:
                    raise SchedulerError("start_time is more than one year ahead of now, not allowed.", content.ResponseCodes.BAD_REQUEST)
            else:
                starttime = None
            if content.IsFieldSet('payload'):
                # extract, serialize
                payload = content.Repository.index_hash[content.payload.MyId].serialize()
            else:
                payload = None
            if content.IsFieldSet('end_time'):
                log.warn("Scheduler does not handle end_time yet!")
                endtime = content.end_time
            else:
                endtime = None
            if content.IsFieldSet('user_id'):
                user_id = content.user_id
            else:
                user_id = ''

        except KeyError, ke:
            log.exception('Required keys in op_add_task content not found!')
            raise SchedulerError(str(ke), content.ResponseCodes.BAD_REQUEST)

        log.debug('AddTask: about to add task %s' % task_id)

        resp = yield self.mc.create_instance(ADDTASK_RSP_TYPE)

        # check to see if the task_id already exists in the store
        existing_task = yield self.scheduled_events.get(task_id)
        if existing_task is not None:
            log.info("Already have task with id %s scheduled." % task_id)
            resp.duplicate = True
            resp.task_id = task_id
            yield self.reply_ok(msg, resp)
            defer.returnValue(None)

        #create the response: task_id and actual origin
        resp.task_id    = task_id
        resp.origin     = desired_origin

        # extract content of message
        yield self.scheduled_events.put(task_id,
                                        task_id,  # ok to use for value? seems kind of silly
                                        index_attributes={'task_id': task_id,
                                                          'constant': '1',    # used for being able to pull all tasks
                                                          'user_id': user_id,
                                                          'start_time': str(starttime),
                                                          'end_time': str(endtime),
                                                          'interval_seconds': str(msg_interval),
                                                          'desired_origin': desired_origin,
                                                          'payload': str(payload)})

        # Now that task is stored into registry, add to messaging callback
        log.debug('Adding task to scheduler')

        self._schedule_event(starttime, msg_interval, task_id)

        log.debug('Add completed OK')

        yield self.reply_ok(msg, resp)

    @defer.inlineCallbacks
    def op_rm_task(self, content, headers, msg):
        """
        Remove a task from the list/store. Will be dropped from the reactor
        when the timer fires and _send_and_reschedule checks the registry.
        """
        task_id = content.task_id

        if not task_id:
            err = 'required argument task_id not found in message'
            log.error(err)
            self.reply_err(msg, {'value': err})
            return

        # if the task is active, remove it
        if self._callback_tasks.has_key(task_id):
            self._callback_tasks[task_id].cancel()
            del self._callback_tasks[task_id]

        log.debug('Removing task_id %s from store...' % task_id)
        yield self.scheduled_events.remove(task_id)

        resp = yield self.mc.create_instance(RMTASK_RSP_TYPE)
        resp.value = 'OK'

        log.debug('Removal completed')
        yield self.reply_ok(msg, resp)

    ##################################################
    # Internal methods

    @defer.inlineCallbacks
    def _send_and_reschedule(self, task_id, query_result=None):
        """
        Check to see if we're still in the store - if not, we've been removed
        and should abort the run.

        @param  query_result    Internal param - passed in from Scheduler activation, which has already done a query.
                                Allows us to skip making another query for the info we already have.
        """
        log.debug('Worker activated for task %s' % task_id)

        if query_result is None:
            q = Query()
            q.add_predicate_eq('task_id', task_id)

            tdefs = yield self.scheduled_events.query(q)
            if len(tdefs) != 1:
                log.error("Query did not find task_id: %s, expected 1, got %d" % (task_id, len(tdefs)))
                defer.returnValue(False)

            tdef = tdefs.values()[0]
        else:
            tdef = query_result

        # pop callback object off of scheduled items
        if not self._callback_tasks.has_key(task_id):
            log.warn("task_id %s no longer in list of callbacks, aborting" % task_id)
            defer.returnValue(False)
        del self._callback_tasks[task_id]

        # deserialize and objectify payload
        log.debug('Time to send to "%s", id "%s"' % (tdef['desired_origin'], task_id))

        msg = yield self.pub.create_event(origin=tdef['desired_origin'],
                                          task_id=tdef['task_id'],
                                          user_id=tdef['user_id'])

        try:
            se = StructureElement.parse_structure_element(tdef['payload'])
            payload = msg.Repository._load_element(se)
            msg.Repository.index_hash[payload.MyId]=se

            msg.additional_data.payload = payload
        except:
            log.info('No payload found or payload in incorrect format')

        yield self.pub.publish_event(msg, origin=tdef['desired_origin'])

        log.debug('Send completed, rescheduling %s' % task_id)

        #################################################
        ## BANDAID FIX FOR 262 RE-OPEN
        ##
        ## In live system, Jamie noticed a scheduler crash on this line, saying that the Message/Repository
        ## is invalidated already before going into this call, which promptly crashes it and no longer
        ## executes scheduled events. This try/except prevents this from occurring so scheduled events
        ## will still run.
        ##
        ## Proper fix: find out why msg/repo is invalid!
        ##
        #################################################
        try:
            self.workbench.clear_repository(msg.Repository)
        except Exception, ex:
            log.error("Could not clear repository: %s" % str(ex))
            pass

        # start time of None is fine, we just happened so we can be sure interval_seconds is just about right
        self._schedule_event(None, int(tdef['interval_seconds']), task_id)

        log.debug('Task %s rescheduled for %s seconds OK' % (task_id, tdef['interval_seconds']))

class SchedulerServiceClient(ServiceClient):
    """
    Client class for the SchedulerService, simple muster/send/reply.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'scheduler'
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def add_task(self, msg):
        """
        @brief Add a recurring task to the scheduler
        @param msg protocol buffer
        @GPB(Input,2601,1)
        @GPB(Output,2602,1)
        @retval Task ID and origin
        """
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('add_task', msg)
        log.debug("RETURNING %s %s" % (ret.origin, ret.task_id))
        defer.returnValue(ret)


    @defer.inlineCallbacks
    def rm_task(self, msg):
        """
        @brief Remove a task from the scheduler
        @note If using cassandra, writes are delayed
        @param msg protocol buffer
        @GPB(Input,2603,1)
        @GPB(Output,2604,1)
        @retval OK or error
        """
        #log.info("In SchedulerServiceClient: rm_task")
        yield self._check_init()

        (ret, heads, message) = yield self.rpc_send('rm_task', msg)
        defer.returnValue(ret)

# Spawn of the process using the module name
factory = ProcessFactory(SchedulerService)
