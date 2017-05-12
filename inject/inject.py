"""inject module is used to settrace for user programme. The sys.settrace 
allows your own self-designed methods to be called back each time when a 
line of user codes executed, thus we use it to collecting variable values.
"""
__author__ = 'lizhengyang'

import logging
import sys
import traceback
from util import singleton
from varcol import varcol
from util import ConfigLoader

class VarCollector(varcol.VarCollector):
    """for detail of varCollect, please refer to the
    class in varcol module.
    """
    
    def __init__(self, cpoints, pipe=None):
        """VarCollector
        :param cpoints: cpoints dict
        :param pipe: seeding function
        :return:
        """
        super(VarCollector, self).__init__()
        for l, vars in cpoints.items():
            for v in vars:
                self.set_collect(l[0], l[1], v)
        self.pipe = pipe

    def collect(self, frame, event, arg, loc, cond_vars):
        """collect variables
        :param frame: running context
        :param event: variable status
        :param arg:
        :param loc:
        :param cond_vars:
        :return:
        """
        try:
            # TODO: Shall we use a list instead of a dict for vars and msg?
            vars = {}
            for cond, var, primary, idx in cond_vars:
                if cond is True or self.eval_cond(frame, cond):
                    vars[idx] = (self.eval_var(frame, var),
                                 self.eval_var(frame, primary))
            # sending msg through pipe
            if self.pipe is None:
                return
            for idx, v in vars.iteritems():
                msg = dict(index=idx, event=event, value=v[0], primary=v[1])
                self.pipe(msg)
        except:
            self.quitting = True
            logging.error(traceback.format_exc())

@singleton
class Injector:
    # store sys.trace to prev
    # the following procedure will set new sys.trace
    prev = sys.gettrace()
    def __init__(self, config_path=None):
        """Inject init.
        config module is a singleton class, init here.
        :param config_path: config path (str)
        :return:
        """
        self.cf = None
        if config_path:
            self.cf = ConfigLoader(config_path)
        logging.info("Init injector succeed (main process).")

    def start(self):
        """start injection
        :return:
        """
        logging.info("Inject start.")
        try:
            self.pipe_send = None
            nsq_mode = self.cf.nsq_mode
            # nsq mode
            # now it not support file output without nsq-mode open
            if nsq_mode:
                from msg import MsgQueueMgr
                self.pipe_send = MsgQueueMgr()
            # init varcol
            varcol = VarCollector(self.cf.cpoints, self.pipe_send)
            sys.settrace(varcol.trace_dispatch)
            logging.info("Set trace complete.")
        except:
            traceback.print_exc()
    
    def stop(self):
        """stop injection
        push eof flag, let sub-process end.
        set the trace prev back, so as to avoid system faults.
        :return:
        """
        logging.info("Inject stop.")
        if self.pipe_send:
            logging.info("Put eof flag.")
            self.pipe_send({'value': 'EOF'})
        sys.settrace(self.prev)
        logging.info("Exit now.")
