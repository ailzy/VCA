"""Msg module is specially designed for sending messages to nsq cluster. 
A sub-process will receive msg from main-process through a share queue.
Producer-consumer mode makes the task asynchronously. Besides, the sub-
process builds a tornado loop, which's commonly known to be a multiplexing
network-IO framework.
"""
__author__ = 'lizhengyang'

from linecache import getline
import tornado.ioloop
from util import singleton
import json, logging, multiprocessing, time, nsq

class IoLoop:
    """IoLoop
    """
    def __init__(self, ip, port, proc_id, queue, conf):
        """__init__(ip, port, proc_id, queue) -> IoLoop
        :param ip: nsq server ip (str)
        :param port: nsq server port (str)
        :param proc_id: process id (int)
        :param queue: share queue (multiprocessing.queue)
        :param conf: conf (ConfigLoader instance)
        :return:
        """
        logging.info("Init IoLoop (main process)")
        self.queue = queue
        # init nsq-connection writer
        self.write = nsq.Writer(['%s:%s' % (ip, port)])
        self.cf = conf
        # file mode, if true writing messages into file
        self.file_mode = self.cf.file_mode
        self.file_name = self.cf.file_name
        self.file_mgr = open(self.file_name, 'w')
        # process id
        self.proc_id = proc_id
        # mutex for callback pipeline
        # only one loop running each time
        self.mutex = False
        # configs for shaking hands with nsq
        # when msg sending begins
        self.shake_topic = self.cf.shake_topic
        self.shake_msg = self.cf.shake_msg
        self.shake_flag = True
        # maximum connect failed times allowed
        self.wrong_msg_cnt = 0
        self.wrong_msg_allow = self.cf.wrong_limit
        # configs for message publishing
        self.topic = self.cf.topic
        self.msg_cnt = 0
        self.ok_cnt = 0
        # flag, indicates to stop server, when rec "EOF"
        self.eof_flag = False
        logging.info("Init IoLoop complete (main process).")

    def __call__(self):
        """ IoLoop object __call__ method
        :return:
        """
        # judge mutex
        if self.mutex:
            return
        else:
            # set mutex to be True, no more loop will step into the
            # following codes
            self.mutex = True

            # now, step into loop...

            # shake hands if shake_flag is True (if succeed, callback_rec will set
            # it to be False, but only wrong_msg_allow times allowed otherwise).
            if self.shake_flag:
                self.write.pub(self.shake_topic, self.shake_msg, self.callback_rec)
                logging.info('Have send shake message to nsq cluster (sub process %d).'%\
                        self.proc_id)
            else:
                # if eof_flag is true, stop the server
                # waiting for enough "ok" responses back
                # this make sure that messages have been received by NSQ-cluster.
                if self.eof_flag:
                    logging.info("IoLoop receive eof, now wait enough 'OK's, " +
                            "receieved %s OKs, published %s MSGs (sub process %d)."%\
                            ((str(self.ok_cnt), str(self.msg_cnt), self.proc_id)))

                    # if "ok" is not enough, sometimes happened when nsq-cluster out of
                    # connection, or msg out of length. At this time, wrong_msg_cnt will
                    # be increased, and stop will be done since wrong_msg_cnt > wrong_msg_allowed.
                    # Thus, we recommend to set wrong_msg_allowed to be a feasible value.
                    # But, if all these mechanisms failed (I think existed actually), in some
                    # extremely case, sub-process will hang up forever.
                    # Please be aware of this setup and contact the author if needed help.
                    if self.ok_cnt >= self.msg_cnt:
                        self.stop()

                else:
                    # start sending, if queue not empty
                    if not self.queue.empty():
                        msg_dict = self.queue.get()
                        # set eof_flag if rec "EOF" from main process
                        if 'EOF' in msg_dict['data']:
                            self.eof_flag = True
                        msg = json.dumps(msg_dict)
                        logging.debug("Publish to nsq server begin, " + 
                                "[msg len is %d bytes] (sub process %d)."%\
                                        (len(msg), self.proc_id))
                        # publish to NSQ-cluster
                        self.write.pub(self.topic, msg, self.callback_rec)
                        if self.file_mode:
                            # if file_mode is True, write to file
                            self.file_mgr.write(msg + '\n')
                            self.file_mgr.flush()
                            logging.info("Write publish msg to file complete.")
                        logging.info("Publish to nsq server end, " +
                                "[pub index is %d] (sub process %d)."%\
                                        (self.msg_cnt, self.proc_id))
                        self.msg_cnt += 1

            # maximum times exceed now, stop the server...
            if self.wrong_msg_cnt > self.wrong_msg_allow:
                logging.info("Message sent to nsq failed times exceeded limit " +
                        "(sub process %d)."%self.proc_id)
                self.stop()
            # step out of loop...

        # set mutex back
        self.mutex = False
    
    def callback_rec(self, conn, msg):
        """callback_rec process msg from nsq.Writer
        :param conn:
        :param msg: response from NSQ-cluster
        :return:
        """
        logging.info("Have receieved responses, " +
                "msg is : %s (sub process %d)"%\
                        (msg, self.proc_id))
        # set shake_flag, ok_cnt, wrong_msg_cnt
        if 'OK' in msg:
            if self.shake_flag:
                self.shake_flag = False
            else:
                self.ok_cnt += 1
        else:
            self.wrong_msg_cnt += 1

    def run(self):
        """running tornado
        :return:
        """
        logging.info("Run IoLoop (sub process %d)."%\
                self.proc_id)
        tornado.ioloop.PeriodicCallback(self, 1).start()
        nsq.run()

    def stop(self):
        """stop tornado
        :return:
        """
        tornado.ioloop.IOLoop.instance().stop()
        logging.info("IoLoop stop (sub process %d)."%\
                self.proc_id)

    def __del__(self):
        """close file
        :return:
        """
        self.file_mgr.close()

class Msg:
    def __init__(self, **kw):
        """one message
        :param kw:
        :return:
        """
        self.idx = kw['index'] if 'index' in kw else 'None'
        self.event = kw['event'] if 'event' in kw else 'None'
        self.value = kw['value'] if 'value' in kw else 'None'
        self.primary = kw['primary'] if 'primary' in kw else 'None'

    def get_v(self):
        """make a dict
        :return: dict
        """
        return {'primary': self.primary, 
                'event': self.event,
                'value': self.value,
                }

class MsgQueue:
    """Each variable collected is stored into
    a queue.
    """
    def __init__(self, idx, fname, lineno, 
            cond, expr1, expr2):
        """one queue, storing a variable
        :param idx: variable number(int)
        :param fname: file name(str)
        :param lineno: line number(int)
        :param cond: collecting condition(bool)
        :param expr1: variable expression(str)
        :param expr2: primary expression(str)
        :return:
        """
        self.idx = idx
        self.fname = fname
        self.lineno = lineno
        self.cond = cond
        self.variable = expr1
        self.primary = expr2
        self.context = getline(fname, lineno).strip()
        self.queue = []
        
    def append(self, msg):
        """push back and filtering with primary value
        :param msg:
        :return:
        """
        if len(self.queue) > 0:
            last_msg = self.queue[-1]
            if msg.primary == last_msg.primary:
                self.queue[-1] = msg
                return
        self.queue.append(msg)

    def clear(self):
        """clear queue
        :return:
        """
        self.queue = []

    def __iter__(self):
        """iterator method
        :return:
        """
        for v in self.queue:
            yield v

    def get_v(self):
        """make msg-queue dict
        :return:
        """
        return {'index': self.idx,
                'fname': self.fname,
                'lineno': self.lineno,
                'cond': self.cond,
                'var': self.variable,
                'primary': self.primary,
                'context': self.context,
                'value': [v.get_v() for v in self.queue],
                }

    def __len__(self):
        return len(self.queue)

@singleton
class MsgQueueMgr:
    """Message Queue Manager
    """
    def __init__(self):
        """init Msg Queue Manager
        :return:
        """
        logging.info("Init MsgQueue (main process).")
        self.cf = ConfigLoader().varcol
        # set thread number
        # actually, larger number(more than 1) may not speed up
        # the seeding, due to the limited cpu-cores
        self.queue = multiprocessing.Queue(self.cf.process_qsize
                or 1000)
        self.proc_num = self.cf.process_num or 1
        # idx2queue, is a dict storing the MsgQueue
        self.idx2queue = dict()
        # messages sending setup
        # maximum var_limit, send to queue if exceeded
        self.var_limit = self.cf.limit
        self.var_cnt = 0
        self.msg_cnt = 0
        # process id
        self.proc_id = 0
        # MsgQueue Inited, storing to idx2queue
        for k, v in self.cf.cpoints.iteritems():
            fname, lineno = k
            for cond, expr1, expr2, idx in v:
                self.idx2queue[idx] =\
                MsgQueue(idx, fname, lineno, cond, expr1, expr2)
        # init sub-process
        self.fork_subprocess()
        # name is loaded from config, and sending with each
        # report transparent.
        self.name = self.cf.name

    def run_subprocess(self, queue):
        """init a IoLoop and activate run function
        :param queue:
        :return:
        """
        ip, port = self.cf.ip, self.cf.port
        IoLoop(ip, port, self.proc_id, queue).run()
        self.proc_id += 1
    
    def fork_subprocess(self):
        """fork sub-process with self.queue shared
        :return:
        """
        logging.info("Fork subprocess begin now (main process).")
        self.process_arr = []
        for i in range(self.proc_num):
            process = multiprocessing.Process(target=self.run_subprocess, args=(self.queue,))
            self.process_arr.append(process)
            process.start()
        logging.info("Fork subprocess complete (main process).")
    
    def wait_subprocess(self):
        """join sub-process
        :return:
        """
        logging.info("wait sub-process stop (main process).")
        for p in self.process_arr:
            p.join()
        self.queue.cancel_join_thread()
        logging.info("Sub processes stoped, join complete.")

    def stop_subprocess(self):
        """stop subprocess with "eof report" sending to sub-process
        :return:
        """
        msg = self._make_eof_msg()
        for i in range(self.proc_num):
            self.queue.put(msg)
        self.wait_subprocess()

    def __call__(self, var):
        """call method of this class
        :param var:
        :return:
        """
        # build one msg
        one_var = Msg(**var)
        self.var_cnt += 1
        if one_var.value == 'EOF':
            # if got "EOF" message
            logging.info("Got eof flag, wait all sub-processes")
            # sending out remaining messages in idx2queue
            msg = self._make_common_msg()
            self._publish_msg(msg)
            # stop sub-process
            self.stop_subprocess()
        else:
            if self.var_cnt == int(self.var_limit):
                # if collected messages exceed the var_limit
                # sending out
                msg = self._make_common_msg()
                self._publish_msg(msg)
                self.var_cnt = 0
            # push the message to idx2queue
            self.idx2queue[one_var.idx].append(one_var)

    def _publish_msg(self, msg):
        """publish msg
        :param msg:
        :return:
        """
        logging.debug("Push all messages to share-queue, start (main process).")
        self.queue.put(msg)
        logging.debug("Push all messages to share-queue, end, " +
                "msg len is %d bytes (main process)."%(len(msg)))

    def _make_common_msg(self):
        """make a report
        :return:
        """
        vec = []
        for i, q in self.idx2queue.iteritems():
            if len(q) > 0:
                logging.debug("Push message, index of var : %s, "%str(i) +
                        "num of var: %d (main process)."%len(q)
                        )
                vec.append(q.get_v())
                q.clear()
        msg = {'index': self.msg_cnt,
                'time': time.time(),
                'name': self.name,
                'type': 'varcol_message',
                'data': vec,
                }
        self.msg_cnt += 1
        # please be aware of "msg" usually means one variable information
        # collected, that __call__ received. But, we also use "msg" repre-
        # sending a report pushed to share queue.
        return msg

    def _make_eof_msg(self):
        # make eof msg
        msg = {'index': self.msg_cnt,
                'time': time.time(),
                'name': self.name,
                'type': 'eof_message',
                'data': ['EOF'],
                }
        self.msg_cnt += 1
        return msg
