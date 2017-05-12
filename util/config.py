"""config module used to load config file. ConfigLoader is a 
singleton class. It is noticed that, logging system is also a
singleton, it will be initialized in ConfigLoader.
"""

__author__ = 'lizhengyang'

from util import singleton
from ConfigParser import ConfigParser
import logging, traceback

class ConfigMap:
    '''store each config members as an attribute
    '''
    def __init__(self):
        pass
    
    def __setattr__(self, name, value):
        self.__dict__[name] = value
    
    def __getattribute__(self, name):
        return self.__dict__[name]
    
    def __getattr__(self, name):
        return None
    
    def __iter__(self):
        for k in self.__dict__:
            yield k
    
    def __str__(self):
        return self.__dict__.__str__()

@singleton
class ConfigLoader:
    def __init__(self, path=None):
        if path:
            self._config_parser = ConfigParser()
            self._config_parser.read(path)
            self._config = ConfigMap()
            self._init()
        logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=self.log_file,  
                filemode='w')  
 
    def _init(self):
        '''init config
        '''

        # mode config
        cf = self._config
        try:
            cf.log_file = self._config_parser.get('CONF', 'Log_file')
            log_mode = self._config_parser.get('CONF', 'Log_mode')
            if 'debug' in log_mode:
                cf.log_mode = logging.DEBUG
            else:
                cf.log_mode = logging.INFO
        except:
            cf.log_file = None
            cf.log_mode = logging.INFO
        logging.basicConfig(level=cf.log_mode,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=cf.log_file,  
                filemode='w')
        logging.info("Init logging complete")
        logging.info("Config mode.log_file is : %s"%cf.log_file)
        logging.info("Config mode.log_mode is : %s"%\
                ('INFO' if cf.log_mode == logging.INFO else 'DEBUG'))

        # varcol config
        #transform the first char of the key to upper-case one
        up = lambda key : str.upper(key[0]) + key[1:]
        #get the config value from the config file
        def get_v(k):
            try:
                return self._config_parser.get("CONF", up(k))
            except:
                #traceback.print_exc() 
                return None
        
        #cf.ip, cf.port, cf.topic = '127.0.0.1', '4151', "varcollect"
        cf.ip, cf.port, cf.topic = (get_v('Nsq.' + x) for x in ('Ip', 'Port', 'Topic'))
        logging.info("Config nsq_ip is : %s" % cf.ip)
        logging.info("Config nsq_port is : %s" % cf.port)
        logging.info("Config nsq_topic is : %s" % cf.topic)
        
        cf.limit = int(get_v('Nsq.Limit'))
        logging.info("Config nsq_limit is : %s" % cf.limit)
        
        cf.name = get_v('Nsq.Name')
        logging.info("Config nsq_name is : %s" % cf.name)
        
        cf.cpoints_file = get_v('Cpoints.Path')
        logging.info("Config cpoints_Path is : %s" % cf.cpoints_file)
        
        cf.cpoints = dict()
        try:
            with open(cf.cpoints_file) as f:
                for idx, l in enumerate(f.readlines()):
                    fname, lineno, cond, var_expr = l.strip('\n').strip().split(':')
                    lineno = int(lineno)
                    if cond == 'True':
                        cond = True
                    if var_expr == 'True':
                        assert cond is True
                        cf.cpoints[(fname, lineno)] = True
                    else:
                        cf.cpoints.setdefault((fname, lineno), [])\
                          .append((cond, var_expr, idx))
        except:
            traceback.print_exc()
        logging.info("Config cpoints is : %s" % cf.cpoints)
        
        cf.shake_msg = get_v('Nsq.Shake_msg')
        logging.info("Config shake_msg is : %s" % cf.shake_msg)
        
        cf.shake_topic = get_v('Nsq.Shake_topic')
        logging.info("Config shake_topic is : %s" % cf.shake_topic)
        
        try:
            cf.nsq_mode = eval(get_v('Output.NSQ_mode').strip())
        except:
            cf.nsq_mode = False
        logging.info("Config nsq_mode is : %s" % str(cf.nsq_mode))
        
        try:
            cf.file_mode = eval(get_v('Output.NSQ_with_File_mode').strip())
            cf.file_name = get_v('Output.NSQ_with_File_name').strip()
        except:
            cf.file_mode = False
            cf.output_file = None
        logging.info("Config file_mode is : %s" % str(cf.file_mode))
        logging.info("Config file_name is : %s" % str(cf.file_name))
        
        cf.wrong_limit = int(get_v('Nsq.Wrong_limit'))
        logging.info("Config wrong_limit is : %s" % cf.wrong_limit)
        
        cf.process_num = int(get_v('Process.Num'))
        logging.info("Config process_num is : %d" % cf.process_num)
        
        cf.process_qsize = int(get_v('Process.Queue_size'))
        logging.info("Config process_qsize is : %d" % cf.process_qsize)

    @property
    def varcol_config_dict(self):
        return self._varcol_config.__dict__
    
    def __getattribute__(self, name):
        pass

    def __getattr__(self, name):
        return self._config
    
    def __str__(self):
        formate = "{config: %s}"
        return formate%(self._config.__str__())
 
if __name__ == '__main__':
    print ConfigLoader('../config.ini')
