# variable-collection-async
# Collecting/Sending Programs' Variables  Asynchronously (NSQ)

DEBUG (Qdb and Varcol)

1. introduction
   Varcol is a variable collecting module.

2. Usage
2.1 Preliminary
2.1.1 example
    Please, make sure of that debug module has been exported to your system path. Then,
    the path of config.ini should be passed to Injector. A simple example like this,

        from debug.inject import Injector
        Injector(config_path).start()
        # User Programmes Here
        Injector().stop()

2.1.2 config.ini's [mode]
    There are three modes in config.ini 's [Mode]. If we set 'Task = 2' in config.ini,
    Variable-Collection mode is chosen.

        # 0, do nothing
        # 1, open debug
        # 2, open varcollect
        Task = 2

    The other remaining options, including log_file and log_mode. The meaning of both is
    obvious, log file path and log file ouput level (debug or info supported). We don't
    suggest you to revise them.

        #log file name
        Log_file = varcol.log
        #debug or info(default)
        Log_mode =

2.2 Variable Collection
2.2.1 Cpoints.ini
    Variable-Collection collects the value of an given expression at each time the given
    line executed. Now, we use our example directory to make things clear.
    It includes three files: cpoints.ini, demo.py user.py. Move all of them to your working
    directory. It's noticed that, 'demo.py' is a typical script with 'user.py' as
    it's model class.

    The aims now are collecting variables in user.py. For example, we aim to collect an
    expression, such as "self.cnt1", at the end of line 19 in user.py. Correspondingly, the
    file cpoints.ini need to add a line as following.

        f_name	no	cond_expr	var_expr	primary_key
        user.py	19	True	self.cnt1	_.get_current_time()

    While the option primary_key is frequently set to be "_.get_current_time()",
    for the fact that, we need only one value of a variable at each timestamp. Variable-
    Collection will substitute the value of a new value if both have the same primary-key.

    Sometimes, we want to collect a variable expression at the ending of a given line when
    some conditions satisfied. This is easily to be done, since the value of variable expre-
    ssion (var_expr) will be collected only when the given condition satisfied, in the other
    words, the value of cond_expr is True.

2.2.2 NSQ
    NSQ is a message queue. Please, make sure you have a nsq node available and set the config
    file as flowing,

        Output.NSQ_mode = True

    There is a option named "with_file_mode", if True, let each report sent to NSQ node be written
    to a file "with_file_name". Tcp connection of the NSQ node should be set correctly. If loss of
    connection to NSQ node, file output will be closed.

        Output.NSQ_with_file_mode = True
        Output.NSQ_with_file_name = varcol.dat
        Nsq.Ip = xx.xx.xx.xx
        Nsq.Port = xx

    The other settings of NSQ cluster are topics and messages-number-limit in a report. For details,
    refer to the codes' comments in msg.py.

        Nsq.Topic = varcol
        Nsq.Name = taskid
        Nsq.Limit = 10000
        Nsq.Shake_topic = comp_target
        Nsq.Shake_msg = <--Hello World Nothing-->
        Nsq.Wrong_limit = 10
        Process.Num = 1
        Process.Queue_size = 100000
3 Qdb
  Pass
