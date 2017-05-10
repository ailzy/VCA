import bdb
from linecache import getline
import os
import sys


def dict_str(d):
    return ', '.join('%s = %r' % v for v in sorted(d.items()))


class VarCollector(object):
    """Trace the execution of python code and collect (print) variables
    at user-defined collecting points.

    """
    def __init__(self):
        self.bdb = bdb.Bdb()    # only use its canonic function
        self.canonic = self.bdb.canonic

        # User-defined collecting points
        #     dict: file -> lineno -> (location, list of cvars)
        self.cpoints = {}
        # Frame stacks with active collecting points.
        self.cp_frames = [(None,)]
        # Set this to True and we will quit.
        self.quitting = False

    def set_collect(self, filename, lineno, cvar):
        """Add a collecting point at `filename`:`lineno`.

        :param cvar: Variables to collect, usually ``(cond, var)``.
        :return: False if the collecting point is invalid (e.g., on an
            empty line).

        """
        filename = self.canonic(filename)
        line = getline(filename, lineno).rstrip()
        if not line:
            return False
        cps = self.cpoints.setdefault(filename, {})
        loc = (filename, lineno, line)
        cvars = cps.setdefault(lineno, (loc, []))[1]
        cvars.append(cvar)
        return True

    def run_script(self, fname):
        # Convenient function to "debug" a script.
        sys.path[0] = os.path.dirname(fname)
        main_env = {
            '__name__'    : '__main__',
            '__file__'    : fname,
            '__builtins__': __builtins__,
            '__doc__'     : None,
            '__package__' : None,
        }

        prev = sys.gettrace()
        sys.settrace(self.trace_dispatch)
        execfile(fname, main_env)
        sys.settrace(prev)

    def get_loc_cvars(self, frame):
        filename = self.canonic(frame.f_code.co_filename)
        if filename not in self.cpoints:
            return
        lineno = frame.f_lineno
        return self.cpoints[filename].get(lineno)

    def collect_anywhere(self, frame):
        return self.canonic(frame.f_code.co_filename) in self.cpoints

    def trace_dispatch(self, frame, event, arg):
        if self.quitting:
            return

        if event == 'call':
            # No stop if there are no collecting points within this frame.
            # However, if further calls are made, we'll check again here.
            return self.trace_dispatch if self.collect_anywhere(frame) else None

        if event == 'exception':
            # Do not set self.quitting as user's code may catch the exception.
            return

        if frame is self.cp_frames[-1][0]:
            loc, cvars = self.cp_frames.pop(-1)[1]
            self.collect(frame, event, arg, loc, cvars)

        if event == 'line':
            cvars = self.get_loc_cvars(frame)
            if cvars:
                self.cp_frames.append((frame, cvars))

    def eval_cond(self, frame, cond):
        try:
            val = eval(cond, frame.f_globals, frame.f_locals)
            return bool(val)
        except:
            # if eval fails, the conservative thing to do is to collect.
            # TODO: warn the user.
            return True

    def eval_var(self, frame, var):
        try:
            return eval(var, frame.f_globals, frame.f_locals)
        except:
            # TODO: Shall we return the exception?
            return None

    def collect(self, frame, event, arg, loc, cond_vars):
        # This is an example of how variables are collected, assume a ``cvar``
        # is ``(cond, var)``.  Users should override this function.
        vars = {}
        for cond, var in cond_vars:
            if var is True:
                # Filter, and also make sure we don't contaminate f_locals.
                vars.update(i for i in frame.f_locals.items()
                            if not i[0].startswith('__'))
                if event == 'return':
                    vars['<retval>'] = arg
            elif cond is True or self.eval_cond(frame, cond):
                vars[var] = self.eval_var(frame, var)

        fname, lineno, line = loc
        print '%30s:%-5d%-6s %-40s%s' % \
            (fname.split('/')[-1], lineno, event, line, dict_str(vars))


if __name__ == '__main__':
    # usage: varcol.py -c [cfg] -c [cfg] ... script [arg] ...
    argv = sys.argv
    del argv[0]

    vc = VarCollector()
    # A hacky way to parse user-defined collecting points.
    while len(argv) > 2 and argv[0] == '-c':
        for l in open(argv[1]):
            fname, lineno, cond, var = l.rstrip().split(':')
            lineno = int(lineno)
            if cond == 'True':
                cond = True
            if var == 'True':
                var = True
            vc.set_collect(fname, lineno, (cond, var))
        del argv[:2]

    script = sys.argv[0]        # Get script filename
    if not os.path.exists(script):
        print 'Error:', script, 'does not exist'
        sys.exit(1)
    vc.run_script(script)
