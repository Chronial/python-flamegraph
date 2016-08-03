import re
import sys
import time
import os.path
import argparse
import threading
import signal
import collections
import atexit


def get_thread_name(ident):
    for th in threading.enumerate():
        if th.ident == ident:
            return th.getName()
    return str(ident)  # couldn't find, return something useful anyways


def extract_stack(f):
    lst = []
    while f is not None:
        parts = []
        add = parts.append
        if '__name__' in f.f_globals:
            # module name
            add(f.f_globals['__name__'])
            add(':')
        if 'self' in f.f_locals:
            add(f.f_locals['self'].__class__.__name__)
            add('.')
        add(f.f_code.co_name)
        lst.append(''.join(parts))
        f = f.f_back
    lst.reverse()
    return lst


def create_flamegraph_entry(thread_id, frame, collapse_recursion=False):
    # threadname = get_thread_name(thread_id)
    # if threadname == 'MainThread':
    #     threadname = None

    return ';'.join(extract_stack(frame))


class ProfileThread(threading.Thread):
    def __init__(self, fd, interval, filter, collapse_recursion=False):
        threading.Thread.__init__(self, name="FlameGraph Thread")
        self.daemon = True

        self._lock = threading.Lock()
        self._fd = fd
        self._written = False
        self._interval = interval
        self._collapse_recursion = collapse_recursion
        if filter is not None:
            self._filter = re.compile(filter)
        else:
            self._filter = None

        self._stats = collections.defaultdict(int)

        self._keeprunning = True
        self._stopevent = threading.Event()

        atexit.register(self.stop)

    def run(self):
        my_thread = threading.current_thread().ident
        while self._keeprunning:
            for thread_id, frame in sys._current_frames().items():
                if thread_id == my_thread:
                    continue

                entry = create_flamegraph_entry(thread_id, frame, self._collapse_recursion)
                if self._filter is None or self._filter.search(entry):
                    self._stats[entry] += 1

                time.sleep(self._interval)

        self._write_results()

    def _write_results(self):
        with self._lock:
            if self._written:
                return
            self._written = True
            for key in sorted(self._stats.keys()):
                self._fd.write('%s %d\n' % (key, self._stats[key]))
            self._fd.close()

    def num_frames(self, unique=False):
        if unique:
            return len(self._stats)
        else:
            return sum(self._stats.values())

    def stop(self):
        self._keeprunning = False
        self._stopevent.set()
        self._write_results()
        # Wait for the thread to actually stop.
        # Using atexit without this line can result in the interpreter shutting
        # down while the thread is alive, raising an exception.
        self.join()


class Sampler(object):
    """
    A simple stack sampler for low-overhead CPU profiling: samples the call
    stack every `interval` seconds and keeps track of counts by frame. Because
    this uses signals, it only works on the main thread.
    """
    def __init__(self, interval=0.005):
        self.interval = interval
        self._started = None
        self._stack_counts = collections.defaultdict(int)

    def start(self):
        self._started = time.time()
        try:
            signal.signal(signal.SIGVTALRM, self._sample)
        except ValueError:
            raise ValueError('Can only sample on the main thread')

        print('interval:', self.interval)
        signal.setitimer(signal.ITIMER_VIRTUAL, self.interval, self.interval)
        atexit.register(self.stop)

    def _sample(self, signum, frame):
        stack = []
        while frame is not None:
            stack.append(self._format_frame(frame))
            frame = frame.f_back

        stack = ';'.join(reversed(stack))
        self._stack_counts[stack] += 1
        #signal.setitimer(signal.ITIMER_VIRTUAL, self.interval)

    def _format_frame(self, frame):
        f = frame
        parts = []
        add = parts.append
        if '__name__' in f.f_globals:
            # module name
            add(f.f_globals['__name__'])
            add(':')
        if 'self' in f.f_locals:
            add(type(f.f_locals['self']).__name__)
            add('.')
        add(f.f_code.co_name)
        return ''.join(parts)

        return '{}({})'.format(frame.f_code.co_name,
                               frame.f_globals.get('__name__'))

    def output_stats(self):
        if self._started is None:
            return ''
        elapsed = time.time() - self._started
        lines = ['elapsed {}'.format(elapsed),
                 'granularity {}'.format(self.interval)]
        ordered_stacks = sorted(self._stack_counts.items(),
                                key=lambda kv: kv[1], reverse=True)
        lines.extend(['{} {}'.format(frame, count)
                      for frame, count in ordered_stacks])
        return '\n'.join(lines) + '\n'

    def reset(self):
        self._started = time.time()
        self._stack_counts = collections.defaultdict(int)

    def stop(self):
        # self.reset()
        print('profiling stopped')
        signal.setitimer(signal.ITIMER_VIRTUAL, 0)

    def __del__(self):
        self.stop()

    def write_results(self, fd):
        for key in sorted(self._stack_counts.keys()):
            fd.write('%s %d\n' % (key, self._stack_counts[key]))

    def num_frames(self, unique=False):
        if unique:
            return len(self._stack_counts)
        else:
            return sum(self._stack_counts.values())



def start_profile_thread(fd, interval=0.001, filter=None, collapse_recursion=False):
    """Start a profiler thread."""
    profile_thread = ProfileThread(
        fd=fd,
        interval=interval,
        filter=filter,
        collapse_recursion=collapse_recursion)
    profile_thread.start()
    return profile_thread

def main():
    parser = argparse.ArgumentParser(prog='python -m flamegraph', description="Sample python stack frames for use with FlameGraph")
    parser.add_argument('script_file', metavar='script.py', type=str,
            help='Script to profile')
    parser.add_argument('script_args', metavar='[arguments...]', type=str, nargs=argparse.REMAINDER,
            help='Arguments for script')
    parser.add_argument('-o', '--output', nargs='?', type=argparse.FileType('w'), default=sys.stderr,
            help='Save stats to file. If not specified default is to stderr')
    parser.add_argument('-i', '--interval', type=float, nargs='?', default=0.001,
            help='Interval in seconds for collection of stackframes (default: %(default)ss)')
    parser.add_argument('-c', '--collapse-recursion', action='store_true',
            help='Collapse simple recursion (function calls itself) into one stack frame in output')
    parser.add_argument('-f', '--filter', type=str, nargs='?', default=None,
            help='Regular expression to filter which stack frames are profiled.    The '
            'regular expression is run against each entire line of output so you can '
            'filter by function or thread or both.')

    args = parser.parse_args()
    print(args)

    # thread = ProfileThread(args.output, args.interval, args.filter, args.collapse_recursion)

    s = Sampler(args.interval)
    s.start()

    if not os.path.isfile(args.script_file):
        parser.error('Script file does not exist: ' + args.script_file)

    sys.argv = [args.script_file] + args.script_args
    sys.path.insert(0, os.path.dirname(args.script_file))
    script_compiled = compile(open(args.script_file, 'rb').read(), args.script_file, 'exec')
    script_globals = {'__name__': '__main__', '__file__': args.script_file, '__package__': None}

    start_time = time.clock()
    # thread.start()

    try:
        # exec docs say globals and locals should be same dictionary else treated as class context
        exec(script_compiled, script_globals, script_globals)
    finally:
        #thread.stop()
        #thread.join()
        s.stop()
        s.write_results(args.output)
        print('Elapsed Time: %2.2f seconds.    Collected %d stack frames (%d unique)'
              % (time.clock() - start_time, s.num_frames(), s.num_frames(unique=True)))

if __name__ == '__main__':
    main()
