"""
Daemonize a process.

Based on: http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
"""
import atexit
import os
import signal
import sys
import time
from typing import Callable, Optional, Tuple

class Daemon(object):
    """
    A generic daemon class.

    Usage:
      d=Daemon(pidfile - filename for pidfile (required)
               runner - function to execute in daemon (required)
               stdin - input filename (default is /dev/null)
               stdout - output filename (default is /dev/null)
               stderr - error filename (default is /dev/null)
               chdir - working directory (default is /)
               umask - umask of new files (default is 0)
              )
      d.start()
      d.stop()
      d.kill()
    """

    def __init__(self,
                 pidfile: str,
                 runner: Callable[[], None],
                 stdin: str = '/dev/null',
                 stdout: str = '/dev/null',
                 stderr: str = '/dev/null',
                 chdir: str = '/',
                 umask: int = 0) -> None:
        """Initialize the Daemon."""
        if not isinstance(pidfile, str):
            raise Exception('pidfile is not a string')
        if not callable(runner):
            raise Exception('runner is not callable')
        self.pidfile = pidfile
        self.runner = runner
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.chdir = chdir
        self.umask = umask

    def _daemonize(self) -> None:
        """
        Do the UNIX double-fork magic.

        See Stevens' "Advanced Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError as e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir(self.chdir)
        os.setsid()
        os.umask(self.umask)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError as e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(self.stdin, 'rb')
        so = open(self.stdout, 'ab+')
        se = open(self.stderr, 'ab+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.delpid)
        pidstr = str(os.getpid())
        pgrpstr = str(os.getpgrp())
        open(self.pidfile, 'w+').write("%s %s\n" % (pidstr, pgrpstr))

    def _send(self,
              kill: Callable[[int, int], None],
              pid: int,
              sig: int,
              wait: bool) -> bool:
        """Use the specified function to send a specified signal."""
        try:
            kill(pid, sig)
            if wait:
                for _ in range(10):
                    time.sleep(1)
                    kill(pid, sig)
                return False
        except OSError as err:
            errstr = str(err)
            if 'No such process' in errstr:
                self.delpid()
            else:
                sys.stdout.write("OSError: %s\n" % errstr)
                sys.exit(1)
        return True

    def _sendsignal(self, pid: int, sig: int, wait: bool = True) -> bool:
        """Send the specified signal to the process."""
        return self._send(os.kill, pid, sig, wait)

    def _sendsignalgrp(self, pid: int, sig: int, wait: bool = True) -> bool:
        """Send the specified signal to the process group."""
        return self._send(os.killpg, pid, sig, wait)

    def delpid(self) -> None:
        """Remove the pidfile, if it exists."""
        if os.path.exists(self.pidfile):
            sys.stdout.write("Deleting pidfile\n")
            os.remove(self.pidfile)

    def getpid(self) -> Tuple[Optional[int], Optional[int]]:
        """Get the pid from the pidfile."""
        pid: Optional[int] = None
        pgrp: Optional[int] = None
        try:
            with open(self.pidfile, 'r') as pf:
                pid, pgrp = [int(x.strip()) for x in pf.read().split()]
        except IOError:
            pid = None
            pgrp = None
        return (pid, pgrp)

    def start(self) -> None:
        """Start the daemon."""
        pid, pgrp = self.getpid()
        if pid:
            message = "pidfile %s already exist. Daemon already running?"
            raise Exception(message % self.pidfile)

        # Start the daemon
        self._daemonize()
        self.runner()

    def stop(self) -> None:
        """Stop the daemon."""
        pid, pgrp = self.getpid()
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return
        if not self._sendsignal(pid, signal.SIGINT):
            sys.stdout.write('SIGINT failed, try SIGQUIT\n')
            if pgrp:
                if not self._sendsignalgrp(pgrp, signal.SIGQUIT):
                    sys.stdout.write('SIGQUIT failed, try SIGKILL\n')
                    self._sendsignalgrp(pgrp, signal.SIGKILL)

    def kill(self) -> None:
        """Kill the daemon."""
        pid, pgrp = self.getpid()
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return
        if not self._sendsignal(pid, signal.SIGQUIT):
            if pgrp:
                self._sendsignalgrp(pgrp, signal.SIGTERM)

    def hardkill(self) -> None:
        """Kill the daemon."""
        pid, pgrp = self.getpid()
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return
        if pgrp:
            self._sendsignalgrp(pgrp, signal.SIGKILL)

    def restart(self) -> None:
        """Restart the daemon."""
        self.stop()
        self.start()
