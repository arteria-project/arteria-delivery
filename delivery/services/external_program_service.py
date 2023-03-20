

from tornado.process import Subprocess
from tornado import gen
import logging

from subprocess import PIPE

from delivery.models.execution import ExecutionResult, Execution

log = logging.getLogger(__name__)

class ExternalProgramService(object):
    """
    A service for running external programs
    """

    @staticmethod
    def run(cmd):
        """
        Run a process and do not wait for it to finish
        :param cmd: the command to run as a list, i.e. ['ls','-l', '/']
        :return: A instance of Execution
        """
        log.info(f"Running external command: {' '.join(cmd)}")
        p = Subprocess(cmd,
                       stdout=PIPE,
                       stderr=PIPE,
                       stdin=PIPE)
        return Execution(pid=p.pid, process_obj=p)

    @staticmethod
    @gen.coroutine
    def wait_for_execution(execution):
        """
        Wait for an execution to finish
        :param execution: instance of Execution
        :return: an ExecutionResult for the execution
        """
        status_code = yield execution.process_obj.wait_for_exit(raise_error=False)
        out = execution.process_obj.stdout.read().decode('UTF-8')
        err = execution.process_obj.stderr.read().decode('UTF-8')

        if status_code != 0:
            error_msg = (
                f"Failed to run external command: {err}."
                f" Program returned status code: {status_code}")
            log.error(error_msg)
            raise RuntimeError(error_msg)

        return ExecutionResult(out, err, status_code)
