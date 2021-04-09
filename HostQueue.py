from loguru import logger
import gevent
from gevent import subprocess
from gevent.pywsgi import WSGIServer, Environ
from gevent.pool import Pool
from gevent.lock import Semaphore
from gevent.queue import Queue
import sys, os
from collections import deque
import random

#from gevent import monkey
#monkey.patch_all()



PID_FILE = '/tmp/mks_bot_service.pid'
SLEEP_TIMERS = {
    # TIE MKS
    'TIE_DATA_EXTRACT': 10,
    'TIE_DEV_EXTRACT': 10,
    'TIE_DEV_EXEC': 5,
    'TIE_DATA_EXEC': 5,
    'MKS_QUEUE_MASTER': 30,
    # SCR MKS
    'SCR_DATA_EXTRACT': 10,
    'SCR_DEV_EXTRACT': 10,
    'SCR_DEV_EXEC': 5,
    'SCF_DATA_EXEC': 5,
    'SCR_QUEUE_MASTER': 30
}

###############################################################################
# these imports for test only - should be removed

###############################################################################
import subprocess

###############################################################################
class HostQueue:

    def _add_to_queue(self, p_queue: deque, p_item: str) -> None:
        try:
            if p_queue.index(p_item) is not None:
                self.logger.trace(f"Element {p_item} already in queue")
        except ValueError:
            # raised when element is not in queue, so we will add element
            self.logger.trace(f"Adding {p_item} to queue")
            p_queue.append(p_item)

    def _check_if_already_runnning(self) -> tuple:
        if not os.path.isfile(PID_FILE):
            # pid file do not exists, this is OK to start app
            # if file do not exist - create file and put current pid into file
            my_pid = str(os.getpid())
            self.logger.debug(f"My PID is: {my_pid}")
            try:
                with open(PID_FILE, 'w') as file:
                    file.write(my_pid)
            except Exception as e:
                self.logger.error(f"Error while trying to create PID file: {PID_FILE}, {str(e)}")
            return False, None
        else:
            # check if pid file is not orphaned file, by checking PID inside of file
            self.logger.warning(f"PID file exists, {PID_FILE}, checking if process is running")
            with open(PID_FILE, 'r') as file:
                pid_data = file.read().replace("\n", "")
            self.logger.debug(f'PID is: {pid_data}')
            # TODO: check if process in pid file same with one that it should be and PID is not rophaved and recycled
            return (True, pid_data)

    def __init__(self, p_logger, poolsize: int) -> None:
        self.logger = p_logger
        # check if pid file already exists
        is_running = self._check_if_already_runnning()
        if is_running[0]:
            self.logger.critical(f"Service already running, PID is: [{is_running[1]}], PID File is {PID_FILE}")
            exit(-1)

        #self.pool = Pool(poolsize)
        # queues for TIE
        # DEV is for DEV line extraction
        self.q_tie_dev_extract = deque()
        self.q_tie_dev_exec = deque()
        # data is for oracle DATA tickets
        self.q_tie_data_extract = deque()
        self.q_tie_data_exec = deque()
        # we can have one results queue for logs to be pushed somewhere later for both data and dev tickets
        self.q_tie_result = deque()
        # SCR queues
        self.q_scr_extract = deque()
        self.q_scr_exec = deque()
        self.q_scr_result = deque()
        # MKS workers
        gevent.spawn(self._mks_queue_master)
        gevent.spawn(self._mks_dev_extract_master)
        gevent.spawn(self._mks_data_extract_master)
        gevent.spawn(self._mks_dev_exec_master)
        gevent.spawn(self._mks_data_exec_master)
        gevent.spawn(self._mks_result_master)
        # SCR Workers
        #

    def _mks_queue_master(self) -> None:
        # This one is to get tickets from MKS queue and put them into MKS DEV / DATA Extract Queue
        self.logger.info("Starting MKS Queue Master")
        while True:
            try:
                self.logger.debug(f"In MKS queue master")
                self._add_to_queue(self.q_tie_dev_extract, str(random.randint(1, 20)))
                self._add_to_queue(self.q_tie_data_extract, str(random.randint(1, 20)))
                gevent.sleep(SLEEP_TIMERS['MKS_QUEUE_MASTER'])
            except Exception as e:
                self.logger.critical(str(e))

    def _mks_dev_extract_master(self) -> None:
        self.logger.info("Starting MKS DEV Extract Master")
        while True:
            tie = None
            try:
                self.logger.debug(f"In MKS DEV Extract master, queue len: {len(self.q_tie_dev_extract)}")

                if len(self.q_tie_dev_extract) > 0:
                    tie = self.q_tie_dev_extract.popleft()
                    self.logger.debug(f"Extracting [{tie}] element")
                    # testing delay via external command
                    self.logger.info(f"Start DEV extraction for [{tie}]")
                    # TODO PLACE call to MKS DEV line extraction here
                    process = subprocess.Popen("./delay_test", shell=True, stdout=subprocess.PIPE)
                    process.wait()
                    self.logger.debug(process.returncode)
                    for line in process.stdout:
                        self.logger.warning(line)
                # if extraction fine, add it to deploy queue
                self._add_to_queue(self.q_tie_dev_exec, tie)
                gevent.sleep(SLEEP_TIMERS["TIE_DEV_EXTRACT"])
            except Exception as e:
                self.logger.critical(str(e))

    def _mks_data_extract_master(self) -> None:
        self.logger.info("Starting MKS DATA Extract Master")
        tie = None
        while True:
            try:
                self.logger.debug(f"In MKS DATA Extract master, queue len: {len(self.q_tie_data_extract)}")
                if len(self.q_tie_dev_extract) > 0:
                    tie = self.q_tie_data_extract.popleft()
                    self.logger.info(f"Start DATA extraction for [{tie}]")
                    # testing delay via external command
                    self.logger.warning(f"Start running long command for [{tie}]")
                    # TODO PLACE call to MKS DATA extraction here
                    process = subprocess.Popen("./delay_test", shell=True, stdout=subprocess.PIPE)
                    process.wait()
                    self.logger.debug(process.returncode)
                    for line in process.stdout:
                        self.logger.warning(line)
                # if extraction fine, add it to deploy queue
                self._add_to_queue(self.q_tie_data_exec, tie)
                gevent.sleep(SLEEP_TIMERS["TIE_DATA_EXTRACT"])
            except Exception as e:
                self.logger.critical(str(e))

    def _mks_dev_exec_master(self) -> None:
        self.logger.info("Starting MKS Dev Deploy Master")

    def _mks_data_exec_master(self) -> None:
        self.logger.info("Starting MKS Data Deploy Master")

    def _mks_result_master(self) -> None:
        self.logger.info("Starting MKS Result Master")

   ##### SCRs part
    def _scr_queue_master(self) -> None:
        # This one is to get tickets from SCR queue and put them into MKS DEV / DATA Extract Queue
        self.logger.info("Starting SCR Queue Master")
        while True:
            try:
                self.logger.debug(f"In MKS queue master")
                self._add_to_queue(self.q_tie_dev_extract, str(random.randint(1, 20)))
                self._add_to_queue(self.q_tie_data_extract, str(random.randint(1, 20)))
                gevent.sleep(SLEEP_TIMERS['MKS_QUEUE_MASTER'])
            except Exception as e:
                self.logger.critical(str(e))

    def _scr_dev_extract_master(self) -> None:
        self.logger.info("Starting SCR DEV Extract Master")


    def _scr_data_extract_master(self) -> None:
        self.logger.info("Starting SCR DATA Extract Master")

    def _scr_dev_exec_master(self) -> None:
        self.logger.info("Starting SCR Dev Deploy Master")

    def _scr_data_exec_master(self) -> None:
        self.logger.info("Starting SCR Data Deploy Master")

    def _scr_result_master(self) -> None:
        self.logger.info("Starting SCR Result Master")


    def __del__(self) -> None:
        self.logger.info("Exiting from System, cleanup all items")
        try:
            os.unlink(PID_FILE)
            self.logger.debug(f"Deleting PID file: {PID_FILE}")
        except Exception as e:
            self.logger.error(f"Issue while deleting PID file: {PID_FILE}: {str(e)}")

    # def state(self) -> str:
    #
    #     res = f"q_tie_dev_extract: len={len(self.q_tie_dev_extract)}\n\t"
    #
    #     for item in self.q_tie_dev_extract:
    #         res += (item + ", ")
    #
    #     lst = vars(self)
    #     res += "\n"
    #
    #     tmpObj = vars(obj)
    #
    #     for item in tmpObj:
    #         if not isinstance(tmpObj[item], Pool ) and not isinstance(tmpObj[item], logger):
    #             res += f"{item}: {type(item)}: {len(item)}\n"
    #
    #     return res


###############################################################################
###############################################################################

def app(environ, start_response) -> list:
    logger = environ.get("wsgi.errors")

    logger.debug(f"environ={environ}")

    hq = environ.get("hostsqueue")

    status = "200 OK"
    # response = hq.state()
    response = "DeploymentBot\nCurrent state:\n"
    tmpObj = vars(hq)

    for item in sorted(tmpObj):
        if not isinstance(tmpObj[item], Pool) and item not in ["logger"]:
            response += f"{item}: {len(tmpObj[item])}\n"
    '''
    if is_host_in_ip_list(host, BLACKLIST) or not is_host_in_ip_list(host, WHITELIST):
        status, response = "403 Access denied", f"Host {host} is not allowed"
    elif hq.is_host_enqueued(host):
        status, response = "200 OK", f"Host {host} already enqueued"
    elif hq.is_host_executing(host):
        status, response = "200 OK", f"Host {host} already executing"
    else:
        hq.enqueue(host)
        status, response = "200 OK", "OK"
    '''
    logger.info(f"status={status} response={response}")
    start_response(status, [("Content-Type", "text/plain",)])
    return [response.encode("utf-8"), b"\n"]


###############################################################################


def main() -> int:
    logger.info("Starting Main Thread")
    hq = None
    try:
        hq = HostQueue(logger, poolsize=1)
        server = WSGIServer(("", 5000), app, log=logger, error_log=logger, environ=Environ(hostsqueue=hq))
        server.serve_forever()
    except Exception as e:
        logger.critical(f">>>>>>>> {str(e)}")
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt, cleaning up")
    finally:
        if hq is not None:
            del hq
    return 0


if __name__ == "__main__":
    sys.exit(main())
