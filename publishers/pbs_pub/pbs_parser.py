import time
import json
import logging
from cache import Cache
from pbsrd import CmdParser
from pbsstat import PBSRd, parse_concatenated_json

logger = logging.getLogger('examon')


class JobStateCache:
    """Manages separate caches for different job states with JSON payload"""
    def __init__(self, timeout=None):
        self.finished_jobs = Cache(max_size=100000, timeout=timeout)
        self.running_jobs = Cache(max_size=100000, timeout=timeout)
        self.pending_jobs = Cache(max_size=100000, timeout=timeout)

    def get_cache_for_state(self, state):
        if state == 'F':
            return self.finished_jobs
        elif state == 'R':
            return self.running_jobs
        elif state == 'Q':
            return self.pending_jobs
        return None

    def update_jobs_for_state(self, state, new_job_list):
        """Updates cache for given state and returns jobs that need querying"""
        cache = self.get_cache_for_state(state)
        if not cache:
            return set(), {}

        current_jobs = set(new_job_list)
        cached_jobs = set(cache._store.keys())

        jobs_to_query = current_jobs - cached_jobs
        jobs_to_remove = cached_jobs - current_jobs

        for job_id in jobs_to_remove:
            if job_id in cache._store:
                del cache._store[job_id]

        cached_job_data = {}
        for job_id in (current_jobs & cached_jobs):
            cached_job_data[job_id] = cache.get(job_id)

        return jobs_to_query, cached_job_data

    def update_job_data(self, state, job_id, job_data):
        """Store job data in appropriate cache"""
        cache = self.get_cache_for_state(state)
        if cache:
            if len(cache._store) >= cache._max_size:
                logger.warning("Cache for state {} has reached size limit of {}".format(state, cache._max_size))
            cache.set(job_id, job_data)


class AdaptiveController:
    """PID-like controller for maintaining optimal batch size"""
    def __init__(self, min_batch=50, max_batch=1000, target_time=15.0, p_gain=0.5):
        self.min_batch_size = min_batch
        self.max_batch_size = max_batch
        self.target_time = target_time  
        self.current_batch_size = max_batch  
        self.p_gain = p_gain  
        self.time_window = (0.7 * target_time, 1.3 * target_time)  
        self.history = []  
        self.max_history = 20  
        self.server_load = 'normal'  

    def adjust(self, execution_time, success):
        """
        Adjust batch size based on execution time
        Args:
            execution_time: Time taken for last execution in seconds
            success: Whether the execution succeeded
        Returns:
            New batch size
        """
        if not success:
            self.current_batch_size = max(self.min_batch_size, int(self.current_batch_size * 0.5))
            logger.info("Execution failed, reducing batch size to %d", self.current_batch_size)
            self.server_load = 'high'  
            return self.current_batch_size

        time_lower, time_upper = self.time_window

        self.history.append((self.current_batch_size, execution_time))
        if len(self.history) > self.max_history:
            self.history.pop(0)

        if len(self.history) >= 5:
            recent_samples = self.history[-5:]  
            avg_execution_time = sum(t for _, t in recent_samples) / 5
            avg_batch_size = sum(b for b, _ in recent_samples) / 5

            if (avg_execution_time < self.target_time and
                    avg_batch_size > self.max_batch_size * 0.8):  
                self.server_load = 'normal'
            else:
                self.server_load = 'high'

            logger.info("Server load status: %s (avg exec time: %.2fs, avg batch size: %d)",
                       self.server_load, avg_execution_time, avg_batch_size)

        if time_lower <= execution_time <= time_upper:
            error = self.target_time - execution_time
            adjustment = int(error * self.p_gain * self.current_batch_size / self.target_time)
            adjustment = max(min(adjustment, int(0.1 * self.current_batch_size)), -int(0.1 * self.current_batch_size))
        elif execution_time < time_lower:
            ratio = self.target_time / max(execution_time, 0.1)  
            adjustment = int(min(self.current_batch_size * 0.2, (ratio - 1) * self.current_batch_size * self.p_gain))
        else:
            ratio = execution_time / self.target_time
            adjustment = -int(min(self.current_batch_size * 0.2, (ratio - 1) * self.current_batch_size * self.p_gain))

        self.current_batch_size = max(self.min_batch_size,
                                     min(self.max_batch_size,
                                         self.current_batch_size + adjustment))

        logger.debug("Execution time: %.2fs, Target: %.2fs, Adjustment: %d, New batch size: %d",
                   execution_time, self.target_time, adjustment, self.current_batch_size)

        return self.current_batch_size


class PbsParser:
    """Drop-in replacement for CmdParser with optimized PBS querying"""

    def __init__(self, cmd, schema, host=None, username=None, password=None,
                 timeout=120, pkey=None, skipline=0):
        self.host = host
        self.username = username
        self.password = password
        self.timeout = timeout
        self.pkey = pkey
        self.skipline = skipline
        self.ps = PBSRd()
        self.ps.key = 'Jobs'

        self.controller_target_time = 15.0
        self.controller_min_batch = 25
        self.controller_max_batch = 500

        # Initialize adaptive controller
        self.adaptive_controller = AdaptiveController(min_batch=self.controller_min_batch, max_batch=self.controller_max_batch, target_time=self.controller_target_time)
        self.current_timeout = self.timeout  # Initial timeout value
        self.min_timeout = self.timeout
        self.max_timeout = self.timeout

        self.cmd_timeout = 60
        self.finished_jobs_history = 70

        # Create command parsers for different states
        self.cmd_parsers = {
            'F': CmdParser("""timeout {} qselect -x -tm.gt.$(date -d "{} seconds ago" "+%Y%m%d%H%M") -s F""".format(self.cmd_timeout, self.finished_jobs_history),
                          schema, host=host, username=username, password=password,
                          timeout=30, pkey=pkey, skipline=skipline),
            'R': CmdParser("""timeout {} qselect -s R""".format(self.cmd_timeout),
                          schema, host=host, username=username, password=password,
                          timeout=30, pkey=pkey, skipline=skipline),
            'Q': CmdParser("""timeout {} qselect -s Q""".format(self.cmd_timeout),
                          schema, host=host, username=username, password=password,
                          timeout=30, pkey=pkey, skipline=skipline)
        }

        # Create qstat parser
        self.qstat_parser = CmdParser("""timeout {} qstat -xfF json -J""",
                                    schema, host=host, username=username, password=password,
                                    timeout=timeout, pkey=pkey, skipline=skipline)

        # Initialize cache
        self.state_cache = JobStateCache(timeout=None)

    def read(self):
        """
        Implements the same interface as CmdParser.read()
        Returns: (timestamp, json_data)
        """
        try:
            current_time = time.time()
            all_jobs_data = []

            for state, parser in self.cmd_parsers.items():
                if self.adaptive_controller.server_load == 'high' and state in ['R', 'Q']:
                    logger.info("Server load is high, skipping state %s", state)
                    continue
                try:
                    result = parser.read()
                    if result and len(result) > 1:
                        job_list = result[1].strip().split('\n') if result[1] else []
                        job_list = [j.strip() for j in job_list if j.strip()]

                        logger.info("Found %d jobs in state %s", len(job_list), state)

                        jobs_to_query, cached_job_data = self.state_cache.update_jobs_for_state(state, job_list)

                        logger.info("State %s: %d jobs from cache, %d jobs need querying",
                                   state, len(cached_job_data), len(jobs_to_query))

                        all_jobs_data.extend(cached_job_data.values())

                        if jobs_to_query:
                            jobs_list = list(jobs_to_query)
                            batch_size = self.adaptive_controller.current_batch_size

                            self.current_timeout = min(self.max_timeout,
                                                    max(self.min_timeout,
                                                        int(self.adaptive_controller.target_time * 1.5) + 15))

                            position = 0
                            while position < len(jobs_list):
                                batch_jobs = jobs_list[position:position + batch_size]
                                job_list_str = ' '.join(batch_jobs)
                                try:
                                    start_time = time.time()

                                    self.qstat_parser.tool_cmd = """timeout {} qstat -xfF json -J {}""".format(
                                        self.current_timeout, job_list_str)

                                    qstat_result = self.qstat_parser.read()
                                    execution_time = time.time() - start_time

                                    if qstat_result and len(qstat_result) > 1:
                                        batch_size = self.adaptive_controller.adjust(execution_time, True)

                                        self.current_timeout = min(self.max_timeout,
                                                                max(self.min_timeout,
                                                                    int(execution_time * 3) + 15))

                                        logger.debug("Successful execution. Time: %.2fs, Timeout: %ds, Batch size: %d",
                                                   execution_time, self.current_timeout, batch_size)

                                        self.ps.data = parse_concatenated_json(qstat_result[1])
                                        new_jobs_data = self.ps.get()

                                        logger.info("Retrieved %d new job details for state %s (%d remaining)",
                                                   len(new_jobs_data), state, len(jobs_list) - position - len(new_jobs_data))

                                        for job_id, job_data in new_jobs_data.items():
                                            self.state_cache.update_job_data(state, job_id, job_data)
                                            all_jobs_data.append(job_data)

                                        position += len(batch_jobs)

                                except Exception as e:
                                    logger.exception("Error executing qstat for {} jobs: {}".format(state, e))
                                    execution_time = time.time() - start_time

                                    batch_size = self.adaptive_controller.adjust(execution_time, False)

                                    if 'Jobs' in str(e):
                                        logger.warning("Timeout detected. Timeout: %ds, New batch size: %d",
                                                     self.current_timeout, batch_size)
                                        self.current_timeout = min(self.max_timeout,
                                                                max(self.min_timeout,
                                                                    int(execution_time * 3) + 15))

                                    time.sleep(10)
                                    continue

                except Exception as e:
                    logger.exception("Error getting {} jobs: {}".format(state, e))

            logger.info("Total jobs processed: %d", len(all_jobs_data))

            return (current_time, all_jobs_data)

        except Exception as e:
            logger.exception("Error in PbsParser.read(): {}".format(e))
            return (time.time(), [])