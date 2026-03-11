# -*- coding: utf-8 -*-
"""
Simple scheduler for running jobs at regular intervals
"""
import functools
import time

from onionbalance.common import log

logger = log.get_logger()

jobs = []


class Job(object):
    """
    Object to represent a scheduled job task
    """

    def __init__(self, interval, job_func, *job_args, **job_kwargs):
        self.interval = interval
        self.planned_run_time = time.time()

        # Configure the job function and calling arguments
        self.job_func = functools.partial(job_func, *job_args, **job_kwargs)
        functools.update_wrapper(self.job_func, job_func)

    def __lt__(self, other):
        """
        Jobs are sorted based on their next scheduled run time
        """
        return self.planned_run_time < other.planned_run_time

    @property
    def should_run(self):
        """
        Check if the job should be run now
        """
        return self.planned_run_time <= time.time()

    def run(self, override_run_time=None):
        """
        Run job then reschedule it in the job list
        """
        ret = self.job_func()

        # Pretend the job was scheduled now, if we ran it early with run_all()
        if override_run_time:
            self.planned_run_time = time.time()
        self.planned_run_time += self.interval

        return ret

    def __repr__(self):
        """
        Return human readable representation of the Job and arguments
        """
        args = [repr(x) for x in self.job_func.args]
        kwargs = ["{}={}".format(k, repr(v)) for
                  k, v in self.job_func.keywords.items()]
        return "{}({})".format(self.job_func.__name__,
                               ', '.join(args + kwargs))


class DynamicJob(Job):
    """
    A job whose interval is recomputed dynamically after each run
    by consulting an IntervalPolicy with data from the persistent store.
    """

    def __init__(self, interval_policy, store, job_func, *job_args, **job_kwargs):
        self.interval_policy = interval_policy
        self.store = store
        super().__init__(interval_policy.base_interval, job_func, *job_args, **job_kwargs)

    def run(self, override_run_time=None):
        """Run job, then recompute interval before rescheduling."""
        ret = self.job_func()

        # Recompute the dynamic interval from persistent signal data
        try:
            self.interval = self.interval_policy.get_interval(self.store)
            logger.debug("Dynamic job %s: next interval = %.1f seconds",
                         self.job_func.__name__, self.interval)
        except Exception:
            logger.debug("Failed to compute dynamic interval, keeping %.1f",
                         self.interval, exc_info=True)

        if override_run_time:
            self.planned_run_time = time.time()
        self.planned_run_time += self.interval

        return ret


def add_job(interval, function, *job_args, **job_kwargs):
    """
    Add a job to be executed at regular intervals

    The `interval` value is in seconds, starting from now.
    """
    job = Job(interval, function, *job_args, **job_kwargs)
    jobs.append(job)


def add_dynamic_job(interval_policy, store, function, *job_args, **job_kwargs):
    """
    Add a dynamically-scheduled job whose interval adapts based on
    persistent signal data.
    """
    job = DynamicJob(interval_policy, store, function, *job_args, **job_kwargs)
    jobs.append(job)


def _run_job(job, override_run_time=False):
    """
    Run a job and put it back in the job queue
    """
    return job.run(override_run_time)


def run_all(delay_seconds=0):
    """
    Run all jobs at `delay_seconds` regardless of their schedule
    """
    for job in jobs:
        _run_job(job, override_run_time=True)
        time.sleep(delay_seconds)


def run_forever(check_interval=1):
    """
    Run jobs forever
    """
    while True:
        if not jobs:
            logger.error("No scheduled jobs found, scheduler exiting.")
            return None

        jobs_to_run = (job for job in jobs if job.should_run)
        for job in sorted(jobs_to_run):
            _run_job(job)

        time.sleep(check_interval)
