from __future__ import print_function
import os.path
import time

import azure.batch as batch

from . import resources
from .JobSpec import JobSpec
from .BatchHelp import BatchHelper
from .status import StatusReporter
from .PrepareInput import InputPrepper

class JobChecker(StatusReporter):
    def __init__(self, group_name, batch_name, job_id, verbosity=1):
        self.verbosity = verbosity
        self.batch = BatchHelper(group_name, batch_name, verbosity=verbosity-1)
        self.job_id = job_id

    def wait_for_completion(self, timeout_s=3600):
        t0 = time.time()
        tMax = t0 + timeout_s
        while self.get_state() != batch.models.JobState.completed:
            if time.time() > tMax:
                self.critical("Timed out!")
                raise RunTimeError("Job checking timed out")
            
            self.debug("Job not complete")
            time.sleep(10)

    def get_state(self):
        job_info = self.batch.client.job.get(self.job_id)
           return job_info.state
