from __future__ import print_function, unicode_literals
import os.path
import time

#import azure.batch as batch

from ..az import batch
from ..status import StatusReporter
from ..common.cd import cd

class SubmittedJob(StatusReporter):
    def __init__(self, group_name, batch_name, job_id, verbosity=1):
        self.verbosity = verbosity
        self.batch = batch.Helper(group_name, batch_name, verbosity=verbosity-1)
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


    def fetch_output(self, output_path):
        assert self.get_state() ==  batch.models.JobState.completed
        
        output_path = os.path.abspath(output_path)
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        blob_service = self.batch.storage.block_blob_service
        out_cont = blob_service.get_container(self.job_id)
        with cd(output_path):
            for blb in out_cont.list():
                # Make any necessary containing directories
                out_dir = os.path.dirname(blb)
                if out_dir != '' and not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                out_cont.download(blb, blb)
        return
    pass
