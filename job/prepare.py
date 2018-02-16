from __future__ import print_function, unicode_literals
import os.path
import hashlib

from azure.storage.blob.models import ContainerPermissions

from .spec import ReproducibleHash    
from ..status import StatusReporter
from ..az import batch

class InputPrepper(StatusReporter):
    def __init__(self, group_name, batch_name, verbosity=1):
        self.verbosity = verbosity
        self.batch = batch.Helper(group_name, batch_name, verbosity=verbosity-1)
        self.blob_service = self.batch.storage.block_blob_service
        return
    
    @staticmethod
    def ComputeHash(input_spec):
        """Generate a unique name that depends on the input
        """
        hashable = [i.ToJson() for i in input_spec]
        # Start with the SHA1 of the input specification
        in_spec_hash = ReproducibleHash(hashable)

        # Now update with the hashes of the actual input files
        hasher = hashlib.sha1(in_spec_hash)
        def filehash(path):
            BLOCKSIZE = 2 << 15
            with open(path, 'rb') as afile:
                buf = afile.read(BLOCKSIZE)
                while len(buf) > 0:
                    hasher.update(buf)
                    buf = afile.read(BLOCKSIZE)
                    
        for input_item in input_spec:
            input_item.apply(filehash)
        return hasher.hexdigest()
    
    def ReadContainer(self, job_input_container):
        self.debug('Getting input commands from existing container')
        in_cont = self.blob_service.get_container(job_input_container)
        input_command_str = in_cont.to_str(job_input_container)
        in_sas = in_cont.generate_sas(ContainerPermissions.READ)
        return input_command_str.format(input_container_sas=in_sas)
    
    def CreateContainer(self, job_input_container, input_spec):
        self.debug('Creating input container')
        in_cont = self.blob_service.create_container(job_input_container, fail_on_exist=True)
        
        def upld(path):
            base = os.path.basename(path)
            url = in_cont.url(base)
            self.debug('Uploading {} -> {}'.format(path, url))
            in_cont.upload(path)
            return "curl '{}?{{input_container_sas}}' > {}\n".format(url, path)
        
        input_commands = []
        for input_item in input_spec:
            input_commands += input_item.apply(upld)
        input_command_str = '\n'.join(input_commands)
        
        # Azure metadata is sent in HTTP heads so escaping it is a
        # nightmare. Just store in a blob with same name at the container
        in_cont.from_str(job_input_container, input_command_str)
        in_sas = in_cont.generate_sas(ContainerPermissions.READ)
        return input_command_str.format(input_container_sas=in_sas)
    
    def __call__(self, input_spec):
        # Input name is the digest
        job_input_container = self.ComputeHash(input_spec)
        self.info('Input container:', job_input_container)
        
        if self.blob_service.exists(job_input_container):
            return self.ReadContainer(job_input_container)
        else:
            return self.CreateContainer(job_input_container, input_spec)
            pass
        
    def ReadOnly(self, input_spec):
        job_input_container = self.ComputeHash(input_spec)
        assert self.blob_service.exists(job_input_container)
        input_command_str = self.ReadContainer(job_input_container)
        in_sas = in_cont.generate_sas(ContainerPermissions.READ)
        return input_command_str.format(input_container_sas=in_sas)
    pass

if __name__ == "__main__":
    import argparse
    from .JobSpec import JobSpec
    
    parser = argparse.ArgumentParser(description="Prepare job input")
    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Increase the verbosity level - can be provided multiple times")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="Decrease the verbosity level")
    
    parser.add_argument("--resource-group", "-g", required=True,
                        help="Name of resource group containing the batch account (required)")
    parser.add_argument("--batch-account", "-b", required=True,
                        help="Name of the batch account containing the pool (required)")

    parser.add_argument("jobspec",
                        help="Job specification file - see resources/batch/example_job.json")
    
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1

    job = JobSpec.open(args.jobspec)
    prep = InputPrepper(args.resource_group, args.batch_account, verbosity=verbosity)
    prep(job.inputs)
