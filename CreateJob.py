from __future__ import print_function
import os.path
import re
import hashlib
import uuid

from azure.storage.blob.models import ContainerPermissions
import azure.batch as batch

from . import AzHelp
from .status import StatusReporter
from .CreatePool import PoolCreator
from . import resources
from .JobSpec import JobSpec

class Blob(object):
    def __init__(self, base, url):
        self.base = base
        self.url = url
        self.resource = batch.models.ResourceFile(self.url, self.base)
        
class Uploader(StatusReporter):
    def __init__(self, container, sas, verbosity=1):
        self.verbosity = verbosity
        self.cont = container
        self.sas = sas

    def file(self, path):
        base = os.path.basename(path)
        url = self.cont.url(base, sas_token=self.sas)
        self.info('Uploading {} -> {}'.format(path, url))
        self.cont.upload(path)
        return Blob(base, url)

    def string(self, blob_name, text):
        url = self.cont.url(blob_name, sas_token=self.sas)
        self.info('Uploading string -> {}'.format(url))
        self.cont.from_str(blob_name, text)
        return Blob(blob_name, url)
    pass

def DemangleId(az_id):
    parts = az_id.split('/')
    
    p = parts.pop(0)
    assert p == ''

    ans = {}
    
    p = parts.pop(0)
    assert p == 'subscriptions'
    ans['subscription'] = parts.pop(0)

    p = parts.pop(0)
    assert p == 'resourceGroups'
    ans['resourceGroup'] = parts.pop(0)
    
    if len(parts):
        p = parts.pop(0)
        assert p == 'providers'
        ans['provider'] = parts.pop(0)
        ans['resource'] = parts.pop(0)
        ans['name'] = parts.pop(0)

        if len(parts):
            ans['subparts'] = parts
    return ans

def IsValidContainerName(c_name):
    """Check the string matches Azure Blob storage container name rules:
    
    A container name must be a valid DNS name, conforming to the following naming rules:

    1. Container names must start with a letter or number, and can
    contain only letters, numbers, and the dash (-) character.

    2. Every dash (-) character must be immediately preceded and
    followed by a letter or number; consecutive dashes are not permitted
    in container names.
    
    3. All letters in a container name must be lowercase.

    4. Container names must be from 3 through 63 characters long.
    """
    # rule 1 and start/finish dashes from 2
    if not re.match('^[a-z0-9][a-z0-9-]*[a-z0-9]$', c_name):
        return False
    
    # rule 2 double dash
    if re.search('--', c_name):
        return False
    
    # rule 3
    if c_name.isupper():
        return False

    # rule 4
    if len(c_name) < 3 or len(c_name) > 63:
        return False

    return True

def JobContainerName(job_id):
    """Implement the Azure Batch conventions on job container names.
    
    https://github.com/Azure/azure-sdk-for-net/tree/vs17Dev/src/SDKs/Batch/Support/FileConventions#job-output-container-name
    """
    # Normalize the job ID to lower case
    norm_id = job_id.lower()
    # If prepending "job-" to the normalized ID gives a valid
    # container name, use that
    c_name = 'job-' + norm_id
    if IsValidContainerName(c_name):
        return c_name
    
    # Calculate the SHA1 hash of the normalized ID, and express it as a 40-character hex string.
    sha1 = hashlib.sha1(c_name).hexdigest()
    # Replace all sequences of one or more hyphens or underscores in
    # the normalized ID by single hyphens, then remove any leading or
    # trailing hyphens.
    c_name = re.sub('[-_]+', '-', norm_id).strip('-')
    # If the resulting string is empty, use the string "job" instead.
    c_name = 'job' if c_name == '' else c_name
    # If the resulting string is longer than 15 characters, truncate
    # it to 15 characters. If truncation results in a trailing hyphen,
    # remove it.
    c_name = c_name[:15].strip('-')
    # The container name is the string "job-", followed by the
    # truncated ID, followed by a hyphen, followed by the hash.
    return 'job-' + c_name +'-' + sha1

class JobCreator(StatusReporter):
    node_size = 16
    task_id = 'task'
    
    def __init__(self, group_name, batch_name, verbosity=1):
        self.verbosity = verbosity
        
        self.auth = AzHelp.Auth('polnetbatchtest')
        
        self.group_name = group_name
        self.batch_name = batch_name

        self.debug('Getting batch account info')
        batch_manager = self.auth.BatchManagementClient()
        self.batch_account = batch_manager.batch_account.get(self.group_name, self.batch_name)
        
        batch_url = self.batch_account.account_endpoint
        if not batch_url.startswith('https://'):
            batch_url = 'https://' + batch_url
        self.debug('Batch URL:', batch_url)
        self.batch_url = batch_url

        storage_id = self.batch_account.auto_storage.storage_account_id
        storage_name = DemangleId(storage_id)['name']
        
        self.debug('Opening storage account', storage_name)
        storage = AzHelp.StorageAccount.open(self.auth, self.group_name, storage_name)
        self.blob_service = storage.BlockBlobService
        
        self.debug('Creating batch client')
        self.batch_client = self.auth.BatchServiceClient(base_url=batch_url)
        return
    
    def __call__(self, pool_name, requested_nodes, job_spec_file):
        job = JobSpec(job_spec_file)
        
        job_id = uuid.uuid4()
        self.info('Job ID:', job_id)
        
        pool, n_nodes = self._PoolSetup(pool_name, requested_nodes)

        job_input_container = 'input-' + job.name
        job_output_container = str(job_id)
        in_uploader, (out_cont_url, out_sas) = self._StorageSetup(job_input_container, job_output_container)
        
        self.info('Processing job spec')
        input_commands = []
        for input_item in job.inputs:
            input_commands += input_item.process(in_uploader)


        exec_commands = []
        for cmd in job.commands:
            exec_commands += cmd.process(len(exec_commands))
            
        output_commands = []
        for output_item in job.outputs:
            output_commands += output_item.process()
        
        self.debug('Preparing run script')
        with open(resources.get('batch', 'run_template.sh')) as f:
            run_script_template = f.read()
                
        run_script = run_script_template.format(
            job_id=job_id,
            input='\n'.join(input_commands),
            commands='\n'.join(exec_commands),
            output='\n'.join(output_commands),
            
            num_cores=self.node_size*n_nodes,
            cores_per_node=self.node_size,
            
            output_container_url=out_cont_url,
            output_sas=out_sas)
        self.debug(run_script)
        
        self.info('Uploading run and coordination scripts')
        run_blob = in_uploader.string('{}.sh'.format(job_id), run_script)
        coord_path = resources.get('batch', 'coordination.sh')
        coord_blob = in_uploader.file(coord_path)

        self.info('Submitting job')
        pool_info = batch.models.PoolInformation(pool_name)
        job_param = batch.models.JobAddParameter(id=job_id, pool_info=pool_info, display_name=job.name)
        
        self.batch_client.job.add(job_param)
        sudoer =  batch.models.UserIdentity(auto_user=batch.models.AutoUserSpecification(elevation_level='admin'))
        mpi = batch.models.MultiInstanceSettings(n_nodes,
                                                 coordination_command_line="../coordination.sh",
                                                 common_resource_files=[coord_blob.resource])
        
        task_param = batch.models.TaskAddParameter(id=self.task_id,
                                                   resource_files=[run_blob.resource],
                                                   command_line='sudo -u _azbatch ./{}.sh'.format(job_id),
                                                   multi_instance_settings=mpi,
                                                   user_identity=sudoer)
        self.batch_client.task.add(job_id, task_param)
        # Set the job to finish once the task is done
        self.batch_client.job.patch(job_id, batch.models.JobPatchParameter(on_all_tasks_complete='terminateJob'))
        
        return str(job_id)
        
    def _PoolSetup(self, pool_name, requested_nodes):
        self.info('Requesting {} node(s) from pool {}/{}'.format(requested_nodes if requested_nodes > 0 else 'all', self.batch_url, pool_name))
        pool = self.batch_client.pool.get(pool_name)
        size = pool.current_dedicated_nodes
        
        n_nodes = requested_nodes if requested_nodes else size
        
        if n_nodes > size:
            raise RuntimeError('Requested more nodes that in the pool')
        
        return pool, n_nodes
    
    def _StorageSetup(self, job_input_container, job_output_container):
        self.debug('Create input container', job_input_container)
        in_cont = self.blob_service.create_container(job_input_container)
        in_sas = in_cont.generate_sas(ContainerPermissions.READ)
        in_uploader = Uploader(in_cont, in_sas, verbosity=self.verbosity-1)
        
        self.debug('Create output container', job_output_container)
        out_cont = self.blob_service.create_container(job_output_container)
        out_sas = out_cont.generate_sas(ContainerPermissions.WRITE)
        out_cont_url = 'https://{}/{}'.format(self.blob_service.primary_endpoint, job_output_container)
        
        return in_uploader, (out_cont_url, out_sas)
    pass

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create a job in your Batch pool")
    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Increase the verbosity level - can be provided multiple times")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="Decrease the verbosity level")
    
    parser.add_argument("--resource-group", "-g", required=True,
                        help="Name of resource group containing the batch account (required)")
    parser.add_argument("--batch-account", "-b", required=True,
                        help="Name of the batch account containing the pool (required)")
    parser.add_argument("--pool-name", "-p", required=True,
                        help="Name of the pool (required)")

    parser.add_argument("--nodes", "-n", default=0, type=int,
                        help="Number of nodes to use - zero => whole pool")

    parser.add_argument("jobspec",
                        help="Job specification file - see resources/batch/example_job.json")
    
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1

    jc = JobCreator(args.resource_group, args.batch_account, verbosity=verbosity)
    jc(args.pool_name, args.nodes, args.jobspec)
