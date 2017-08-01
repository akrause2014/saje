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
from .JobSpec import JobSpec, ReproducibleHash


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
        storage_name = AzHelp.DemangleId(storage_id)['name']
        
        self.debug('Opening storage account', storage_name)
        storage = AzHelp.StorageAccount.open(self.auth, self.group_name, storage_name)
        self.blob_service = storage.BlockBlobService
        
        self.debug('Creating batch client')
        self.batch_client = self.auth.BatchServiceClient(base_url=batch_url)
        return

    def __call__(self, pool_name, requested_nodes, job_spec_file):
        job = JobSpec.open(job_spec_file)
        
        job_id = uuid.uuid4()
        self.info('Job ID:', job_id)
        
        input_command_str = self._PrepareInput(job.inputs)
        pool, n_nodes = self._PoolSetup(pool_name, requested_nodes)

        job_output_container = str(job_id)
        self.info('Output container:', job_output_container)
        out_cont = self.blob_service.create_container(job_output_container)
        out_sas = out_cont.generate_sas(ContainerPermissions.WRITE | ContainerPermissions.READ)
        out_cont_url = 'https://{}/{}'.format(self.blob_service.primary_endpoint, job_output_container)
        
        self.debug('Processing job spec')
        exec_commands = []
        for cmd in job.commands:
            exec_commands += cmd.process(len(exec_commands))
            
        output_commands = []
        for output_item in job.outputs:
            output_commands += output_item.process()
        
        self.debug('Preparing run script')
        with open(resources.get('batch', 'run_template.sh')) as f:
            run_script_template = f.read()
            pass
        
        run_script = run_script_template.format(
            job_id=job_id,
            input=input_command_str,
            commands='\n'.join(exec_commands),
            output='\n'.join(output_commands),
            
            num_cores=self.node_size*n_nodes,
            cores_per_node=self.node_size,
            
            output_container_url=out_cont_url,
            output_sas=out_sas)
        self.debug(run_script)
        
        self.info('Uploading run and coordination scripts')
        run = 'run.sh'
        run_url = out_cont.url(run, sas_token=out_sas)
        out_cont.from_str(run, run_script)
        run_resource = batch.models.ResourceFile(run_url, run)
        
        coord = 'coordination.sh'
        coord_path = resources.get('batch', coord)
        coord_url = out_cont.url(coord, sas_token=out_sas)
        out_cont.upload(coord_path, coord)
        coord_resource = batch.models.ResourceFile(coord_url, coord)

        self.info('Submitting job')
        pool_info = batch.models.PoolInformation(pool_name)
        job_param = batch.models.JobAddParameter(id=job_id, pool_info=pool_info, display_name=job.name)
        
        self.batch_client.job.add(job_param)
        sudoer =  batch.models.UserIdentity(auto_user=batch.models.AutoUserSpecification(elevation_level='admin'))
        mpi = batch.models.MultiInstanceSettings(n_nodes,
                                                 coordination_command_line="../coordination.sh",
                                                 common_resource_files=[coord_resource])
        
        task_param = batch.models.TaskAddParameter(id=self.task_id,
                                                   resource_files=[run_resource],
                                                   command_line='sudo -u _azbatch ./run.sh',
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

    def _PrepareInput(self, input_spec):
        # Generate a unique name that depends on the input
        hashable = [i.ToJson() for i in input_spec]
        # Start with the SHA1 of the input specification
        in_spec_hash = ReproducibleHash(hashable)

        # Now update with the hashes of the actual input files
        hasher = hashlib.sha1(in_spec_hash)
        def filehash(path):
            BLOCKSIZE = 65536
            with open(path, 'rb') as afile:
                buf = afile.read(BLOCKSIZE)
                while len(buf) > 0:
                    hasher.update(buf)
                    buf = afile.read(BLOCKSIZE)
                    
        for input_item in input_spec:
            input_item.apply(filehash)
        # Input name is the digest
        job_input_container = hasher.hexdigest()
        self.info('Input container:', job_input_container)
        
        if self.blob_service.exists(job_input_container):
            self.debug('Container exists ')
            in_cont = self.blob_service.get_container(job_input_container)
            input_command_str = in_cont.to_str(job_input_container)
        else:
            self.debug('Creating input container', job_input_container)
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
            # nightmare. Just store in a blob with same name at the
            # container
            in_cont.from_str(job_input_container, input_command_str)
            pass
        
        in_sas = in_cont.generate_sas(ContainerPermissions.READ)
        return input_command_str.format(input_container_sas=in_sas)
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
