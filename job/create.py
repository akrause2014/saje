from __future__ import print_function
import os.path
import hashlib
import uuid

from azure.storage.blob.models import ContainerPermissions

from .. import resources
from ..az import batch

from .spec import JobSpec
from ..status import StatusReporter
from .prepare import InputPrepper
from .submitted import SubmittedJob

class JobCreator(StatusReporter):
    node_size = 16
    task_id = 'task'

    def __init__(self, group_name, batch_name, verbosity=1):
        self.verbosity = verbosity
        self.batch = batch.Helper(group_name, batch_name, verbosity=verbosity-1)
        self.input_prep = InputPrepper(group_name, batch_name, verbosity=verbosity-1)
        return

    def __call__(self, pool_name, requested_nodes, job_id, job_spec, input_command_str, cleanup_command=None):

        job = JobSpec.FromJson(job_spec)
        self.info('Job ID:', job_id)

        pool, n_nodes = self._PoolSetup(pool_name, requested_nodes)

        job_output_container = str(job_id)
        self.info('Output container:', job_output_container)
        blob_service = self.batch.storage.block_blob_service
        out_cont = blob_service.create_container(job_output_container)
        out_sas = out_cont.generate_sas(ContainerPermissions.WRITE | ContainerPermissions.READ)
        out_cont_url = 'https://{}/{}'.format(blob_service.primary_endpoint, job_output_container)

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

            num_nodes=n_nodes,
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
        sudoer =  batch.models.UserIdentity(auto_user=batch.models.AutoUserSpecification(elevation_level='admin'))
        pool_info = batch.models.PoolInformation(pool_name)

        if n_nodes == 1:
            job_prep_task = None
            job_rel_task = None
            mpi = None
        else:
            job_prep_task = batch.models.JobPreparationTask(command_line='echo "Job prep required by API but not needed here"')
            job_rel_task = batch.models.JobReleaseTask(command_line='sh ../../uncoordinate.sh',
                                                       user_identity=sudoer)
            mpi = batch.models.MultiInstanceSettings(number_of_instances=n_nodes,
                                                     coordination_command_line="sh ../coordination.sh > coord_out.txt 2> coord_err.txt",
                                                     common_resource_files=[coord_resource])
            pass

        job_param = batch.models.JobAddParameter(id=job_id, pool_info=pool_info,
                                                 display_name=job.name,
                                                 job_preparation_task=job_prep_task,
                                                 job_release_task=job_rel_task)
        self.batch.client.job.add(job_param)
        task_param = batch.models.TaskAddParameter(id=self.task_id,
                                                   resource_files=[run_resource],
                                                   command_line='sudo -u _azbatch ./run.sh',
                                                   multi_instance_settings=mpi,
                                                   user_identity=sudoer)
        self.batch.client.task.add(job_id, task_param)

        if cleanup_task is not None:
            cleanup_param = batch.models.TaskAddParameter(id=self.task_id+'_cleanup',
                                                          command_line=cleanup_command,
                                                          depends_on=batch.models.TaskDependencies(task_ids=[self.task_id]))
            self.batch.client.task.add(job_id, cleanup_param)

        # Set the job to finish once the task is done
        self.batch.client.job.patch(job_id, batch.models.JobPatchParameter(on_all_tasks_complete='terminateJob'))

        return SubmittedJob(self.batch.group, self.batch.name, str(job_id))

    def _PoolSetup(self, pool_name, requested_nodes):
        self.info('Requesting {} node(s) from pool {}/{}'.format(requested_nodes if requested_nodes > 0 else 'all', self.batch.url, pool_name))
        pool = self.batch.client.pool.get(pool_name)
        size = pool.current_dedicated_nodes

        n_nodes = requested_nodes if requested_nodes else size

        if n_nodes > size:
            raise RuntimeError('Requested more nodes that in the pool')

        return pool, n_nodes

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
