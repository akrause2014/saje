from __future__ import print_function
import time
import azure.batch.models as batchmodels
import os

from .status import StatusReporter
from . import AzHelp
from .BatchHelp import BatchHelper

class PoolStartWaiter(object):
    def __init__(self, client, pool_id, user_params=None):
        self.client = client
        self.pool_id = pool_id
        self.target_states = set((batchmodels.ComputeNodeState.idle,))
        self.user_params = user_params
        return

    def test(self):
        p = self.client.pool.get(self.pool_id)
        if p.resize_errors is not None:
            raise RuntimeError('resize error encountered for pool {}:\n{}'.format(p.id, p.resize_errors[0]))
        nodes = list(self.client.compute_node.list(p.id))
        if len(nodes) < p.target_dedicated_nodes:
            return False
        
        return all(node.state in self.target_states for node in nodes)
        
    def wait(self):
        while not self.test():
            time.sleep(5)
        return

    def print_connection_info(self):
        if not self.user_params:
            return
        
        if self.test():
            nodes = list(self.client.compute_node.list(self.pool_id))
            login = self.client.compute_node.get_remote_login_settings(self.pool_id, nodes[0].id)
            self.user_params['ip_addr'] = login.remote_login_ip_address
            self.user_params['port'] = login.remote_login_port
            print('ssh -p {port} {username}@{ip_addr}\nPW: {password}'.format(**self.user_params))
        else:
            print('username:', self.user_params['username'])
            print('password:', self.user_params['password'])

class PoolCreator(StatusReporter):
    # This MUST match the VHD's OS
    AGENT_SKU_ID = 'batch.node.centos 7'
    
    def __init__(self, group_name, batch_name, vhd_url, vm_size='Standard_H16r', verbosity=1):
        self.verbosity = verbosity
        
        self.account_name = batch_name
        self.batch = BatchHelper(group_name, batch_name, verbosity=verbosity-1)
        
        self.vhd_url = vhd_url
        self.vm_size = vm_size
        self.os_disk = batchmodels.OSDisk(caching='readOnly')
        self.vm_conf = batchmodels.VirtualMachineConfiguration(
            batchmodels.ImageReference(virtual_machine_image_id=self.vhd_url),
            self.AGENT_SKU_ID,
            os_disk=self.os_disk)
        
    def __call__(self, pool_name, n_nodes, create_user=False):
        users = []
        user_params = {}
        if create_user:
            self.info('Configuring SSH access')
            username = os.getlogin()
            pw = AzHelp.GenPw()
            user = batchmodels.UserAccount(name=username,
                                           password=pw,
                                           elevation_level="admin")
            users.append(user)
            user_params['username'] = username
            user_params['password'] = pw
            pass

        self.info('Configuring pool params')
        pool_conf = batchmodels.PoolAddParameter(id=pool_name,
                                                 vm_size=self.vm_size,
                                                 virtual_machine_configuration=self.vm_conf,
                                                 target_dedicated_nodes=n_nodes,
                                                 enable_auto_scale=False,
                                                 enable_inter_node_communication=True,
                                                 max_tasks_per_node=1,
                                                 user_accounts=users)
        self.info('Creating pool', pool_name)
        self.batch.client.pool.add(pool_conf)
        return PoolStartWaiter(self.batch.client, pool_name, user_params)
    
    pass

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create a Batch pool")
    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Increase the verbosity level - can be provided multiple times")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="Decrease the verbosity level")
    
    parser.add_argument("--resource-group", "-g", required=True,
                        help="Name of resource group containing the batch account (required)")
    parser.add_argument("--batch-account", "-b", required=True,
                        help="Name of the batch account to create pool in (required)")
    parser.add_argument("--image-url", "-i", required=True,
                        help="URL of the VHD image to use")
    
    parser.add_argument("--pool-name", "-p", required=True,
                        help="Name for the pool (required)")

    parser.add_argument("--nodes", "-n", required=True, type=int,
                        help="Number of nodes to allocate")

    parser.add_argument("--no-wait", action="store_true",
                        help="Do not wait for the pool to provision and boot")

    parser.add_argument("--create-user", "-c", action="store_true",
                        help="Whether to create a user")
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1

    
    pc = PoolCreator(args.resource_group, args.batch_account, args.image_url)
    waiter = pc(args.pool_name, args.nodes, args.create_user)
    if not args.no_wait:
        waiter.wait()
        
    waiter.print_connection_info()
