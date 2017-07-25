import time
import azure.batch.models as batchmodels

from .status import StatusReporter
from . import AzHelp

class PoolStartWaiter(object):
    def __init__(self, client, pool_id):
        self.client = client
        self.pool_id = pool_id
        self.target_states = set((batchmodels.ComputeNodeState.idle,))
        
        return

    def test(self):
        p = self.client.pool.get(self.pool_id)
        if p.resize_errors is not None:
            raise RuntimeError('resize error encountered for pool {}:\n{}'.format(p.id, resize_errors))
        nodes = list(self.client.compute_node.list(p.id))
        if len(nodes) < p.target_dedicated_nodes:
            return False
        
        return all(node.state in self.target_states for node in nodes)
        
    def wait(self):
        while not self.test():
            time.sleep(5)
        return
    
class PoolCreator(StatusReporter):
    # This MUST match the VHD's OS
    AGENT_SKU_ID = 'batch.node.centos 7'
    
    def __init__(self, group_name, batch_name, vhd_url, vm_size='Standard_H16r'):
        self.account_name = batch_name
        self.auth = AzHelp.Auth('polnetbatchtest')
        batch_manager = AzHelp.Auth().BatchManagementClient()
        self.account = batch_manager.batch_account.get(group_name, batch_name)
        
        batch_url = self.account.account_endpoint
        if not batch_url.startswith('https://'):
            batch_url = 'https://' + batch_url
            
        self.client = self.auth.BatchServiceClient(base_url=batch_url)
        
        self.vhd_url = vhd_url
        self.vm_size = vm_size
        self.os_disk = batchmodels.OSDisk(image_uris=[self.vhd_url],
                                     caching='readOnly')
        self.vm_conf = batchmodels.VirtualMachineConfiguration(os_disk=self.os_disk,
                                                               node_agent_sku_id=self.AGENT_SKU_ID)
        
    def __call__(self, pool_name, n_nodes):
        pool_conf = batchmodels.PoolAddParameter(id=pool_name,
                                                 vm_size=self.vm_size,
                                                 virtual_machine_configuration=self.vm_conf,
                                                 target_dedicated_nodes=n_nodes,
                                                 enable_auto_scale=False,
                                                 enable_inter_node_communication=True,
                                                 max_tasks_per_node=1)
        self.client.pool.add(pool_conf)
        return PoolStartWaiter(self.client, pool_name)
    
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
    
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1

    
    pc = PoolCreator(args.resource_group, args.batch_account, args.image_url)
    waiter = pc(args.pool_name, args.nodes)
    if not args.no_wait:
        waiter.wait()
        