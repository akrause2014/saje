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
    
class PoolDeleter(StatusReporter):
    # This MUST match the VHD's OS
    AGENT_SKU_ID = 'batch.node.centos 7'
    
    def __init__(self, verbosity=1):
        self.verbosity = verbosity
        
    def __call__(self, group_name, batch_name, pool_name):
        auth = AzHelp.Auth('polnetbatchtest')
        batch_manager = auth.BatchManagementClient()
        account = batch_manager.batch_account.get(group_name, batch_name)
        
        batch_url = account.account_endpoint
        if not batch_url.startswith('https://'):
            batch_url = 'https://' + batch_url
            
        client = auth.BatchServiceClient(base_url=batch_url)
        self.info("Starting delete of pool", pool_name)
        client.pool.delete(pool_name)
    
    pass

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Delete a Batch pool")
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
    
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1

    deleter = PoolDeleter(verbosity)
    deleter(args.resource_group, args.batch_account, args.pool_name)
        
