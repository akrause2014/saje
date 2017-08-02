import time
import azure.batch.models as batchmodels

from .status import StatusReporter
from . import AzHelp
from .BatchHelp import BatchHelper

class PoolDeleter(StatusReporter):
    # This MUST match the VHD's OS
    AGENT_SKU_ID = 'batch.node.centos 7'
    
    def __init__(self, verbosity=1):
        self.verbosity = verbosity
                
    def __call__(self, group_name, batch_name, pool_name):
        batch = BatchHelper(group_name, batch_name, verbosity=self.verbosity-1)
        self.info("Starting delete of pool", pool_name)
        batch.client.pool.delete(pool_name)
    
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
        
