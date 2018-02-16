#!/usr/bin/env python
from ..az.auth import Auth
from ..az.deploy import Deployer
from ..status import StatusReporter
from .. import resources

def DeployBatch(location, group_name, batch_acc_name):
    auth = Auth()
    res_client = auth.ResourceManagementClient()
    res_client.resource_groups.create_or_update(
        group_name, {'location': location}
        )
    dep = Deployer(auth, group_name)
    dep(resources.get('batch', 'batch_account.json'),
            {'batchAccountName': batch_acc_name})
    

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create a Batch account suitable to accept custom VHDs")

    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Increase the verbosity level - can be provided multiple times")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="Decrease the verbosity level")
    parser.add_argument("--location", "-l", default="westeurope",
                        help="Azure data centre to use")
    parser.add_argument("--resource-group", "-g", required=True,
                        help="Name of resource group to put stuff in (required)")
    parser.add_argument("--batch-account-name", "-b", required=True,
                            help="Name of the batch account")
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1
    DeployBatch(args.location, args.resource_group, args.batch_account_name)
    
