#!/usr/bin/env python
from __future__ import print_function
import os.path
from haikunator import Haikunator
from azure.mgmt.compute.compute.models import Image, SubResource, VirtualMachineCaptureParameters

from . import AzHelp
from . import ssh
from .status import StatusReporter
from . import resources

class ImageCreator(StatusReporter):
    admin_username = 'imgcreator'
    vm_size = 'Standard_H16r'
    base_vm_name = 'base'
    
    def __init__(self, loc, out_grp_name, out_stg_acc_name, out_cont_name, out_img_name, verbosity=1, keep=False):
        self.verbosity = verbosity
        self.keep = keep
        
        self.location = loc
        self.output_group_name = out_grp_name
        self.output_storage_account_name = out_stg_acc_name
        self.output_container_name = out_cont_name
        self.output_image_name = out_img_name
        self.ssh = ssh.CmdRunner(verbosity=self.verbosity)
        
        namer = Haikunator()
        self.working_group_name = namer.haikunate()
        self.working_storage_account_name = namer.haikunate(delimiter='')
        self.dns_label_prefix = namer.haikunate()
        
        self.auth = AzHelp.Auth()
        return

    
    def __call__(self, script, *other_files):
        self.create_temp_sa(script, *other_files)
        out_acc = self.create_out_sa()
        
        self.deployer = AzHelp.Deployer(self.auth,
                                        self.working_group_name)
        self.info("Creating base VM")
        self.create_base_vm()
        
        self.info("Deprovisioning over ssh")
        self.ssh.run(self.admin_username, self.remote_hostname,
                'sudo /usr/sbin/waagent -deprovision+user -force') 

        self.info("Deallocate and generalise the VM")
        client = self.auth.ComputeManagementClient()
        client.virtual_machines.deallocate(self.working_group_name, self.base_vm_name).wait()
        client.virtual_machines.generalize(self.working_group_name, self.base_vm_name)

        if not self.keep:
            self.info("Deleting work resource group", self.working_group_name)
            res_client = self.auth.ResourceManagementClient()
            res_client.resource_groups.delete(self.working_group_name)
        
        
    
    def create_temp_sa(self, script, *other_files):
        self.info("Creating temporary resource group", self.working_group_name)
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.working_group_name, {'location': self.location}
            )
        
        self.info("Creating temporary storage account", self.working_storage_account_name)
        acc = AzHelp.StorageAccount.create(self.auth, self.location, self.working_group_name, self.working_storage_account_name,
                                           'BlobStorage', 'Standard_LRS', 'Hot')
        container = acc.BlockBlobService.create_container('provisioning-data',
                                                          public=AzHelp.blob.PublicAccess.Container)
        self.info("Uploading files")
        def upld(path):
            base = os.path.basename(path)
            self.debug("{} -> {}".format(path, container.url(base)))
            return container.upload(base, path)
        
        self.customise_cmd = "sh {}".format(os.path.basename(script))
        self.data_urls = [upld(script)]
        for f in other_files:
            self.data_urls.append(upld(f))
               
        return
    
    def create_out_sa(self):
        self.info("Creating output resource group", self.output_group_name)
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.output_group_name, {'location': self.location}
            )

        self.info("Creating output storage account", self.output_storage_account_name)
        acc = AzHelp.StorageAccount.create(self.auth, self.location,
                                           self.output_group_name, self.output_storage_account_name,
                                           'Storage', 'Standard_LRS', 'Hot')
        
        self.base_vhd_url = os.path.join(acc.primary_endpoints.blob,
                                         self.output_container_name,
                                         self.output_image_name + '.vhd')
        return acc
   
    @property
    def remote_hostname(self):
        return '{}.{}.cloudapp.azure.com'.format(self.dns_label_prefix, self.location)
    
    def create_base_vm(self):
        params = {
            'location': self.location,
            'virtualMachineName': self.base_vm_name,
            'virtualMachineSize': self.vm_size,
            'adminUsername': self.admin_username,
            'virtualNetworkName': self.base_vm_name + '-vnet',
            'networkInterfaceName': self.base_vm_name + '-nic',
            'networkSecurityGroupName': self.base_vm_name + '-nsg',
            'subnetName': 'default',
            'publicIpAddressName': self.base_vm_name + '-ip',
            'customiseNodeCommand': self.customise_cmd,
            'customiseNodeUris': self.data_urls,
            'adminPublicKey': self.ssh.pubkey,
            'dnsLabelPrefix': self.dns_label_prefix,
            'vhdUri': self.base_vhd_url
            }
        template_path = resources.get('image_creation', 'vmtemplate.json')
        self.deployer(template_path, params)
        
        return
    pass

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create an Azure VHD image based on Centos 7.1 HPC")
    # -v -g hemeprep --storage-account hemelb --container vhds --image hemelb-0.1.0 node_prep.sh hemelb.tar.gz 

    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Increase the verbosity level - can be provided multiple times")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="Decrease the verbosity level")
    parser.add_argument("--keep", "-k", action="store_true",
                        help="Keep intermediate things (may incur charges!)")
    parser.add_argument("--location", "-l", default="westeurope",
                        help="Azure data centre to use")
    parser.add_argument("--resource-group", "-g", required=True,
                        help="Name of resource group to put the output vhd in (required)")
    parser.add_argument("--storage-account", required=True,
                        help="Name of the storage account to put the output vhd in (required)")
    parser.add_argument("--container", required=True,
                        help="Name of the blob container to put the output vhd in (required)")
    parser.add_argument("--image", required=True,
                        help="Name prefix for the blob name (required)")
    
    parser.add_argument("script", help="Shell script to execute to customise the the VM - will be placed in /var/lib/waagent/custom-script/download/{integer}/")
    parser.add_argument("other_files", nargs="*",
                        help="Zero or more other files needed by the script - will be placed in the same directory as the script")
    
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1
    
    creator = ImageCreator(args.location, args.resource_group, args.storage_account, args.container, args.image, verbosity=verbosity, keep=args.keep)
    creator(args.script, *args.other_files)
    print(creator.base_vhd_url)
    
