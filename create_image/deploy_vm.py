"""A deployer class to deploy a template on Azure"""
from __future__ import print_function
import os.path
from haikunator import Haikunator
import AzHelp
import ssh
from azure.mgmt.compute.compute.models import Image, SubResource, VirtualMachineCaptureParameters

class ImageCreator(object):
    admin_username = 'polnet'
    vm_size = 'Standard_H16r'
    base_vm_name = 'base'
    
    def __init__(self, loc, out_grp_name, out_stg_acc_name, out_cont_name, out_img_name, verbose=1):
        self.location = loc
        
        self.output_group_name = out_grp_name
        self.output_storage_account_name = out_stg_acc_name
        self.output_container_name = out_cont_name
        self.output_image_name = out_img_name
        self.verbosity = verbose
        self.ssh = ssh.CmdRunner()
        
        namer = Haikunator()
        self.base_vhd_container_name = namer.haikunate()
        self.working_group_name = namer.haikunate()
        self.working_storage_account_name = namer.haikunate(delimiter='')
        self.dns_label_prefix = namer.haikunate()
        self.working_group_name='divine-heart-0107'
        self.working_storage_account_name='icyviolet8801'
        self.auth = AzHelp.Auth()
        return

    def critical(self, *args):
        print(*args)
        return
    
    def info(self, *args):
        if self.verbosity >= 1:
            print(*args)
        return
    
    def debug(self, *args):
        if self.verbosity >= 2:
            print(*args)
        return
    
    def __call__(self):
        # with async() as a:
        #     a.run(self.create_temp_sa)
        #     a.run(self.create_out_sa)
        self.create_temp_sa()
        out_acc = self.create_out_sa()
        
        self.deployer = AzHelp.Deployer(self.auth,
                                        self.location, self.working_group_name)
        print "Creating base VM"
        self.create_base_vm()
        
        print "Deprovisioning over ssh"
        self.ssh.run(self.admin_username, self.remote_hostname,
                'sudo /usr/sbin/waagent -deprovision+user --force') 

        print "Deallocate and generalise the VM"
        client = self.auth.ComputeManagementClient()
        client.virtual_machines.deallocate(self.working_group_name, self.base_vm_name).wait()
        client.virtual_machines.generalize(self.working_group_name, self.base_vm_name)

        print "Creating the image from VM"
        vmcp = VirtualMachineCaptureParameters(self.output_image_name, self.output_container_name, False)
        async = client.virtual_machines.capture(self.working_group_name, self.base_vm_name, vmcp)
        template = async.result().output
        
        print "Deleting work resource group " + self.working_group_name
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.delete(self.working_group_name)
        
        print "Deleting work storage container " + self.base_vhd_container_name
        out_acc.BlockBlobService.delete_container(self.base_vhd_container_name)
        return template
    
    def create_temp_sa(self):
        print "Creating temporary resource group " + self.working_group_name
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.working_group_name, {'location': self.location}
            )
        
        print "Creating temporary storage account " + self.working_storage_account_name
        acc = AzHelp.StorageAccount.create(self.auth, self.location, self.working_group_name, self.working_storage_account_name,
                                           'BlobStorage', 'Standard_LRS', 'Hot')
        container = acc.BlockBlobService.create_container('provisioning-data',
                                                          public=AzHelp.blob.PublicAccess.Container)
        print "Uploading files"
        self.tarball_url = container.upload('hemelb.tar.gz', 'hemelb.tar.gz')
        self.script_url = container.upload('node_prep.sh', 'node_prep.sh')
        
        return
    
    def create_out_sa(self):
        print "Creating output resource group " + self.output_group_name
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.output_group_name, {'location': self.location}
            )

        print "Creating output storage account " + self.output_storage_account_name
        acc = AzHelp.StorageAccount.create(self.auth, self.location,
                                           self.output_group_name, self.output_storage_account_name,
                                           'Storage', 'Standard_LRS', 'Hot')
        
        self.base_vhd_url = os.path.join(acc.primary_endpoints.blob,
                                         self.base_vhd_container_name,
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
            'customiseNodeScriptUri': self.script_url,
            'customiseNodeTarballUri': self.tarball_url,
            'adminPublicKey': self.ssh.pubkey,
            'dnsLabelPrefix': self.dns_label_prefix,
            'vhdUri': self.base_vhd_url
            }
        self.deployer('vmtemplate.json', params)
        
        return
    pass

if __name__ == "__main__":
    out_storage_acc = 'polnetretest'
    creator = ImageCreator('westeurope', 'polnet-vhd', out_storage_acc, 'vhds', 'hemelb-0.1.0', verbose=1)
    print creator()
    
