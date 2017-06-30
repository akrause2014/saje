"""A deployer class to deploy a template on Azure"""
import os.path
from haikunator import Haikunator
import AzHelp
import ssh
from azure.mgmt.compute.compute.models import Image, SubResource

class ImageCreator(object):
    admin_username = 'polnet'
    vm_size = 'Standard_H16r'
    base_vm_name = 'base'
    
    def __init__(self, loc, output_rg_name, output_img_name, pub_ssh_key_path='~/.ssh/id_rsa.pub'):
        self.output_group_name = output_rg_name
        self.output_image_name = output_img_name
        
        self.location = loc
        
        pub_ssh_key_path = os.path.expanduser(pub_ssh_key_path)
        # Will raise if file not exists or not enough permission
        with open(pub_ssh_key_path, 'r') as pub_ssh_file_fd:
            self.pub_ssh_key = pub_ssh_file_fd.read()
            
        self.name_generator = Haikunator()
        self.working_group_name = self.name_generator.haikunate()
        
        self.auth = AzHelp.Auth()
        # Note this creates our resource group
        print "Creating temporary resource group " + self.working_group_name
        self.deployer = AzHelp.Deployer(self.auth,
                                        self.location, self.working_group_name)
        

    def __call__(self):        
        self.storage_account_name = self.name_generator.haikunate(delimiter='')
        
        print "Creating storage account " + self.storage_account_name
        saf = AzHelp.BlobStorageAccountFactory(self.auth)
        acc = saf(self.location, self.working_group_name, self.storage_account_name,
                      'Standard_LRS', 'Hot')
        container = acc.create_block_blob_container('provisioning-data',
                                                    public=AzHelp.PublicAccess.Container)
        print "Uploading files"
        self.tarball_url = container.upload('hemelb.tar.gz', 'hemelb.tar.gz')
        self.script_url = container.upload('node_prep.sh', 'node_prep.sh')
        
        print "Creating base VM"
        self.dns_label_prefix = self.name_generator.haikunate()
        self.create_base_vm()
        
        print "Deprovisioning over ssh"
        ssh.run(self.admin_username, self.remote_hostname,
                'sudo /usr/sbin/waagent -deprovision+user --force') 

        print "Deallocate and generalise the VM"
        client = self.auth.ComputeManagementClient()
        client.virtual_machines.deallocate(self.working_group_name, self.base_vm_name).wait()
        client.virtual_machines.generalize(self.working_group_name, self.base_vm_name)

        print "Creating the image from VM"
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.output_group_name, {'location': self.location}
            )
        vm = client.virtual_machines.get(self.working_group_name, self.base_vm_name)
        img = Image(self.location, source_virtual_machine=SubResource(vm.id))
        client.images.create_or_update(self.output_group_name,
                                       self.output_image_name, img).wait()
        print "Deleting work resource group " + self.working_group_name
        res_client.resource_groups.delete(self.working_group_name)
        return
    
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
            'adminPublicKey': self.pub_ssh_key,
            'dnsLabelPrefix': self.dns_label_prefix,
            }
        self.deployer('vmtemplate.json', params)
        
        return
    pass

if __name__ == "__main__":
    creator = ImageCreator('westeurope', 'polnet-images', 'hemelb-0.1.0')
    creator()
    
