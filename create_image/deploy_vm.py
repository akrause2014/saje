"""A deployer class to deploy a template on Azure"""
import os.path
from haikunator import Haikunator
import AzHelp

class ImageCreator(object):
    
    def __init__(self, rg_name, loc, pub_ssh_key_path='~/.ssh/id_rsa.pub'):
        self.resource_group_name = rg_name
        self.location = loc
        
        pub_ssh_key_path = os.path.expanduser(pub_ssh_key_path)
        # Will raise if file not exists or not enough permission
        with open(pub_ssh_key_path, 'r') as pub_ssh_file_fd:
            self.pub_ssh_key = pub_ssh_file_fd.read()
            
        self.name_generator = Haikunator()

        self.auth = AzHelp.Auth()
        # Note this creates our resource group
        print "Creating resource group " + self.resource_group_name
        self.deployer = AzHelp.Deployer(self.auth,
                                        self.location, self.resource_group_name)
        

    def __call__(self):
        self.storage_account_name = self.name_generator.haikunate(delimiter='')
        saf = AzHelp.BlobStorageAccountFactory(self.auth)
        acc = saf(self.location, self.resource_group_name, self.storage_account_name,
                      'Standard_LRS', 'Hot')
        container = acc.create_block_blob_container('provisioning-data',
                                                    public=AzHelp.PublicAccess.Container)
        print "Uploading files"
        self.tarball_url = container.upload('hemelb.tar.gz', 'hemelb.tar.gz')
        self.script_url = container.upload('node_prep.sh', 'node_prep.sh')
        self.dns_label_prefix = self.name_generator.haikunate()
        print "Creating base VM"
        ssh_arg = self.create_base_vm()
        print ssh_arg
        
    def create_base_vm(self):
        base_name = 'base'
        params = {
            'location': self.location,
            'virtualMachineName': base_name,
            'virtualMachineSize': 'Standard_H16r',
            'adminUsername': 'polnet',
            'virtualNetworkName': base_name + '-vnet',
            'networkInterfaceName': base_name + '-nic',
            'networkSecurityGroupName': base_name + '-nsg',
            'subnetName': 'default',
            'publicIpAddressName': base_name + '-ip',
            'customiseNodeScriptUri': self.script_url,
            'customiseNodeTarballUri': self.tarball_url,
            'adminPublicKey': self.pub_ssh_key,
            'dnsLabelPrefix': self.dns_label_prefix,
            }
        self.deployer('vmtemplate.json', params)
        
        return "{adminUsername}@{dnsLabelPrefix}.{location}.cloudapp.azure.com".format(**params)


if __name__ == "__main__":
    creator = ImageCreator('image_creation', 'westeurope')
    creator()
    
