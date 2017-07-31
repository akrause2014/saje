#!/usr/bin/env python
from __future__ import print_function
import os.path
import re
from haikunator import Haikunator

from . import AzHelp
import azure.common
from azure.storage.blob.models import ContainerPermissions

from .status import StatusReporter
from . import resources

class ImageTester(StatusReporter):
    id_demangle_regex = re.compile(r'/subscriptions/([0-9a-f\-]*)/resourceGroups/([-.()\w]*)/.*')
    url_regex = re.compile(r'https://([a-z0-9]{3,24}).blob.core.windows.net/([a-zA-Z0-9\-]{3,63})/(.*)')
    
    def __init__(self, loc, grp=None, acc=None, dns=None, verbosity=1):
        self.verbosity = verbosity
        
        namer = Haikunator()
        
        self.location = loc
        if grp is None:
            grp = namer.haikunate()
        self.working_group_name = grp
        
        if acc is None:
            acc = namer.haikunate(delimiter='')
        self.working_storage_account_name = acc

        if dns is None:
            dns = namer.haikunate()
        self.dns_label_prefix = dns
        
        self.auth = AzHelp.Auth()
        return

    def create(self, vhd_url):
        self.info("Creating working resource group", self.working_group_name)
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.working_group_name, {'location': self.location}
            )
        
        self.info("Creating working storage account", self.working_storage_account_name)
        acc = AzHelp.StorageAccount.create(self.auth, self.location, self.working_group_name, self.working_storage_account_name,
                                           'Storage', 'Standard_LRS')
        container = acc.BlockBlobService.create_container('vhds', fail_on_exist=False)
        
        self.info("Copying VHD")
        try:
            container.copy('source.vhd', vhd_url)
        except azure.common.AzureMissingResourceHttpError:
            self.critical('Source VHD not accessible - trying to generate a SAS token')
            
            match = self.url_regex.match(vhd_url)
            assert match, "Can't figure out storage/container/blob names from URL"

            src_account_name = match.group(1)
            src_container_name = match.group(2)
            src_blob = match.group(3)
            
            client = self.auth.StorageManagementClient()
            def find():
                for acc in client.storage_accounts.list():
                    if acc.name == src_account_name:
                        return acc
                raise ValueError("Can't find the storage account '{}' in your subscription".format(name))
            
            acc = find()
            match = self.id_demangle_regex.match(acc.id)
            assert match, "Can't figure out the resource group from the storage account ID"
            src_group_name = match.group(2)

            acc = AzHelp.StorageAccount.open(self.auth, src_group_name, src_account_name)
            blobber = acc.PageBlobService
            cont = blobber.get_container(src_container_name)
            sas_token = cont.generate_sas(ContainerPermissions.READ)
            vhd_url += '?' + sas_token
            self.critical("Generated SAS token - if you need to retry use this URL instead:", vhd_url)
            container.copy('source.vhd', vhd_url)
            pass
        

        pubkey_path = os.path.expanduser('~/.ssh/id_rsa.pub')
        try:
            with open(pubkey_path) as f:
                pubkey = f.read()
                
            self.use_tmp_key = False
        except IOError:
            self.info("Can't read your public key, generating a temporary one for you")
            pubkey = self.gen_pub_key()
            self.use_tmp_key = True

        self.info("Deploying VM")
        self.deployer = AzHelp.Deployer(self.auth, self.working_group_name)
        admin_user = os.getlogin()
        params = {
            'virtualMachineName': 'test',
            'adminUsername': admin_user,
            'adminPublicKey': pubkey,
            'dnsLabelPrefix': self.dns_label_prefix,
            'sourceVhd': container.url('source.vhd'),
            'runVhd': container.url('run.vhd')
            }
        template_path = resources.get('image_creation', 'test_template.json')
        self.deployer(template_path, params)
        ssh_cmd = 'ssh '
        if self.use_tmp_key:
            ssh_cmd += '-i id_tmp '
        ssh_cmd += admin_user + "@" + self.remote_hostname
        return ssh_cmd
        
    def gen_pub_key(self):
        key = paramiko.RSAKey.generate(2048)
        key.write_private_key_file('id_tmp')
        pubkey = "%s %s comment\n" % (key.get_name(), key.get_base64())
        
    def delete(self):
        self.info("Deleting work resource group", self.working_group_name)
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.delete(self.working_group_name)
        
        return
    
    @property
    def remote_hostname(self):
        return '{}.{}.cloudapp.azure.com'.format(self.dns_label_prefix, self.location)
    
    pass

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test your VHD works by deploying a VM and associated infrastructure")

    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Increase the verbosity level - can be provided multiple times")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="Decrease the verbosity level")
    parser.add_argument("--location", "-l", default="westeurope",
                        help="Azure data centre to use")
    parser.add_argument("--resource-group", "-g",
                        help="Name of resource group to put stuff in")
    parser.add_argument("--storage-account",
                        help="Name of storage account for VHDs")
    parser.add_argument("--dns",
                        help="DNS prefix for VM")
    
    parser.add_argument("vhd_url", help="URL of the VHD to use")
    
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1

    tester = ImageTester(args.location, args.resource_group, args.storage_account, args.dns, verbosity=verbosity)
    ssh_cmd = tester.create(args.vhd_url)
    command = raw_input("Test VM created. Access with:\n" + ssh_cmd + "\n[K]eep it or [d]elete it?").lower()
    if 'delete'.startswith(command):
        tester.delete()
    else:
        print("Keeping test VM. Remember to delete it's resource group ({}) later".format(tester.working_group_name))
        
