#!/usr/bin/env python
from __future__ import print_function
import os.path
from haikunator import Haikunator

from ..az.auth import Auth
from ..az.deployer import Deployer
import azure.common
from azure.storage.blob.models import ContainerPermissions

from ..status import StatusReporter
from .. import resources

class ImageTester(StatusReporter):
    
    def __init__(self, loc, grp=None, dns=None, verbosity=1):
        self.verbosity = verbosity
        
        namer = Haikunator()
        
        self.location = loc
        if grp is None:
            grp = namer.haikunate()
        self.working_group_name = grp
        
        if dns is None:
            dns = namer.haikunate()
        self.dns_label_prefix = dns
        
        self.auth = Auth()
        return

    def create(self, image_id):
        self.info("Creating working resource group", self.working_group_name)
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.working_group_name, {'location': self.location}
            )

        self.info("Set up public key access")
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
        self.deployer = Deployer(self.auth, self.working_group_name, name='imgtest_'+Deployer.now_str())
        admin_user = os.getlogin()
        params = {
            'virtualMachineName': 'test',
            'adminUsername': admin_user,
            'adminPublicKey': pubkey,
            'dnsLabelPrefix': self.dns_label_prefix,
            'sourceImage': image_id
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
    
    parser = argparse.ArgumentParser(description="Test your image works by deploying a VM and associated infrastructure")

    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Increase the verbosity level - can be provided multiple times")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="Decrease the verbosity level")
    parser.add_argument("--location", "-l", default="westeurope",
                        help="Azure data centre to use")
    parser.add_argument("--resource-group", "-g",
                        help="Name of resource group to put stuff in")
    parser.add_argument("--dns",
                        help="DNS prefix for VM")
    
    parser.add_argument("image", help="ID of the image to use")
    
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1

    tester = ImageTester(args.location, args.resource_group, args.dns, verbosity=verbosity)
    ssh_cmd = tester.create(args.image)
    command = raw_input("Test VM created. Access with:\n" + ssh_cmd + "\n[K]eep it or [d]elete it?").lower()
    if 'delete'.startswith(command):
        tester.delete()
    else:
        print("Keeping test VM. Remember to delete it's resource group ({}) later".format(tester.working_group_name))
        
