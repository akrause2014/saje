#!/usr/bin/env python
from __future__ import print_function
import os.path
import time
import subprocess

from . import AzHelp
from .status import StatusReporter

class ImageCreator(StatusReporter):
    vm_size = 'Standard_H16r'
    
    def __init__(self, loc, out_grp_name, out_img_name, verbosity=1):
        self.verbosity = verbosity
        
        self.location = loc
        self.output_group_name = out_grp_name
        self.output_image_name = out_img_name
        
        self.auth = AzHelp.Auth()
        return

    
    def __call__(self, packerfile, var_map):
        pf_dir = os.path.dirname(os.path.abspath(packerfile))
        packer_vars = {
            "azure_client_id": self.auth.client_id,
            "azure_client_secret": self.auth.secret,
            "azure_tenant_id": self.auth.tenant_id,
            "azure_subscription_id": self.auth.subscription_id,
            
            "location": self.location,
            "vm_size": self.vm_size,
            "output_group_name": self.output_group_name,
            "output_image_name": self.output_image_name,
            
            "packerfile_dir": pf_dir
            }
        packer_vars.update(var_map)

        cmdline = ['packer', 'build', '-machine-readable']
        for k,v in packer_vars.iteritems():
            cmdline.append('-var')
            cmdline.append('{}={}'.format(k, v))
            
        cmdline.append(packerfile)

        self.info("Creating output resource group", self.output_group_name)
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.output_group_name, {'location': self.location}
            )
        
        self.info("calling packer to create image")
        self.debug(' '.join(cmdline))
        try:
            packer_out = subprocess.check_output(cmdline)
        except subprocess.CalledProcessError as e:
            self.critical("Error calling packer: code %d" % e.returncode)
            self.info(e.cmd)
            self.debug(e.output)
            raise
        self.debug(packer_out)
        
    pass

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create an Azure managed image based on Centos 7.1 HPC")

    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Increase the verbosity level - can be provided multiple times")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="Decrease the verbosity level")
    parser.add_argument("--location", "-l", default="westeurope",
                        help="Azure data centre to use")
    parser.add_argument("--resource-group", "-g", required=True,
                        help="Name of resource group to put the output image in (required)")
    parser.add_argument("--name", "-n", required=True,
                        help="Name of output image (required)")
    
    parser.add_argument("packerfile", help="path to the packer file")
    parser.add_argument("variables", nargs="*",
                        help="Zero or more variable settings (like KEY=VALUE)")
    
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1
    
    var_map = {}
    for v in args.variables:
        key, val = v.split('=')
        var_map[key] = val
    
    creator = ImageCreator(args.location, args.resource_group, args.name,
                           verbosity=verbosity)
    creator(args.packerfile, var_map)

    
