import os.path
from haikunator import Haikunator
from azure.mgmt.compute import models as compute_models

from ..az.auth import Auth
from ..az.storage import StorageAccount, blob
from ..az.deployer import Deployer
from ..status import StatusReporter
from .. import resources
from .. import ssh

class Converter(StatusReporter):
    def __init__(self, loc, out_grp, out_name, wrk_grp=None, wrk_acc=None, wrk_vm=None, keep=False, vrb=1):
        
        self.auth = Auth()

        self.admin_user_name = os.getlogin()
        
        self.ssh = ssh.CmdRunner(verbosity=vrb)
                
        self.namer = Haikunator()
        
        self.location = loc
        self.output_group_name = out_grp
        self.output_image_name = out_name
        
        if wrk_grp is None:
            wrk_grp = self.namer.haikunate()
        self.working_group_name = wrk_grp

        if wrk_acc is None:
            wrk_acc = self.namer.haikunate(delimiter='')
        self.working_storage_account_name = wrk_acc

        if wrk_vm is None:
            wrk_vm = self.namer.haikunate()
        self.vm_name = wrk_vm
        
        self.verbosity = vrb
        self.keep = keep
        pass
    
    def __call__(self, input_vhd_url):
        self._PrepStorage(input_vhd_url)
        self._StartVhdVm()
        self._ConvertVm()
        self._CaptureVm()
        if not self.keep:
            self._CleanUp()
        return
    
        
    def _PrepStorage(self, input_vhd_url):
        self.info("Creating working resource group (if needed):", self.working_group_name)
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.working_group_name, {'location': self.location}
            )
        
        self.info("Creating output resource group (if needed):", self.output_group_name)
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.create_or_update(
            self.output_group_name, {'location': self.location}
            )

        if StorageAccount.exists(
                self.auth,
                self.working_group_name, self.working_storage_account_name):
            self.info("Opening working storage account:",
                          self.working_storage_account_name)
            acc = StorageAccount.open(
                self.auth,
                self.working_group_name,
                self.working_storage_account_name)
        else:
            self.info("Creating working storage account:",
                          self.working_storage_account_name)
            acc = StorageAccount.create(
                self.auth,
                self.location, self.working_group_name,
                self.working_storage_account_name,
                'Storage', 'Standard_LRS')
        
        container = acc.block_blob_service.create_container(
            'rawimg',
            fail_on_exist=False,
            public=blob.PublicAccess.Container
            )

        if not container.exists('source.vhd'):
            self.info("Copying VHD")
            self.src_vhd_url = container.copy('source.vhd', input_vhd_url)
        else:
            self.info("Using existing copy of VHD")
            self.src_vhd_url = container.url('source.vhd')
            pass
        
        if container.exists('run.vhd'):
            self.info("Deleting existing run VHD")
            container.delete('run.vhd')
        
        self.run_vhd_url = container.url('run.vhd')

    def _StartVhdVm(self):        
        self.info("Deploying VM:", self.vm_name)
        self.deployer = Deployer(self.auth, self.working_group_name)
        
        
        params = {
            'virtualMachineSize': 'Standard_H16',
            'virtualMachineName': self.vm_name,
            'adminUsername': self.admin_user_name,
            'adminPublicKey': self.ssh.pubkey,
            'sourceVhd': self.src_vhd_url,
            'runVhd': self.run_vhd_url
            }
        template_path = resources.get('vhd2img', 'vhd_vm.json')
        dep_res = self.deployer(template_path, params)
        outputs = dep_res.properties.outputs
        
        return
    
        
    def _ConvertVm(self):
        vm = self.auth.ComputeManagementClient().virtual_machines
        self.info("Deallocating VM")
        vm.deallocate(self.working_group_name, self.vm_name).wait()
        self.info("Converting VM to use managed disks")
        vm.convert_to_managed_disks(self.working_group_name, self.vm_name).wait()

        return
        

    def _CaptureVm(self):
        self.info("Getting VM IP")
        comp_client = self.auth.ComputeManagementClient()
        vm_ops = comp_client.virtual_machines
        net_client = self.auth.NetworkManagementClient()
        
        vm_info = vm_ops.get(self.working_group_name, self.vm_name)
        
        nic_id = vm_info.network_profile.network_interfaces[0].id
        nic_name = nic_id.split('/')[-1]
        nic_info = net_client.network_interfaces.get(self.working_group_name, nic_name)
        ip_id = nic_info.ip_configurations[0].public_ip_address.id
        ip_name = ip_id.split('/')[-1]
        ip_info = net_client.public_ip_addresses.get(self.working_group_name, ip_name)

        self.debug("IP address is ", ip_info.ip_address)
                
        self.info("Deprovisioning over ssh")
        self.ssh.run(self.admin_user_name, ip_info.ip_address,
                'sudo /usr/sbin/waagent -deprovision+user -force') 
        
        self.info("Deallocate and generalise the VM")
        vm_ops.deallocate(self.working_group_name, self.vm_name).wait()
        vm_ops.generalize(self.working_group_name, self.vm_name)
        # no need to wait

        self.info("Creating image from VM")
        img = compute_models.Image(
            self.location,
            source_virtual_machine=compute_models.SubResource(vm_info.id))
        comp_client.images.create_or_update(
            self.output_group_name,
            self.output_image_name,
            img).wait()
        return
    
    def _CleanUp(self):
        self.info("Deleting work resource group", self.working_group_name)
        res_client = self.auth.ResourceManagementClient()
        res_client.resource_groups.delete(self.working_group_name)
        
    pass

        
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert a VHD machine image into an Azure managed image")

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
    parser.add_argument("--work-group", default=None,
                        help="Name of resource group to use for temporary resources (random if omitted)")
    parser.add_argument("--work-storage", default=None,
                        help="Name of storage account temporary data (random if omitted)")
    parser.add_argument("--work-vm", default=None,
                        help="Name of the VM used for conversion (random if omitted)")
    parser.add_argument("--keep", "-k", action="store_true",
                        help="Keep the temporary resources")

    parser.add_argument("source_url",
                        help="The URL of the VHD to convert")
    
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet + 1
    
    conv = Converter(args.location, args.resource_group, args.name, args.work_group, args.work_storage, args.work_vm, keep=args.keep, vrb=verbosity)
    
    conv(args.source_url)
    
