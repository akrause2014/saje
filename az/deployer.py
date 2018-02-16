import json
import time
from azure.mgmt.resource.resources.models import DeploymentMode
from ..status import StatusReporter


class Deployer(StatusReporter):
    """Deploy an ARM template.
    Initialize with credentials, location and group
    """

    @staticmethod
    def now_str():
        'Get the current time as a compact string'
        return time.strftime('%Y%m%d%H%M%S')
    
    def __init__(self, auth, rg_name, verbosity=1):
        self.verbosity = verbosity
        self.auth = auth
        self.client = auth.ResourceManagementClient()
        self.rg = rg_name
        return
    
    def __call__(self, template_path, pdict={}, name=None):
        """Deploy the template with parameters."""
        if name is None:
            name = self.now_str()
        
        with open(template_path, 'r') as template_file_fd:
            template = json.load(template_file_fd)

        deployment_properties = {
            'mode': DeploymentMode.incremental,
            'template': template,
            'parameters': {k: {'value': v} for k, v in pdict.items()}
        }

        deployment_async_operation = self.client.deployments.create_or_update(
            self.rg,
            name,
            deployment_properties
        )
        
        return deployment_async_operation.result()
    pass

