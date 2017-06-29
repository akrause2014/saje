import os.path
import ConfigParser
import json

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlockBlobService, PublicAccess

class Auth(object):
    _credentials = None
    @classmethod
    def GetCredentials(cls, wanted_key=None):
        if cls._credentials is None:
            cls._credentials = cls._ReadCredentials()
        
        if wanted_key is None:
            return cls._credentials
        else:
            return cls._credentials[wanted_key]

    @classmethod
    def _ReadCredentials(cls):
        names = ['subscription_id', 'client_id', 'secret_id', 'tenant_id']
        env_vars = ['AZURE_SUBSCRIPTION_ID', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID']
        ans = {}
        cred_file = os.path.expanduser('~/.azure/credentials')
        
        if os.path.exists(cred_file):
            # Read from credentials first
            cp = ConfigParser.ConfigParser()
            cp.read(cred_file)
            for key in names:
                ans[key] = cp.get('default', key)
                
        # Allow env to override
        for ev, key in zip(env_vars, names):
            val = os.environ.get(ev, None)
            if val is not None:
                ans[key] = val
            
        # check we've got them all
        for key in names:
            assert key in ans, "Missing value of '{}'".format(key)
        return ans
    
    def __init__(self):
        self.subscription_id = self.GetCredentials('subscription_id')
        self.credentials = ServicePrincipalCredentials(
            client_id=self.GetCredentials('client_id'),
            secret=self.GetCredentials('secret_id'),
            tenant=self.GetCredentials('tenant_id')
        )

class StorageAccount(object):
    """Minimal wrapper of a storage account"""

    def __init__(self, name, key):
        self.name = name
        self.key = key
        self._block_blob_service = None

    @property
    def BlockBlobService(self):
        if self._block_blob_service is None:
            self._block_blob_service = BlockBlobService(account_name=self.name, account_key=self.key)
        return self._block_blob_service

    def create_block_blob_container(self, name, public=None):
        print "Creating blob container " + name
        return BlobContainer(self.BlockBlobService, name, public)

class BlobContainer(object):
    """Minimal wrapper of a blob storage container"""
    
    def __init__(self, blob_service, name, public=None):
        self.blob_service = blob_service
        self.name = name
        blob_service.create_container(name, public_access=public)
        return
    
    def upload(self, blob_name, file_path):
        self.blob_service.create_blob_from_path(self.name, blob_name, file_path)
        return self.url(blob_name)
    
    def download(self, blob_name, file_path):
        self.blob_service.get_blob_to_path(self.name, blob_name, file_path)
    
    def delete(self, blob_name):
        self.blob_service.delete_blob(self.name, blob_name)

    def url(self, blob_name):
        return self.blob_service.make_blob_url(self.name, blob_name)
    
    def __iter__(self):
        return iter(self.blob_service.list_blobs(self.name))
    
    pass

class Deployer(object):
    """Deploy an ARM template.
    Initialize with credentials, subscription, location and group
    """

    def __init__(self, auth, location, rg_name):
        """This will create the resource group"""
        self.auth = auth
        self.client = ResourceManagementClient(auth.credentials, auth.subscription_id)
        self.rg = rg_name
        self.loc = location
        self.client.resource_groups.create_or_update(
            self.rg, {'location': self.loc}
            )
        return
    
    def __call__(self, template_path, pdict={}):
        """Deploy the template with parameters."""
        
        with open(template_path, 'r') as template_file_fd:
            template = json.load(template_file_fd)

        deployment_properties = {
            'mode': DeploymentMode.incremental,
            'template': template,
            'parameters': {k: {'value': v} for k, v in pdict.items()}
        }

        deployment_async_operation = self.client.deployments.create_or_update(
            self.rg,
            'hemelb-image-creation',
            deployment_properties
        )
        
        return deployment_async_operation.result()
    pass

class StorageAccountFactory(object):
    def __init__(self, deployer):
        self.deployer = deployer
        return
    def __call__(self, name, location, accountType, accessTier):
        
        params = {
            'location': location,
            'name': name,
            'accountType': accountType,
            'accessTier': accessTier
        }
        print "Creating storage account " + name
        self.deployer('storagetemplate.json', params)
        print "Done"
        print "Retrieving access keys"
        storage_client = StorageManagementClient(self.deployer.auth.credentials,
                                                 self.deployer.auth.subscription_id)
        key_list = storage_client.storage_accounts.list_keys(self.deployer.rg,
                                                             name)
        print "Done"
        return StorageAccount(name, key_list.keys[0].value)
