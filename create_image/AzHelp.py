import os.path
import ConfigParser
import json

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode

from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku, Kind
from azure.storage.blob import BlockBlobService, PublicAccess

from azure.mgmt.compute import ComputeManagementClient

def cache_client(getter):
    name = getter.func_name
    
    def ans(self):
        try:
            return self._clients[name]
        except KeyError:
            ans = getter(self)
            self._clients[name] = ans
            return ans
    return ans

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
        self._clients = {}

    @cache_client
    def ResourceManagementClient(self):
        return ResourceManagementClient(self.credentials, self.subscription_id)
    @cache_client
    def StorageManagementClient(self):
        return StorageManagementClient(self.credentials, self.subscription_id)
    @cache_client
    def ComputeManagementClient(self):
        return ComputeManagementClient(self.credentials, self.subscription_id)
    pass

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
        self.client = auth.ResourceManagementClient()
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

class BlobStorageAccountFactory(object):
    def __init__(self, auth):
        self.client = auth.StorageManagementClient()
        
        return
    def __call__(self, location, group_name, account_name, account_type, access_tier):
        # sku, kind, location, tags=None, custom_domain=None, encryption=None, access_tier=None
        params = StorageAccountCreateParameters(Sku(account_type),
                                                 Kind.blob_storage,
                                                 location,
                                                 access_tier=access_tier)
        
        request = self.client.storage_accounts.create(group_name, account_name, params)
        request.wait()
        key_list = self.client.storage_accounts.list_keys(group_name,
                                                          account_name)
        return StorageAccount(account_name, key_list.keys[0].value)
    pass
