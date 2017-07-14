import os.path
import ConfigParser
import json
import datetime
import time
import operator

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode

from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.keyvault import KeyVaultManagementClient

from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku, Kind
from azure.storage import blob


from .status import StatusReporter

def cache(getter):
    name = '_' + getter.func_name
    def wrapper(self):
        try:
            return getattr(self, name)
        except AttributeError:
            ans = getter(self)
            setattr(self, name, ans)
            return ans
    return wrapper

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

    @cache
    def ResourceManagementClient(self):
        return ResourceManagementClient(self.credentials, self.subscription_id)
    @cache
    def StorageManagementClient(self):
        return StorageManagementClient(self.credentials, self.subscription_id)
    @cache
    def ComputeManagementClient(self):
        return ComputeManagementClient(self.credentials, self.subscription_id)
    @cache
    def BatchManagementClient(self):
        return BatchManagementClient(self.credentials, self.subscription_id)
    @cache
    def KeyVaultManagementClient(self):
        return KeyVaultManagementClient(self.credentials, self.subscription_id)

    pass


class StorageAccount(object):
    """Minimal wrapper of a storage account"""
    @classmethod
    def create(cls, auth, location, group_name, account_name, account_kind, account_type, access_tier=None):
        client = auth.StorageManagementClient()
        kwargs = {}
        if account_kind == 'BlobStorage':
            kwargs['access_tier'] = access_tier
        params = StorageAccountCreateParameters(Sku(account_type),
                                                account_kind,
                                                location,
                                                **kwargs)
        
        request = client.storage_accounts.create(group_name, account_name, params)
        acc = request.result()
        key_list = client.storage_accounts.list_keys(group_name,
                                                          account_name)
        return cls(acc, key_list.keys[0].value)

    @classmethod
    def open(cls, auth, group_name, account_name):
        client = auth.StorageManagementClient()
        acc = client.storage_accounts.get_properties(group_name, account_name)
        key_list = client.storage_accounts.list_keys(group_name, account_name)
        
        return cls(acc, key_list.keys[0].value)
    
    def __init__(self, acc, key):
        self.acc = acc
        self.key = key
        
    def __getattr__(self, name):
        try:
            return getattr(self.acc, name)
        except AttributeError:
            raise AttributeError("Neither AzHelp.StorageAccount nor its delegate azure.mgmt.storage.StorageAccount have the attribute ''{}".format(name) )
    @property
    @cache
    def BlockBlobService(self):
        return BlobService(blob.BlockBlobService(account_name=self.acc.name, account_key=self.key))
    @property
    @cache
    def PageBlobService(self):
        return BlobService(blob.PageBlobService(account_name=self.acc.name, account_key=self.key))

class BlobService(object):
    def __init__(self, az_blob_service):
        self.blob_service = az_blob_service
        
    def get_container(self, name):
        assert self.exists(name)
        return BlobContainer(self.blob_service, name)
    
    def create_container(self, name, public=None, fail_on_exist=True):
        self.blob_service.create_container(name, public_access=public, fail_on_exist=fail_on_exist)
        return BlobContainer(self.blob_service, name)
    def delete_container(self, name):
        return self.blob_service.delete_container(name)
    def list_containers(self, prefix=None):
        return self.blob_service.list_containers(self, prefix=prefix)

    def exists(self, container, blob=None):
        return self.blob_service.exists(container, blob)
    
    pass

class BlobContainer(object):
    """Minimal wrapper of a blob storage container"""
    
    def __init__(self, blob_service, name, public=None):
        self.blob_service = blob_service
        self.name = name
        return
    
    def upload(self, blob_name, file_path):
        self.blob_service.create_blob_from_path(self.name, blob_name, file_path)
        return self.url(blob_name)
    
    def download(self, blob_name, file_path):
        self.blob_service.get_blob_to_path(self.name, blob_name, file_path)
    
    def delete(self, blob_name):
        self.blob_service.delete_blob(self.name, blob_name)

    def copy(self, blob_name, src_url):
        copy_prop = self.blob_service.copy_blob(self.name, blob_name, src_url)
        while copy_prop.status == 'pending':
            time.sleep(1)
            copy_prop = self.blob_service.get_blob_properties(self.name, blob_name).properties.copy
            
        return self.url(blob_name)
    
    def exists(self, blob):
        return self.blob_service.exists(self.name, blob)
    
    def url(self, blob_name):
        return self.blob_service.make_blob_url(self.name, blob_name)

    def generate_sas(self, permissions):
        return self.blob_service.generate_container_shared_access_signature(
            self.name,
            permission=permissions,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=24)
            )
        
    def __iter__(self):
        return iter(self.blob_service.list_blobs(self.name))
    
    pass

class Deployer(StatusReporter):
    """Deploy an ARM template.
    Initialize with credentials, location and group
    """

    def __init__(self, auth, location, rg_name, verbosity=1):
        self.verbosity = verbosity
        self.auth = auth
        self.client = auth.ResourceManagementClient()
        self.rg = rg_name
        self.loc = location
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

