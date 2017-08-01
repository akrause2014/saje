import os.path
import json
import datetime
import time
import operator

import adal
from msrestazure.azure_active_directory import AdalAuthentication

from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode

from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.keyvault import KeyVaultManagementClient
from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku, Kind
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.graphrbac import GraphRbacManagementClient

from azure import batch
from azure.storage import blob


from .status import StatusReporter

def cache(getter):
    name = '_' + getter.__name__
    def wrapper(self):
        try:
            return getattr(self, name)
        except AttributeError:
            ans = getter(self)
            setattr(self, name, ans)
            return ans
    return wrapper

class Auth(object):
    default_config = os.path.expanduser('~/.azure/polnet.json')
    login_endpoint = 'https://login.microsoftonline.com/'
    
    def __init__(self, name='default', config_path=None):
        if config_path is None:
            config_path = self.default_config

        with open(config_path) as cf:
            config = json.load(cf)
            
        all_creds = {c['name']: c for c in config['credentials']}
        cred = all_creds[name]

        self.tenant_id = str(cred['tenant_id'])
        self.subscription_id = str(cred['subscription_id'])
        self.client_id = str(cred['client_id'])
        self.secret = str(cred['secret'])

        self.context = adal.AuthenticationContext(self.login_endpoint + self.tenant_id)
        self._resource_credentials = {}
    
    def GetCredentialsForResource(self, resource):
        try:
            return self._resource_credentials[resource]
        except KeyError:
            ans = AdalAuthentication(self.context.acquire_token_with_client_credentials,
                                     resource, self.client_id, self.secret)
            self._resource_credentials[resource] = ans
            return ans        
    @property
    def ManagementCredentials(self):
        return self.GetCredentialsForResource('https://management.azure.com/')

    @cache
    def ResourceManagementClient(self):
        return ResourceManagementClient(self.ManagementCredentials, self.subscription_id)
    
    @cache
    def StorageManagementClient(self):
        return StorageManagementClient(self.ManagementCredentials, self.subscription_id)
    
    @cache
    def ComputeManagementClient(self):
        return ComputeManagementClient(self.ManagementCredentials, self.subscription_id)
    
    @cache
    def BatchManagementClient(self):
        return BatchManagementClient(self.ManagementCredentials, self.subscription_id)
    def BatchServiceClient(self, base_url=None):
        return batch.BatchServiceClient(self.GetCredentialsForResource('https://batch.core.windows.net/'), base_url=base_url)
   
    @cache
    def KeyVaultManagementClient(self):
        return KeyVaultManagementClient(self.ManagementCredentials, self.subscription_id)

    @cache
    def AuthorizationManagementClient(self):
        return AuthorizationManagementClient(self.ManagementCredentials, self.subscription_id)
    @cache
    def GraphRbacManagementClient(self):
        return GraphRbacManagementClient(self.GetCredentialsForResource('https://graph.windows.net/'), self.tenant_id)
    pass

def DemangleId(az_id):
    """Unpack an Azure ID string, at least partially.
    """
    parts = az_id.split('/')
    
    p = parts.pop(0)
    assert p == ''

    ans = {}
    
    p = parts.pop(0)
    assert p == 'subscriptions'
    ans['subscription'] = parts.pop(0)

    p = parts.pop(0)
    assert p == 'resourceGroups'
    ans['resourceGroup'] = parts.pop(0)
    
    if len(parts):
        p = parts.pop(0)
        assert p == 'providers'
        ans['provider'] = parts.pop(0)
        ans['resource'] = parts.pop(0)
        ans['name'] = parts.pop(0)

        if len(parts):
            ans['subparts'] = parts
    return ans


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

    def __getattr__(self, name):
        try:
            return getattr(self.blob_service, name)
        except AttributeError:
            raise AttributeError("Neither AzHelp.BlobService nor its delegate azure.storage.blob.BaseBlobService have the attribute ''{}".format(name) )
        
    def get_container(self, name):
        assert self.exists(name)
        return BlobContainer(self.blob_service, name)
    
    def create_container(self, name, public=None, fail_on_exist=False):
        self.blob_service.create_container(name, public_access=public, fail_on_exist=fail_on_exist)
        return BlobContainer(self.blob_service, name)
    def delete_container(self, name):
        return self.blob_service.delete_container(name)
    
    def list_containers(self, prefix=None):
        for raw_c in self.blob_service.list_containers(prefix=prefix):
            yield BlobContainer(self.blob_service, raw_c.name)
    
    def exists(self, container, blob=None):
        return self.blob_service.exists(container, blob)
    
    pass

class BlobContainer(object):
    """Minimal wrapper of a blob storage container"""
    
    def __init__(self, blob_service, name):
        self.blob_service = blob_service
        self.name = name
        return
    
    def upload(self, file_path, blob_name=None):
        if blob_name is None:
            blob_name = os.path.basename(file_path)
            
        self.blob_service.create_blob_from_path(self.name, blob_name, file_path)
        return self.url(blob_name)
    
    def download(self, blob_name, file_path):
        self.blob_service.get_blob_to_path(self.name, blob_name, file_path)

    def from_str(self, blob_name, text):
        self.blob_service.create_blob_from_text(self.name, blob_name, text)
        return self.url(blob_name)

    def to_str(self, blob_name):
        blb = self.blob_service.get_blob_to_text(self.name, blob_name)
        return blb.content
    
    def delete(self, blob_name):
        self.blob_service.delete_blob(self.name, blob_name)

    def copy(self, blob_name, src_url):
        copy_prop = self.blob_service.copy_blob(self.name, blob_name, src_url)
        while copy_prop.status == 'pending':
            time.sleep(1)
            copy_prop = self.blob_service.get_blob_properties(self.name, blob_name).properties.copy
            
        return self.url(blob_name)
    
    def exists(self, blob_name):
        return self.blob_service.exists(self.name, blob_name)
    
    def list(self, prefix=None):
        for b in self.blob_service.list_blobs(self.name, prefix=prefix):
            yield b.name
    
    def url(self, blob_name, sas_token=None):
        return self.blob_service.make_blob_url(self.name, blob_name, sas_token=sas_token)

    def generate_sas(self, permissions, hours=24):
        return self.blob_service.generate_container_shared_access_signature(
            self.name,
            permission=permissions,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=hours)
            )
    
    def __iter__(self):
        return iter(self.blob_service.list_blobs(self.name))
    
    pass

class Deployer(StatusReporter):
    """Deploy an ARM template.
    Initialize with credentials, location and group
    """

    def __init__(self, auth, rg_name, verbosity=1):
        self.verbosity = verbosity
        self.auth = auth
        self.client = auth.ResourceManagementClient()
        self.rg = rg_name
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

