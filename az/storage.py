import time, datetime
import os.path

from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku, Kind
from azure.storage import blob

from msrestazure.azure_exceptions import CloudError
from ..common.cacher import cache

class StorageAccount(object):
    '''Minimal wrapper of an Azure storage account'''
    
    @staticmethod
    def exists(auth, group_name, account_name):
        '''Query existence of the storage account.'''
        client = auth.StorageManagementClient()
        try:
            acc = client.storage_accounts.get_properties(group_name, account_name)
            return True
        except CloudError:
            return False
        
    @classmethod
    def create(cls, auth, location, group_name, account_name, account_kind, account_type, access_tier=None):
        '''Factory method: create an instance when the account does not exist.
        '''
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
        '''Factory method: create an instance when the account does exist.
        '''
        client = auth.StorageManagementClient()
        acc = client.storage_accounts.get_properties(group_name, account_name)
        key_list = client.storage_accounts.list_keys(group_name, account_name)
        
        return cls(acc, key_list.keys[0].value)
    
    def __init__(self, acc, key):
        '''Internal constructor'''
        self.acc = acc
        self.key = key
        
    def __getattr__(self, name):
        try:
            return getattr(self.acc, name)
        except AttributeError:
            raise AttributeError('Neither az.storage.StorageAccount nor its delegate azure.mgmt.storage.StorageAccount have the attribute "{}"'.format(name))
        
    @property
    @cache
    def block_blob_service(self):
        return BlobService(blob.BlockBlobService(account_name=self.acc.name, account_key=self.key))
    @property
    @cache
    def page_blob_service(self):
        return BlobService(blob.PageBlobService(account_name=self.acc.name, account_key=self.key))

class BlobService(object):
    @staticmethod
    def IsValidContainerName(c_name):
        '''Check the string matches Azure Blob storage container name rules:
        
        A container name must be a valid DNS name, conforming to the following naming rules:
        
        1. Container names must start with a letter or number, and can
        contain only letters, numbers, and the dash (-) character.

        2. Every dash (-) character must be immediately preceded and
        followed by a letter or number; consecutive dashes are not permitted
        in container names.
    
        3. All letters in a container name must be lowercase.

        4. Container names must be from 3 through 63 characters long.
        '''
        # rule 1 and start/finish dashes from 2
        if not re.match('^[a-z0-9][a-z0-9-]*[a-z0-9]$', c_name):
            return False
        
        # rule 2 double dash
        if re.search('--', c_name):
            return False
        
        # rule 3
        if c_name.isupper():
            return False
        
        # rule 4
        if len(c_name) < 3 or len(c_name) > 63:
            return False
        
        return True
    
    def __init__(self, az_blob_service):
        self.blob_service = az_blob_service

    def __getattr__(self, name):
        try:
            return getattr(self.blob_service, name)
        except AttributeError:
            raise AttributeError('Neither AzHelp.BlobService nor its delegate azure.storage.blob.BaseBlobService have the attribute "{}"'.format(name) )
        
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
    '''Minimal wrapper of a blob storage container'''
    
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
