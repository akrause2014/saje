from __future__ import print_function, unicode_literals
import re
import hashlib

from azure.batch import models
from ..status import StatusReporter
from .auth import Auth
from .storage import StorageAccount, BlobService

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

def JobContainerName(job_id):
    '''Implement the Azure Batch conventions on job container names.
    
    https://github.com/Azure/azure-sdk-for-net/tree/vs17Dev/src/SDKs/Batch/Support/FileConventions#job-output-container-name
    '''
    # Normalize the job ID to lower case
    norm_id = job_id.lower()
    # If prepending "job-" to the normalized ID gives a valid
    # container name, use that
    c_name = 'job-' + norm_id
    if BlobService.IsValidContainerName(c_name):
        return c_name
    
    # Calculate the SHA1 hash of the normalized ID, and express it as a 40-character hex string.
    sha1 = hashlib.sha1(c_name).hexdigest()
    # Replace all sequences of one or more hyphens or underscores in
    # the normalized ID by single hyphens, then remove any leading or
    # trailing hyphens.
    c_name = re.sub('[-_]+', '-', norm_id).strip('-')
    # If the resulting string is empty, use the string "job" instead.
    c_name = 'job' if c_name == '' else c_name
    # If the resulting string is longer than 15 characters, truncate
    # it to 15 characters. If truncation results in a trailing hyphen,
    # remove it.
    c_name = c_name[:15].strip('-')
    # The container name is the string "job-", followed by the
    # truncated ID, followed by a hyphen, followed by the hash.
    return 'job-' + c_name +'-' + sha1

class Helper(StatusReporter):
    def __init__(self, group_name, batch_name, cred_name='batch', verbosity=1):
        self.verbosity = verbosity
        
        self.auth = Auth(cred_name)
        
        self.group = group_name
        self.name = batch_name

        self.debug('Getting batch account info')
        self.manager = self.auth.BatchManagementClient()
        self.account = self.manager.batch_account.get(self.group, self.name)
        
        batch_url = self.account.account_endpoint
        if not batch_url.startswith('https://'):
            batch_url = 'https://' + batch_url
        self.debug('Batch URL:', batch_url)
        self.url = batch_url

        storage_id = self.account.auto_storage.storage_account_id
        storage_name = DemangleId(storage_id)['name']
        
        self.debug('Opening storage account', storage_name)
        self.storage = StorageAccount.open(self.auth, self.group, storage_name)
        
        self.debug('Creating batch client')
        self.client = self.auth.BatchServiceClient(base_url=batch_url)
    
    
