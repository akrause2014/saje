import re
import hashlib

from .status import StatusReporter
from . import AzHelp

def IsValidContainerName(c_name):
    """Check the string matches Azure Blob storage container name rules:
    
    A container name must be a valid DNS name, conforming to the following naming rules:

    1. Container names must start with a letter or number, and can
    contain only letters, numbers, and the dash (-) character.

    2. Every dash (-) character must be immediately preceded and
    followed by a letter or number; consecutive dashes are not permitted
    in container names.
    
    3. All letters in a container name must be lowercase.

    4. Container names must be from 3 through 63 characters long.
    """
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

def JobContainerName(job_id):
    """Implement the Azure Batch conventions on job container names.
    
    https://github.com/Azure/azure-sdk-for-net/tree/vs17Dev/src/SDKs/Batch/Support/FileConventions#job-output-container-name
    """
    # Normalize the job ID to lower case
    norm_id = job_id.lower()
    # If prepending "job-" to the normalized ID gives a valid
    # container name, use that
    c_name = 'job-' + norm_id
    if IsValidContainerName(c_name):
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

class BatchHelper(StatusReporter):
    def __init__(self, group_name, batch_name, verbosity=1):
        self.verbosity = verbosity
        
        self.auth = AzHelp.Auth('polnet')
        
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
        storage_name = AzHelp.DemangleId(storage_id)['name']
        
        self.debug('Opening storage account', storage_name)
        self.storage = AzHelp.StorageAccount.open(self.auth, self.group, storage_name)
        
        self.debug('Creating batch client')
        self.client = self.auth.BatchServiceClient(base_url=batch_url)
    
        
