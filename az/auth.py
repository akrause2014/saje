import os.path
import json
import adal
from msrestazure.azure_active_directory import AdalAuthentication
from ..common.cacher import cache

class Auth(object):
    '''Wraps the AAD authentication of Azure users and is a factory for
    management and service clients.

    Credentials need to be supplied in a JSON config file. Format is:
{
    "credentials": [
        {
            "name": "the name of this set of credentials",
            "subscription_id": "your Azure subscription ID",
            "tenant_id": "your Azure AD tenant ID",
            "secret": "your secret key"
        }
    ]
}

    Default location is ~/.azure/saje.json but you can override by
    setting SAJE_AUTH_CONFIG in your environment or supply it to
    the constructor.
    '''

    default_config = os.path.expanduser('~/.azure/saje.json')
    login_endpoint = 'https://login.microsoftonline.com/'

    def __init__(self, name='default', config_path=None):
        if config_path is None:
            config_path = os.environ.get('SAJE_AUTH_CONFIG', self.default_config)

        if os.path.exists(config_path):
            with open(config_path) as cf:
                config = json.load(cf)

            all_creds = {c['name']: c for c in config['credentials']}
            cred = all_creds[name]

            self.tenant_id = str(cred['tenant_id'])
            self.subscription_id = str(cred['subscription_id'])
            self.client_id = str(cred['client_id'])
            self.secret = str(cred['secret'])
        else:
            self.tenant_id = os.environ.get('SAJE_TENANT_ID')
            self.subscription_id = os.environ.get('SAJE_SUBSCRIPTION_ID')
            self.client_id = os.environ.get('SAJE_CLIENT_ID')
            self.secret = os.environ.get('SAJE_SECRET')
            if self.tenant_id is None or self.subscription_id is None \
                or self.client_id is None or self.secret is None:
                    raise Exception("No Azure auth config found")

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
        from azure.mgmt.resource import ResourceManagementClient
        return ResourceManagementClient(self.ManagementCredentials, self.subscription_id)

    @cache
    def StorageManagementClient(self):
        from azure.mgmt.storage import StorageManagementClient
        return StorageManagementClient(self.ManagementCredentials, self.subscription_id)

    @cache
    def ComputeManagementClient(self):
        from azure.mgmt.compute import ComputeManagementClient
        return ComputeManagementClient(self.ManagementCredentials, self.subscription_id)

    @cache
    def BatchManagementClient(self):
        from azure.mgmt.batch import BatchManagementClient
        return BatchManagementClient(self.ManagementCredentials, self.subscription_id)
    def BatchServiceClient(self, base_url=None):
        from azure.batch import BatchServiceClient
        return BatchServiceClient(self.GetCredentialsForResource('https://batch.core.windows.net/'), base_url=base_url)

    @cache
    def NetworkManagementClient(self):
        from azure.mgmt.network import NetworkManagementClient
        return NetworkManagementClient(self.ManagementCredentials, self.subscription_id)

    @cache
    def KeyVaultManagementClient(self):
        from azure.mgmt.keyvault import KeyVaultManagementClient
        return KeyVaultManagementClient(self.ManagementCredentials, self.subscription_id)

    @cache
    def AuthorizationManagementClient(self):
        from azure.mgmt.authorization import AuthorizationManagementClient
        return AuthorizationManagementClient(self.ManagementCredentials, self.subscription_id)
    @cache
    def GraphRbacManagementClient(self):
        from azure.graphrbac import GraphRbacManagementClient
        return GraphRbacManagementClient(self.GetCredentialsForResource('https://graph.windows.net/'), self.tenant_id)
    pass
