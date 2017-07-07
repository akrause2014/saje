import paramiko

class IgnorePolicy(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        return
    pass

class CmdRunner(object):
    def __init__(self):
        self.key = paramiko.RSAKey.generate(2048)
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(IgnorePolicy())
        
    @property
    def pubkey(self):
        return "%s %s comment\n" % (self.key.get_name(), self.key.get_base64())
    
    def run(self, user, host, command):
        try:
            self.client.connect(host, username=user, pkey=self.key)
            self.client.exec_command(command, get_pty=True)
        finally:
            self.client.close()
        
        
