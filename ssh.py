import paramiko
from status import StatusReporter

class IgnorePolicy(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        return
    pass

class CmdRunner(StatusReporter):
    def __init__(self, verbosity=1):
        self.verbosity = verbosity
        self.key = paramiko.RSAKey.generate(2048)
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(IgnorePolicy())
        
    @property
    def pubkey(self):
        return "%s %s comment\n" % (self.key.get_name(), self.key.get_base64())
    
    def run(self, user, host, command):
        try:
            self.debug("ssh {user}@{host} {command}".format(**locals()))
            self.client.connect(host, username=user, pkey=self.key)
            stdin, stdout, stderr = self.client.exec_command(command, get_pty=True)
            stdin.close()
            self.debug("STDOUT:")
            self.debug(stdout.read())
            self.debug("STDERR:")
            self.debug(stderr.read())
        finally:
            self.client.close()
        
        
