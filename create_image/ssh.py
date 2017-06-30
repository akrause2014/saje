import paramiko
class IgnorePolicy(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        return
    pass

def run(user, host, command):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(IgnorePolicy())
    try:
        client.connect(host, username=user)
        client.exec_command(command, get_pty=True)
    finally:
        client.close()
        
