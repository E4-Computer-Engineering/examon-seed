import os
import sys
import time
import socket
import logging

import paramiko  


class Ssh_Util:
    "Class to connect to remote server" 
    
    def __init__(self, host, username, password=None, timeout=10, pkey=None, port=22):
        self.ssh_output = None
        self.ssh_error = None
        self.client = None
        self.host = host
        self.username = username
        self.password = password
        self.timeout = timeout
        self.pkey = pkey
        self.port = port
        #self.connect()
        self.logger = logging.getLogger('examon')
       
    def connect(self):
        "Login to the remote server"
        result_flag=False
        try:
            self.logger.debug("Establishing ssh connection")
            self.client = paramiko.SSHClient()
            self.client.load_system_host_keys()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if self.pkey:
                p_key = paramiko.RSAKey.from_private_key_file(self.pkey)
                self.client.connect(hostname=self.host, port=self.port, username=self.username,pkey=p_key,timeout=self.timeout, allow_agent=False, look_for_keys=False)
                self.logger.debug("Connected to the server: %s" % (self.host,))
            else:
                self.client.connect(hostname=self.host, port=self.port,username=self.username,password=self.password,timeout=self.timeout, allow_agent=False, look_for_keys=False)    
                self.logger.debug("Connected to the server: %s" % (self.host,))
        except paramiko.AuthenticationException:
            self.logger.error("Authentication failed, please verify your credentials")
            result_flag = False
        except paramiko.SSHException as sshException:
            self.logger.error("Could not establish SSH connection: %s" % (sshException,))
            result_flag = False
        except socket.timeout as e:
            self.logger.error("Connection timed out")
            result_flag = False
        except Exception,e:
            self.logger.exception("Uncaught exception in connect()!")
            result_flag = False
            self.client.close()
        else:
            result_flag = True
        
        return result_flag    
        
    def close(self):
        self.client.close()
  
    def exec_command(self,command):
        """Execute a command on the remote host.Return a tuple containing
        an integer status and a two strings, the first containing stdout
        and the second containing stderr from the command."""
        ssh_output = None
        result_flag = True
        ssh_error = None
        try:
            if self.client:
                self.logger.debug("Executing command --> {}".format(command))
                stdin, stdout, stderr = self.client.exec_command(command,timeout=self.timeout)
                ssh_output = stdout.read()
                ssh_error = stderr.read()
                if ssh_error:
                    self.logger.error("Problem occurred while running command:"+ command + " The error is " + ssh_error)
                    result_flag = False
                else:    
                    self.logger.debug("Command execution completed successfully: %s" % (command))
                self.client.close()
            else:
                self.logger.error("Could not establish SSH connection")
                result_flag = False   
        except socket.timeout as e:
            self.logger.debug("Command timed out!: %s" % (command))
            self.client.close()
            result_flag = False                
        except paramiko.SSHException:
            self.logger.debug("Failed to execute the command!: %s" % (command))
            self.client.close()
            result_flag = False    
                      
        return (result_flag, ssh_output, ssh_error)

    def upload_file(self,uploadlocalfilepath,uploadremotefilepath):
        "This method uploads the file to remote server"
        result_flag = True
        try:
            if self.client:
                ftp_client= self.client.open_sftp()
                ftp_client.put(uploadlocalfilepath,uploadremotefilepath)
                ftp_client.close() 
                self.client.close()
            else:
                self.logger.error("Could not establish SSH connection")
                result_flag = False  
        except Exception,e:
            self.logger.exception("Uncaught exception in upload_file()!")
            result_flag = False
            ftp_client.close()
            self.client.close()
        
        return result_flag

    def download_file(self,downloadremotefilepath,downloadlocalfilepath):
        "This method downloads the file from remote server"
        result_flag = True
        try:
            if self.client:
                ftp_client= self.client.open_sftp()
                ftp_client.get(downloadremotefilepath,downloadlocalfilepath)
                ftp_client.close()  
                self.client.close()
            else:
                self.logger.error("Could not establish SSH connection")
                result_flag = False  
        except Exception,e:
            self.logger.exception("Uncaught exception in download_file()!")
            result_flag = False
            ftp_client.close()
            self.client.close()
        
        return result_flag

if __name__=='__main__':
    pass
