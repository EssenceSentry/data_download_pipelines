import logging
import warnings
from ftplib import FTP, error_temp
from typing import Optional

import paramiko
from download_pipelines.helper_utils import write_temp_file
from download_pipelines.logging_utils import set_logger
from paramiko import SSHClient

logger = set_logger(__name__)


class SSHConnection:
    def __init__(self, host: str, username: str, key_filename: str):
        self.connection = None
        self.host = host
        self.username = username
        self.key_filename = key_filename
        logger.info(
            "SHH connection to %s as %s. Connection not set up yet, will connect when needed.",
            self.host, self.username)

    def connect(self) -> SSHClient:
        try:
            warnings.simplefilter("ignore")
            paramiko_logger = logging.getLogger("paramiko.transport")
            paramiko_logger.disabled = True
            ssh: SSHClient = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.host,
                        username=self.username,
                        key_filename=self.key_filename)
            self.connection = ssh
            logger.info("Connection to %s stablished.", self.host)
            return ssh
        except Exception:
            raise Exception("Could not connect to %s with SSH." % self.host)

    def __call__(self) -> SSHClient:
        return self.connect() if not self.connection else self.connection

    def download(self, filename: str):
        connection = self.connect() if not self.connection else self.connection
        stdout = connection.exec_command(f"cat {filename}")[1]
        return write_temp_file(filename, stdout.read())

    def contents(self, path: str = "/"):
        connection = self.connect() if not self.connection else self.connection
        content_list = list(
            connection.exec_command(f"ls {path}")[1].read().decode().split(
                "\n"))
        logger.info("Contents: \n\t%s", content_list)
        return content_list


class FTPConnection:
    def __init__(self, username: str, password: str, host: str):
        self.connection: Optional[FTP] = None
        self.host = host
        self.username = username
        self.password = password
        logger.info(
            "FTP connection to %s as %s. Connection not set up yet, will connect when needed.",
            self.host, self.username)

    def connect(self) -> FTP:
        try:
            self.connection = FTP(host=self.host,
                                  user=self.username,
                                  passwd=self.password)
            logger.info("Connection to %s stablished.", self.host)
            return self.connection
        except Exception:
            raise Exception("Could not connect to FTP server %s." % self.host)

    def __call__(self) -> FTP:
        return self.connect() if not self.connection else self.connection

    def download(self, filename: str):
        connection = self.connect() if not self.connection else self.connection
        temp_filename = write_temp_file(filename, "")
        try:
            with open(temp_filename, r"bw") as temp:
                connection.retrbinary(f"RETR {filename}", temp.write)
        except (error_temp, EOFError):
            self.connect()
            self.download(filename)
        return temp_filename

    def contents(self, path: str = "/"):
        connection = self.connect() if not self.connection else self.connection
        content_list = connection.nlst(path)
        logger.info("Contents: \n\t%s", content_list)
        return content_list
