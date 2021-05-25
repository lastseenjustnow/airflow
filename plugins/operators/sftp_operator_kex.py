import os

from airflow import AirflowException
from airflow.contrib.operators.sftp_operator import SFTPOperation, _make_intermediate_dirs

from hooks.ssh_hook_kex import SSHHookKex


def execute(self, context):
    file_msg = None
    try:
        if self.ssh_conn_id:
            if self.ssh_hook and isinstance(self.ssh_hook, SSHHookKex):
                self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
            else:
                self.log.info("ssh_hook is not provided or invalid. " +
                              "Trying ssh_conn_id to create SSHHookKex.")
                self.ssh_hook = SSHHookKex(ssh_conn_id=self.ssh_conn_id)

        if not self.ssh_hook:
            raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

        if self.remote_host is not None:
            self.log.info("remote_host is provided explicitly. " +
                          "It will replace the remote_host which was defined " +
                          "in ssh_hook or predefined in connection of ssh_conn_id.")
            self.ssh_hook.remote_host = self.remote_host

        with self.ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            if self.operation.lower() == SFTPOperation.GET:
                local_folder = os.path.dirname(self.local_filepath)
                if self.create_intermediate_dirs:
                    # Create Intermediate Directories if it doesn't exist
                    try:
                        os.makedirs(local_folder)
                    except OSError:
                        if not os.path.isdir(local_folder):
                            raise
                file_msg = "from {0} to {1}".format(self.remote_filepath,
                                                    self.local_filepath)
                self.log.info("Starting to transfer %s", file_msg)
                sftp_client.get(self.remote_filepath, self.local_filepath)
            else:
                remote_folder = os.path.dirname(self.remote_filepath)
                if self.create_intermediate_dirs:
                    _make_intermediate_dirs(
                        sftp_client=sftp_client,
                        remote_directory=remote_folder,
                    )
                file_msg = "from {0} to {1}".format(self.local_filepath,
                                                    self.remote_filepath)
                self.log.info("Starting to transfer file %s", file_msg)
                sftp_client.put(self.local_filepath,
                                self.remote_filepath,
                                confirm=self.confirm)

    except Exception as e:
        raise AirflowException("Error while transferring {0}, error: {1}"
                               .format(file_msg, str(e)))

    return self.local_filepath
