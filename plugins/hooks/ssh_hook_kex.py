# -*- coding: utf-8 -*-
#
# This code was added to avoid paramiko mistake when it employs
# incorrect algorithm to negotiate to a remote server
#

from airflow.contrib.hooks.ssh_hook import SSHHook
import paramiko


class SSHHookKex(SSHHook):
    def get_conn(self):
        """
        Opens a ssh connection to the remote host.

        :rtype: paramiko.client.SSHClient
        """

        self.log.debug('Creating SSH client for conn_id: %s', self.ssh_conn_id)
        client = paramiko.SSHClient()

        if not self.allow_host_key_change:
            self.log.warning('Remote Identification Change is not verified. '
                             'This wont protect against Man-In-The-Middle attacks')
            client.load_system_host_keys()
        if self.no_host_key_check:
            self.log.warning('No Host Key Verification. This wont protect '
                             'against Man-In-The-Middle attacks')
            # Default is RejectPolicy
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = dict(
            hostname=self.remote_host,
            username=self.username,
            timeout=self.timeout,
            compress=self.compress,
            port=self.port,
            sock=self.host_proxy
        )

        if self.password:
            password = self.password.strip()
            connect_kwargs.update(password=password)

        if self.pkey:
            connect_kwargs.update(pkey=self.pkey)

        if self.key_file:
            connect_kwargs.update(key_filename=self.key_file)

        connect_kwargs.update(disabled_algorithms={"kex": ["diffie-hellman-group16-sha512"]})

        client.connect(**connect_kwargs)

        if self.keepalive_interval:
            client.get_transport().set_keepalive(self.keepalive_interval)

        self.client = client
        return client
