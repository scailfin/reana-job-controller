# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2019 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Kubernetes Job Manager."""

import ast
import logging
import os
import traceback
import uuid

from flask import current_app
from kubernetes.client.models.v1_delete_options import V1DeleteOptions
from kubernetes.client.rest import ApiException
from reana_commons.config import CVMFS_REPOSITORIES, K8S_DEFAULT_NAMESPACE
from reana_commons.k8s.api_client import current_k8s_batchv1_api_client
from reana_commons.k8s.secrets import REANAUserSecretsStore
from reana_commons.k8s.volumes import get_k8s_cvmfs_volume, get_shared_volume
from reana_db.models import User

from reana_job_controller.errors import ComputingBackendSubmissionError
from reana_job_controller.job_manager import JobManager


class KubernetesJobManager(JobManager):
    """Kubernetes job management."""

    def __init__(self, docker_img=None, cmd=None, env_vars=None, job_id=None,
                 workflow_uuid=None, workflow_workspace=None,
                 cvmfs_mounts='false', shared_file_system=False):
        """Instanciate kubernetes job manager.

        :param docker_img: Docker image.
        :type docker_img: str
        :param cmd: Command to execute.
        :type cmd: list
        :param env_vars: Environment variables.
        :type env_vars: dict
        :param job_id: Unique job id.
        :type job_id: str
        :param workflow_id: Unique workflow id.
        :type workflow_id: str
        :param workflow_workspace: Workflow workspace path.
        :type workflow_workspace: str
        :param cvmfs_mounts: list of CVMFS mounts as a string.
        :type cvmfs_mounts: str
        :param shared_file_system: if shared file system is available.
        :type shared_file_system: bool
        """
        super(KubernetesJobManager, self).__init__(
                         docker_img=docker_img, cmd=cmd,
                         env_vars=env_vars, job_id=job_id,
                         workflow_uuid=workflow_uuid)
        self.backend = "Kubernetes"
        self.workflow_workspace = workflow_workspace
        self.cvmfs_mounts = cvmfs_mounts
        self.shared_file_system = shared_file_system

    @JobManager.execution_hook
    def execute(self):
        """Execute a job in Kubernetes."""
        backend_job_id = str(uuid.uuid4())
        job = {
            'kind': 'Job',
            'apiVersion': 'batch/v1',
            'metadata': {
                'name': backend_job_id,
                'namespace': K8S_DEFAULT_NAMESPACE
            },
            'spec': {
                'backoffLimit': current_app.config['MAX_JOB_RESTARTS'],
                'autoSelector': True,
                'template': {
                    'metadata': {
                        'name': backend_job_id
                    },
                    'spec': {
                        'containers': [
                            {
                                'image': self.docker_img,
                                'command': self.cmd,
                                'name': backend_job_id,
                                'env': [],
                                'volumeMounts': []
                            },
                        ],
                        'volumes': [],
                        'restartPolicy': 'Never'
                    }
                }
            }
        }
        user_id = os.getenv('REANA_USER_ID')
        secrets_store = REANAUserSecretsStore(user_id)
        user_secrets = secrets_store.get_secrets()

        for secret in user_secrets:
            name = secret['name']
            if secret['type'] == 'env':
                job['spec']['template']['spec']['containers'][0]['env'].append(
                    {
                        'name': name,
                        'valueFrom': {
                            'secretKeyRef': {
                                'name': user_id,
                                'key': name
                            }
                        }
                    })

        job['spec']['template']['spec']['volumes'].append(
            {
                'name': user_id,
                'secret': {
                    'secretName': user_id
                }
            }
        )
        job['spec']['template']['spec']['containers'][0][
            'volumeMounts'] \
            .append(
            {
                'name': user_id,
                'mountPath': "/etc/reana/secrets",
                'readOnly': True
            })

        if self.env_vars:
            for var, value in self.env_vars.items():
                job['spec']['template']['spec']['containers'][0]['env'].append(
                    {'name': var, 'value': value}
                )

        if self.shared_file_system:
            self.add_shared_volume(job)

        if self.cvmfs_mounts != 'false':
            cvmfs_map = {}
            for cvmfs_mount_path in ast.literal_eval(self.cvmfs_mounts):
                if cvmfs_mount_path in CVMFS_REPOSITORIES:
                    cvmfs_map[
                        CVMFS_REPOSITORIES[cvmfs_mount_path]] = \
                            cvmfs_mount_path

            for repository, mount_path in cvmfs_map.items():
                volume = get_k8s_cvmfs_volume(repository)

                (job['spec']['template']['spec']['containers'][0]
                    ['volumeMounts'].append(
                        {'name': volume['name'],
                         'mountPath': '/cvmfs/{}'.format(mount_path)}
                ))
                job['spec']['template']['spec']['volumes'].append(volume)

        # add better handling
        try:
            api_response = \
                current_k8s_batchv1_api_client.create_namespaced_job(
                    namespace=K8S_DEFAULT_NAMESPACE,
                    body=job)
            return backend_job_id
        except ApiException as e:
            logging.debug("Error while connecting to Kubernetes"
                          " API: {}".format(e))
        except Exception as e:
            logging.error(traceback.format_exc())
            logging.debug("Unexpected error: {}".format(e))

    def stop(backend_job_id, asynchronous=True):
        """Stop Kubernetes job execution.

        :param backend_job_id: Kubernetes job id.
        :param asynchronous: Whether the function waits for the action to be
            performed or does it asynchronously.
        """
        try:
            propagation_policy = 'Background' if asynchronous else 'Foreground'
            delete_options = V1DeleteOptions(
                propagation_policy=propagation_policy)
            current_k8s_batchv1_api_client.delete_namespaced_job(
                backend_job_id, K8S_DEFAULT_NAMESPACE,
                body=delete_options)
        except ApiException as e:
            logging.error(
                'An error has occurred while connecting to Kubernetes API '
                'Server \n {}'.format(e))
            raise ComputingBackendSubmissionError(e.reason)

    def add_shared_volume(self, job):
        """Add shared CephFS volume to a given job spec.

        :param job: Kubernetes job spec.
        """
        volume_mount, volume = get_shared_volume(
            self.workflow_workspace,
            current_app.config['SHARED_VOLUME_PATH_ROOT'])
        job['spec']['template']['spec']['containers'][0][
            'volumeMounts'].append(volume_mount)
        job['spec']['template']['spec']['volumes'].append(volume)
