# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2019 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA-Job-Controller Job Manager tests."""

import json
import os
import uuid

import mock
import pytest
from reana_db.models import Job, JobStatus

from reana_job_controller.job_manager import JobManager
from reana_job_controller.kubernetes_job_manager import KubernetesJobManager


def test_execute_kubernetes_job(app, session, sample_serial_workflow_in_db,
                                sample_workflow_workspace, default_user,
                                empty_user_secrets,
                                corev1_api_client_with_user_secrets,
                                monkeypatch):
    """Test execution of Kubernetes job."""
    workflow_uuid = sample_serial_workflow_in_db.id_
    next(sample_workflow_workspace(
        str(workflow_uuid)))
    expected_env_var_name = "env_var"
    expected_env_var_value = "value"
    expected_image = "busybox"
    expected_command = ["ls"]
    monkeypatch.setenv('REANA_USER_ID', str(default_user.id_))
    job_manager = KubernetesJobManager(
        docker_img=expected_image, cmd=expected_command,
        env_vars={expected_env_var_name: expected_env_var_value},
        workflow_uuid=workflow_uuid)
    with mock.patch("reana_job_controller.kubernetes_job_manager."
                    "current_k8s_batchv1_api_client") as kubernetes_client:
        with mock.patch("reana_commons.k8s.secrets."
                        "current_k8s_corev1_api_client",
                        corev1_api_client_with_user_secrets(
                            empty_user_secrets)):
            kubernetes_job_id = job_manager.execute()
            created_job = session.query(Job).filter_by(
                backend_job_id=kubernetes_job_id).one_or_none()
            assert created_job
            assert created_job.docker_img == expected_image
            assert created_job.cmd == json.dumps(expected_command)
            assert created_job.env_vars == json.dumps(
                {expected_env_var_name: expected_env_var_value})
            assert created_job.status == JobStatus.created
            kubernetes_client.create_namespaced_job.assert_called_once()
            body = kubernetes_client.create_namespaced_job.call_args[1]['body']
            env_vars = body['spec']['template']['spec']['containers'][0]['env']
            image = body['spec']['template']['spec']['containers'][0]['image']
            command = \
                body['spec']['template']['spec']['containers'][0]['command']
            assert len(env_vars) == 1
            assert env_vars[0]['name'] == expected_env_var_name
            assert env_vars[0]['value'] == expected_env_var_value
            assert image == expected_image
            assert command == expected_command


def test_stop_kubernetes_job(app, session, sample_serial_workflow_in_db,
                             sample_workflow_workspace,
                             empty_user_secrets, default_user,
                             corev1_api_client_with_user_secrets,
                             monkeypatch
                             ):
    """Test stop of Kubernetes job."""
    workflow_uuid = sample_serial_workflow_in_db.id_
    next(sample_workflow_workspace(
        str(workflow_uuid)))
    expected_env_var_name = "env_var"
    expected_env_var_value = "value"
    expected_image = "busybox"
    expected_command = ["ls"]
    monkeypatch.setenv('REANA_USER_ID', str(default_user.id_))
    job_manager = KubernetesJobManager(
        docker_img=expected_image, cmd=expected_command,
        env_vars={expected_env_var_name: expected_env_var_value},
        workflow_uuid=workflow_uuid)
    with mock.patch("reana_job_controller.kubernetes_job_manager."
                    "current_k8s_batchv1_api_client") as kubernetes_client:
        with mock.patch("reana_commons.k8s.secrets."
                        "current_k8s_corev1_api_client",
                        corev1_api_client_with_user_secrets(
                            empty_user_secrets)):
            kubernetes_job_id = job_manager.execute()
            kubernetes_client.create_namespaced_job.assert_called_once()
            job_manager.stop(kubernetes_job_id)
            kubernetes_client.delete_namespaced_job.assert_called_once()


def test_execution_hooks():
    """Test hook execution order."""
    class TestJobManger(JobManager):
        @JobManager.execution_hook
        def execute(self):
            self.order_list.append(2)
            job_id = str(uuid.uuid4())
            return job_id

        def before_execution(self):
            self.order_list = []
            self.order_list.append(1)

        def create_job_in_db(self, job_id):
            self.order_list.append(3)

        def cache_job(self):
            self.order_list.append(4)

    job_manager = TestJobManger("busybox", "ls", {})
    job_manager.execute()
    assert job_manager.order_list == [1, 2, 3, 4]
