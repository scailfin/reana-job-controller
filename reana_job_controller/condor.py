# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2017, 2018 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""HTCondor wrapper. Utilize libfactory's htcondorlib for job submission"""

import json
import logging
import os
import sys
import time
import traceback
import htcondor
import classad
from subprocess import check_output
from retrying import retry

from flask import current_app as app
from reana_db.database import Session
from reana_db.models import Job

from reana_job_controller import config, volume_templates
from reana_job_controller.errors import ComputingBackendSubmissionError

def detach(f):
    """Decorator for creating a forked process"""

    def fork(*args, **kwargs):
        pid = os.fork()
        if pid == 0:
            try:
                os.setuid(int(os.environ.get('VC3USERID')))
                f(*args, **kwargs)
            finally:
                os._exit(0)

    return fork

@retry(stop_max_attempt_number=5)
@detach
def submit(schedd, sub):
    try:
        with schedd.transaction() as txn:
            clusterid = sub.queue(txn)
    except Exception as e:
        logging.debug("Error submission: {0}".format(e))
        raise(Exception)

    return clusterid

def get_schedd():
    """Find and return the HTCondor sched.
    :returns: htcondor schedd object."""

    # Getting remote scheduler
    schedd_ad = classad.ClassAd()
    schedd_ad["MyAddress"] = os.environ.get("HTCONDOR_ADDR", None) 
    schedd = htcondor.Schedd(schedd_ad)
    return schedd

def condor_instantiate_job(job_id, docker_img, cmd, cvmfs_repos, env_vars, namespace,
                    shared_file_system, job_type):
    """Create condor job.

    :param job_id: Job uuid from reana perspective.
    :param docker_img: Docker / singularity  image to run the job.
    :param cmd: Command provided to the container.
    :param cvmfs_repos: List of CVMFS repository names.
    :param env_vars: Dictionary representing environment variables
        as {'var_name': 'var_value'}.
    :param namespace: Job's namespace.
    :shared_file_system: Boolean which represents whether the job
        should have a shared file system mounted.
    :returns: cluster_id of htcondor job.
    """

    # Getting remote scheduler
    schedd_ad = classad.ClassAd()
    schedd_ad["MyAddress"] = os.environ.get("HTCONDOR_ADDR", None) 
    schedd = htcondor.Schedd(schedd_ad)
    sub = htcondor.Submit()
    sub['executable'] = '/usr/bin/singularity'
    sub['arguments'] = "exec docker://{0} {1}".format(docker_img,cmd)
    sub['InitialDir'] = '/tmp'
    clusterid = submit(schedd, sub)
    # with schedd.transaction() as txn:
    #     clusterid = sub.queue(txn,1)

    return clusterid


def condor_watch_jobs(job_db):
    """Watch currently running HTCondor jobs.
    :param job_db: Dictionary which contains all current jobs.
    """
    while True:
        #logging.debug('Starting a new stream request to watch Condor Jobs')


    pass # not implemented yet

def start_watch_jobs_thread(JOB_DB):
    """Watch changes on jobs within HTCondor."""

    job_event_reader_thread = threading.Thread(target=condor_watch_jobs,
                                               args=(JOB_DB,))
    job_event_reader_thread.daemon = True
    job_event_reader_thread.start()


