import base64
from collections import defaultdict
from io import StringIO
from itertools import chain
import json
import os
from pathlib import Path
import subprocess as sp
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Iterable, List, Generator, Optional
import uuid
from immutables import Map
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_ssh.host_management import HostInfo
from snakemake_executor_plugin_ssh import host_management
import snakemake


# Optional:
# Define additional settings for your executor.
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
# Make sure that all defined fields are Optional and specify a default value
# of None or anything else that makes sense in your case.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    hosts: List[str] = field(
        default_factory=list,
        metadata={
            "help": (
                "List of hostnames to spawn jobs to, have to be setup for key-based "
                "password free login via you ssh config."
            ),
            "required": True,
            "nargs": "+",
        },
    )
    gpu_only_hosts: List[str] = field(
        default_factory=list,
        metadata={
            "help": (
                "List of hostnames to spawn jobs to that are for gpu jobs only. "
                "Have to be setup for key-based "
                "password free login via you ssh config."
            ),
            "required": True,
            "nargs": "+",
        },
    )
    identity_file: Optional[str] = field(
        default=None,
        metadata={
            "help": "SSH key file to use (see man ssh, flag -i)",
            "required": False,
        },
    )

    @property
    def all_hosts(self):
        return chain(self.hosts, self.gpu_only_hosts)


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Whether the executor implies to not have a shared file system
    implies_no_shared_fs=True,
    # whether to deploy workflow sources to default storage provider before execution
    job_deploy_sources=True,
    # whether arguments for setting the storage provider shall be passed to jobs
    pass_default_storage_provider_args=True,
    # whether arguments for setting default resources shall be passed to jobs
    pass_default_resources_args=True,
    # whether environment variables shall be passed to jobs (if False, use
    # self.envvars() to obtain a dict of environment variables and their values
    # and pass them e.g. as secrets to the execution backend)
    pass_envvar_declarations_to_cmd=True,
    # whether the default storage provider shall be deployed before the job is run on
    # the remote node. Usually set to True if the executor does not assume a shared fs
    auto_deploy_default_storage_provider=True,
    # specify initial amount of seconds to sleep before checking for job status
    init_seconds_before_status_checks=0,
)


class QueryableHostInfo(HostInfo):
    def is_feasible(self, job: JobExecutorInterface) -> bool:
        job_gpu = job.resources.get("gpu", 0)
        if self.gpu_only and not job_gpu:
            return False
        return (
            job.threads <= self.cpu
            and job.resources.get("mem_mb", 0) <= self.mem_mb
            and job_gpu <= self.gpu
        )

    def register(self, job: JobExecutorInterface) -> None:
        self.cpu -= job.threads
        self.gpu -= job.resources.get("gpu", 0)
        self.mem_mb -= job.resources.get("mem_mb", 0)

    def unregister(self, job: JobExecutorInterface) -> None:
        self.cpu += job.threads
        self.gpu += job.resources.get("gpu", 0)
        self.mem_mb += job.resources.get("mem_mb", 0)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        self.run_id: str = str(uuid.uuid4())
        self.file_to_host: Map[str, str] = {}
        with open(host_management.__file__, "r") as f:
            self.host_info_script = f.read()

        snakemake_ver = ".".join(snakemake.__version__.split(".")[:3])

        # deploy uv to each host
        for host in self.workflow.executor_settings.all_hosts:
            self._run_host_mgmt_script(
                host, "locked-deploy", self.run_id, data=snakemake_ver
            )

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.
        # If required, make sure to pass the job's id to the job_info object, as keyword
        # argument 'external_job_id'.

        with StringIO(self.get_jobscript(job)) as jobscript:
            host = self._get_host(job)
            for f in job.output:
                self.file_to_host[f] = host
            proc = sp.Popen(
                ["ssh", host, "bash"], stdin=jobscript, stdout=sp.PIPE, stderr=sp.STDOUT
            )
            self.report_job_submission(SubmittedJobInfo(job, aux={"proc": proc}))

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None]:
        # Check the status of active jobs.

        # You have to iterate over the given list active_jobs.
        # If you provided it above, each will have its external_jobid set according
        # to the information you provided at submission time.
        # For jobs that have finished successfully, you have to call
        # self.report_job_success(active_job).
        # For jobs that have errored, you have to call
        # self.report_job_error(active_job).
        # This will also take care of providing a proper error message.
        # Usually there is no need to perform additional logging here.
        # Jobs that are still running have to be yielded.
        #
        # For queries to the remote middleware, please use
        # self.status_rate_limiter like this:
        #
        # async with self.status_rate_limiter:
        #    # query remote middleware here
        #
        # To modify the time until the next call of this method,
        # you can set self.next_sleep_seconds here.
        for active_job in active_jobs:
            proc: sp.Popen = active_job.aux["proc"]
            ret = proc.poll()
            if ret is not None:
                if ret == 0:
                    self.report_job_success(active_job)
                else:
                    self.report_job_error(active_job)
            else:
                yield active_job

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        for active_job in active_jobs:
            proc: sp.Popen = active_job.aux["proc"]
            proc.kill()

    def shutdown(self):
        super().shutdown()
        for host in self.workflow.executor_settings.all_hosts:
            self._unlock_host(host)
            self._run_host_cmd(host, f"rm -f {self._script_path}")

    def _get_potential_hosts(self, job: JobExecutorInterface) -> Iterable[str]:
        is_gpu_job = job.resources.get("gpu", 0) > 0
        settings = self.workflow.executor_settings
        if is_gpu_job:
            return settings.all_hosts
        else:
            return settings.hosts

    def _get_host(self, job: JobExecutorInterface):
        feasible_hosts = {}
        for host in self.workflow.executor_settings.hosts:
            host_info = self._lock_and_read_host_info(host)
            if host_info.is_feasible(job):
                feasible_hosts[host] = host_info
            else:
                self._unlock_host(host)

        host_weight = defaultdict(int)
        for f in job.input:
            host = self.file_to_host.get(f)
            if host is not None and host in feasible_hosts:
                host_weight[host] += os.path.getsize(f)

        if host_weight:
            # At least one file is already present on one host.
            # Return the host with the largest total input file size present.
            selected_host = sorted(host, key=host_weight.get, reverse=True)[0]
        else:
            # Otherwise, if gpu job prefer gpu only hosts, if not any.
            # Among them, return the host with least memory used, in order to minimize
            # out of memory issues in case jobs exceed their annotated memory.
            def sort_key(item):
                host, host_info = item
                return (
                    host not in self.workflow.executor_settings.gpu_only_hosts,
                    host_info.mem_mb,
                )

            selected_host = sorted(feasible_hosts.items(), key=sort_key)[0]

        for host, host_info in feasible_hosts.items():
            if host == selected_host:
                host_info.register(job)
                self._write_host_info_and_unlock(host, host_info)
            else:
                self._unlock_host(host)

    def _lock_and_read_host_info(self, host: str) -> QueryableHostInfo:
        res = self._run_host_mgmt_script(host, "lock-read", run_id=self.run_id)
        return QueryableHostInfo(**json.load(res.stdout))

    def _write_host_info_and_unlock(self, host: str, host_info: HostInfo) -> None:
        self._run_host_mgmt_script(
            host,
            "write-unlock",
            run_id=self.run_id,
            data=base64.b64encode(json.dumps(host_info.asdict()).encode()),
        )

    def _unlock_host(self, host: str) -> None:
        self._run_host_mgmt_script(host, "unlock", run_id=self.run_id)

    @property
    def _script_path(self) -> Path:
        return host_management.SCRIPT_PATH / self.run_id / "host_management.py"

    def _run_host_mgmt_script(
        self, host: str, cmd: str, run_id: str, data: Optional[str] = None
    ) -> sp.CompletedProcess[bytes]:
        if data is None:
            data = ""

        return self._run_host_cmd(
            host,
            f"bash -c 'mkdir -p {self._script_path.parent}; "
            f"cat > {self._script_path}; "
            f"python {host_management.SCRIPT_PATH} {cmd} {run_id} {data}'",
            input=self.host_info_script.encode(),
        )

    def _run_host_cmd(
        self, host: str, cmd: str, **kwargs: Any
    ) -> sp.CompletedProcess[bytes]:
        identity_file = self.workflow.executor_settings.identity_file
        identity = "" if identity_file is None else f"-i {identity_file}"
        try:
            return sp.run(
                f"ssh {identity} {host} {cmd}",
                check=True,
                stdout=sp.PIPE,
                stderr=sp.PIPE,
                shell=True,
                **kwargs,
            )
        except sp.CalledProcessError as e:
            raise WorkflowError(
                f"Failed to run command on host {host}: {e.stderr.decode()}"
            )
