from typing import Dict
from functools import partial
from threading import Lock
import time
from threading import Thread
from typing import Deque
from collections import deque
from snakemake_executor_plugin_ssh.host_management import get_snakemake_venv
from tenacity import stop_after_attempt
import shlex
import base64
from collections import defaultdict
from itertools import chain
import json
import os
from pathlib import Path
import subprocess as sp
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional
import uuid
from tenacity import retry

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


@dataclass
class Host:
    hostname: str
    port: int
    workdir: str
    gpu_only: bool = False

    @classmethod
    def from_str(cls, s: str, gpu_only: bool = False) -> "Host":
        parts = s.split(":")
        if len(parts) != 3:
            raise ValueError(f"Invalid host string: {s}")
        return cls(
            hostname=parts[0], port=int(parts[1]), workdir=parts[2], gpu_only=gpu_only
        )

    def __str__(self) -> str:
        return f"{self.hostname}:{self.port}:{self.workdir}"

    def __hash__(self) -> int:
        return hash(self.hostname)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Host):
            return NotImplemented
        return self.hostname == other.hostname


@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    hosts: List[str] = field(
        default_factory=list,
        metadata={
            "help": (
                "List of hosts to spawn jobs to (host:port:workdir), "
                "have to be setup for key-based "
                "password free login via your ssh config or with identity "
                "provided via --identity-file."
            ),
            "required": True,
            "nargs": "+",
        },
    )
    gpu_only_hosts: List[str] = field(
        default_factory=list,
        metadata={
            "help": (
                "List of hostnames to spawn jobs (to host:port:workdir), that are "
                "only meant for GPU jobs. Have to be setup for key-based "
                "password free login via your ssh config or with identity "
                "provided via --identity-file."
            ),
            "required": True,
            "nargs": "+",
        },
    )
    identity_file: Optional[str] = field(
        default=None,
        metadata={
            "help": "SSH key file to use (see man ssh, flag -i). SSH config is considered "
            "otherwise.",
            "required": False,
        },
    )
    ssh_args: Optional[str] = field(
        default=None,
        metadata={"help": "Additional SSH arguments to use"},
    )

    @property
    def all_parsed_hosts(self) -> Iterable[Host]:
        return chain(self.parsed_hosts, self.parsed_gpu_only_hosts)

    @property
    def parsed_hosts(self) -> Iterable[Host]:
        return map(Host.from_str, self.hosts)

    @property
    def parsed_gpu_only_hosts(self) -> Iterable[Host]:
        return map(partial(Host.from_str, gpu_only=True), self.gpu_only_hosts)


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
    implies_no_shared_fs=False,
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


@dataclass
class QueryableHostInfo(HostInfo):
    gpu_only: bool = False

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
        self._run_id: str = str(uuid.uuid4())
        self._file_to_host: Dict[str, Host] = {}
        self._job_queue: Deque[JobExecutorInterface] = deque()
        self._job_queue_lock = Lock()

        with open(host_management.__file__, "r") as f:
            self._host_info_script = f.read()

        self._snakemake_ver = ".".join(snakemake.__version__.split(".")[:3])

        self.all_hosts = list(self.workflow.executor_settings.all_parsed_hosts)
        self.gpu_only_hosts = list(
            self.workflow.executor_settings.parsed_gpu_only_hosts
        )
        self.hosts = list(self.workflow.executor_settings.parsed_hosts)

        if len(set(self.all_hosts)) != len(self.hosts):
            raise WorkflowError(
                "Each hostname may only occur once across hosts and gpu_only_hosts "
                "(also not twice with different workdirs or ports)"
            )

        # deploy uv to each host
        for host in self.all_hosts:
            self._run_host_mgmt_script(
                host, "locked-deploy", self._run_id, data=self._snakemake_ver
            )

        # start thread that retries queued jobs every 30 seconds
        self._queue_handler = Thread(
            target=self._retry_queued_jobs, daemon=True
        ).start()

    def _retry_queued_jobs(self):
        while True:
            job = None
            with self._job_queue_lock:
                try:
                    job = self._job_queue.popleft()
                except IndexError:  # empty queue
                    pass
            if job is not None:
                self.run_job(job)
            time.sleep(30)

    def get_job_exec_prefix(self, job: JobExecutorInterface) -> str:
        return f"source {get_snakemake_venv(self._snakemake_ver)}/bin/activate"

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

        host = self._get_host(job)
        if host is None:
            with self._job_queue_lock:
                self._job_queue.append(job)
            return

        for f in job.output:
            self._file_to_host[f] = host
        proc = sp.Popen(
            ["ssh", *self._ssh_args(host), "bash"],
            stdin=sp.PIPE,
            stdout=sp.PIPE,
            stderr=sp.STDOUT,
        )
        assert proc.stdin is not None
        proc.stdin.write(
            (
                f"mkdir -p {host.workdir} && cd {host.workdir} && "
                + self.format_job_exec(job)
            ).encode()
        )
        proc.stdin.close()
        self.report_job_submission(SubmittedJobInfo(job, aux={"proc": proc}))

    async def check_active_jobs(self, active_jobs: List[SubmittedJobInfo]):
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
            assert active_job.aux is not None
            proc: sp.Popen = active_job.aux["proc"]
            assert proc.stdout is not None
            ret = proc.poll()
            if ret is not None:
                if ret == 0:
                    self.report_job_success(active_job)
                else:
                    self.report_job_error(active_job, msg=proc.stdout.read().decode())
            else:
                yield active_job

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        for active_job in active_jobs:
            assert active_job.aux is not None
            proc: sp.Popen = active_job.aux["proc"]
            proc.kill()

    def shutdown(self):
        super().shutdown()
        for host in self.all_hosts:
            self._unlock_host(host)
            self._run_host_cmd(host, f"rm -f {self._script_path}")

    def _get_potential_hosts(self, job: JobExecutorInterface) -> Iterable[Host]:
        is_gpu_job = job.resources.get("gpu", 0) > 0  # type: ignore
        if is_gpu_job:
            return self.all_hosts
        else:
            return self.hosts

    @retry(stop=stop_after_attempt(3))
    def _get_host(self, job: JobExecutorInterface) -> Host | None:
        feasible_hosts = {}
        try:
            for host in self._get_potential_hosts(job):
                host_info = self._lock_and_read_host_info(host)
                if host_info.is_feasible(job):
                    feasible_hosts[host] = host_info
                else:
                    self._unlock_host(host)

            if not feasible_hosts:
                return None

            host_weight = defaultdict(int)
            for f in job.input:
                host = self._file_to_host.get(f)
                if host is not None and host in feasible_hosts:
                    host_weight[host] += os.path.getsize(f)

            if host_weight:
                # At least one file is already present on one host.
                # Return the host with the largest total input file size present.
                selected_host = sorted(host_weight, key=host_weight.get, reverse=True)[  # type: ignore
                    0
                ]
            else:
                # Otherwise, if gpu job prefer gpu only hosts, if not any.
                # Among them, return the host with least memory used, in order to minimize
                # out of memory issues in case jobs exceed their annotated memory.
                def sort_key(item):
                    host, host_info = item
                    return (
                        host not in self.gpu_only_hosts,
                        host_info.mem_mb,
                    )

                selected_host = sorted(feasible_hosts.items(), key=sort_key)[0][0]

            for host, host_info in feasible_hosts.items():
                if host == selected_host:
                    host_info.register(job)
                    self._write_host_info_and_unlock(host, host_info)
                else:
                    self._unlock_host(host)
            return selected_host
        except Exception:
            # Ensure all locked hosts are unlocked on error
            for host in feasible_hosts:
                try:
                    self._unlock_host(host)
                except Exception:
                    pass
            raise

    def _lock_and_read_host_info(self, host: Host) -> QueryableHostInfo:
        res = self._run_host_mgmt_script(host, "lock-read", run_id=self._run_id)
        return QueryableHostInfo(
            gpu_only=host in self.gpu_only_hosts, **json.loads(res.stdout)
        )

    def _write_host_info_and_unlock(self, host: Host, host_info: HostInfo) -> None:
        self._run_host_mgmt_script(
            host,
            "write-unlock",
            run_id=self._run_id,
            data=base64.b64encode(json.dumps(host_info.asdict()).encode()).decode(),
        )

    def _unlock_host(self, host: Host) -> None:
        self._run_host_mgmt_script(host, "unlock", run_id=self._run_id)

    @property
    def _script_path(self) -> Path:
        return host_management.SCRIPT_PATH / self._run_id / "host_management.py"

    def _run_host_mgmt_script(
        self, host: Host, cmd: str, run_id: str, data: Optional[str] = None
    ) -> sp.CompletedProcess[bytes]:
        if data is None:
            data = ""

        return self._run_host_cmd(
            host,
            f"bash -c 'mkdir -p {self._script_path.parent} && "
            f"cat > {self._script_path} && "
            f"python {self._script_path} {cmd} {run_id} {data}'",
            input=self._host_info_script.encode(),
        )

    def _run_host_cmd(
        self, host: Host, cmd: str, **kwargs: Any
    ) -> sp.CompletedProcess[bytes]:

        self.logger.info(f"Running SSH command on host {host}: {cmd}")
        try:
            return sp.run(
                f"ssh {' '.join(self._ssh_args(host))} {shlex.quote(cmd)}",
                check=True,
                stdout=sp.PIPE,
                #stderr=sp.PIPE,
                shell=True,
                **kwargs,
            )
        except sp.CalledProcessError as e:
            raise WorkflowError(
                f"Failed to run command on host {host}" #: {e.stderr.decode()}"
            )

    def _ssh_args(self, host: Host) -> List[str]:
        executor_settings = self.workflow.executor_settings  # type: ignore
        identity_file = executor_settings.identity_file
        identity = [] if identity_file is None else ["-i", str(identity_file)]
        aux_args = shlex.split(executor_settings.ssh_args) or []

        return ["-p", str(host.port), *identity, *aux_args, host.hostname]
