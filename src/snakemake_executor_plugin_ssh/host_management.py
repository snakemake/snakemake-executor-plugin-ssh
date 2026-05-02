import shlex
from dataclasses import asdict
from abc import ABC, abstractmethod
import base64
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict
import psutil
import subprocess as sp

MIN_PY_VER = "3.7"
BASE_PATH = Path("/tmp/sshclust")
SCRIPT_PATH = BASE_PATH / "script"
USER_TOOLS_PATH = Path("~/.sshclust")
UV_EXEC = USER_TOOLS_PATH / "uv"
VENV_BASE_PATH = USER_TOOLS_PATH / "venvs"


BASE_PATH.mkdir(exist_ok=True, parents=True)


def get_snakemake_venv(snakemake_ver: str) -> Path:
    return VENV_BASE_PATH / "snakemake" / snakemake_ver


@dataclass
class LockManager(ABC):
    id: str

    @classmethod
    @abstractmethod
    def item(cls) -> str: ...

    @classmethod
    @abstractmethod
    def item_suffix(cls) -> str: ...

    @abstractmethod
    def write_init_lock(self) -> None: ...

    @property
    def item_path(self) -> Path:
        return BASE_PATH / self.item()

    @property
    def path_init(self) -> Path:
        return self.item_path.with_suffix(".init")

    @property
    def locked(self) -> Path:
        return self.item_path.with_suffix(f".{self.id}{self.item_suffix()}")

    @property
    def unlocked(self) -> Path:
        return self.item_path.with_suffix(self.item_suffix())

    def lock(self) -> None:
        while True:
            try:
                os.replace(self.unlocked, self.locked)
                self.locked.touch()
                return
            except FileNotFoundError:
                if not self.path_init.exists():
                    # no other process has locked this yet
                    open(self.path_init, "w").close()
                    self.write_init_lock()
                    return
                print("Waiting for lock to be released...", file=sys.stderr)
                # find stale locks and remove them, restoring unlocked state from the latest stale lock if possible
                if not self.cleanup_stale_locks():
                    # TODO use inotify to wait for file to appear instead of busy waiting?
                    time.sleep(10)

    def cleanup_stale_locks(self) -> None:
        current_time = time.time()
        locks = sorted(self.item_path.parent.glob(self.item_path.with_suffix(f".*{self.item_suffix()}").name), key=lambda lock: lock.stat().st_mtime, reverse=True)
        if locks[0].stat().st_mtime < current_time - 60:
            for lock in locks:
                lock.unlink()
            self.path_init.unlink(missing_ok=True)
            return True
        return False

    def unlock(self) -> None:
        try:
            os.replace(self.locked, self.unlocked)
        except FileNotFoundError:
            print(f"Lock file {self.locked} not found when trying to unlock {self.item()}", file=sys.stderr)


class DeployManager(LockManager):
    @classmethod
    def item(cls) -> str:
        return "deployment"

    @classmethod
    def item_suffix(cls) -> str:
        return ".lock"

    def write_init_lock(self) -> None:
        open(self.locked, "w").close()

    def locked_deploy(self, snakemake_ver: str) -> None:
        self.lock()
        print("Deploying Snakemake and dependencies...", file=sys.stderr)
        self.deploy_uv()
        self.deploy_snakemake(snakemake_ver)
        self.unlock()

    def deploy_uv(self) -> None:
        uv_exec = UV_EXEC.expanduser()
        if not uv_exec.exists():
            print(f"uv not found at {uv_exec}, installing...", file=sys.stderr)
            uv_exec.parent.mkdir(parents=True, exist_ok=True)
            sp.run(
                "curl -LsSf https://astral.sh/uv/install.sh | "
                f"env UV_UNMANAGED_INSTALL={shlex.quote(str(uv_exec.parent))} sh",
                shell=True,
                check=True,
            )

    def deploy_snakemake(self, snakemake_ver: str) -> None:
        uv_exec = UV_EXEC.expanduser()
        path = get_snakemake_venv(snakemake_ver).expanduser()
        if not path.exists():
            sp.run(
                f"test -d {path} || ({uv_exec} venv {path} && "
                f"source {path}/bin/activate && "
                f"{uv_exec} pip install snakemake=={snakemake_ver} pip)",
                shell=True,
                check=True,
            )
            print(f"Deployed Snakemake {snakemake_ver} to {path}", file=sys.stderr)
        else:
            print(f"Snakemake {snakemake_ver} already deployed at {path}", file=sys.stderr)


@dataclass
class HostInfo:
    version: int = 1
    cpu: int = field(default_factory=os.cpu_count)
    mem_mb: int = field(default_factory=lambda: psutil.virtual_memory().total)
    gpu: int = 0  # TODO determine if the system has a usable GPU

    def asdict(self) -> Dict[str, Any]:
        # restrict to the fields considered here and avoid leaking in fields from subclasses like QueryableHostInfo
        return {key: value for key, value in asdict(self).items() if key in {"version", "cpu", "mem_mb", "gpu"}}


@dataclass
class HostInfoManager(LockManager):
    @classmethod
    def item(cls) -> str:
        return "host_info"

    @classmethod
    def item_suffix(cls) -> str:
        return ".json"

    def write_init_lock(self) -> None:
        host_info = HostInfo().asdict()
        with open(self.locked, "w") as f:
            json.dump(host_info, f)

    def lock_and_read(self) -> str:
        self.lock()
        try:
            with open(self.locked, "r") as f:
                host_info = f.read()
            # print content to stdout for reading by the caller
            print(host_info)
        except Exception:
            self.unlock()
            raise

    def write_and_unlock(self, host_info: HostInfo) -> None:
        try:
            with open(self.locked, "w") as f:
                json.dump(host_info.asdict(), f)
        finally:
            self.unlock()


def decode_data(data: str) -> Dict[Any, Any]:
    return json.loads(base64.b64decode(data))


if __name__ == "__main__":
    args = sys.argv
    cmd = args[1]
    id = args[2]
    deploy_manager = DeployManager(id)
    host_info_manager = HostInfoManager(id)

    match cmd:
        case "lock-read":
            host_info_manager.lock_and_read()
        case "write-unlock":
            data = decode_data(args[3])
            host_info = HostInfo(**data)
            host_info_manager.write_and_unlock(host_info)
        case "unlock":
            host_info_manager.unlock()
        case "locked-deploy":
            snakemake_ver = args[3]
            deploy_manager.locked_deploy(snakemake_ver)
