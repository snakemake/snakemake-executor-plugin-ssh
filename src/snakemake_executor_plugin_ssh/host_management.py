from abc import ABC, abstractmethod
import base64
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Dict
import psutil
import subprocess as sp

MIN_PY_VER = "3.7"
BASE_PATH = Path("/tmp/sshclust")
SCRIPT_PATH = BASE_PATH / "script"
VENV_BASE_PATH = Path("~/.sshclust/uv/venv")


BASE_PATH.mkdir(exist_ok=True, parents=True)


def get_snakemake_venv(snakemake_ver: str) -> str:
    return str(VENV_BASE_PATH / "snakemake" / snakemake_ver)


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
    def locked(self) -> str:
        return self.item_path.with_suffix(f"{self.id}.{self.item_suffix()}")

    @property
    def unlocked(self) -> str:
        return self.item_path.with_suffix(self.item_suffix())

    def lock(self) -> None:
        while True:
            try:
                os.rename(self.unlocked, self.locked)
            except FileNotFoundError:
                if not self.path_init.exists():
                    # no other process has locked this yet
                    open(self.path_init, "w").close()
                    self.write_init_lock()
                # TODO use inotify to wait for file to appear instead of busy waiting
                time.sleep(10)

    def unlock(self) -> None:
        os.rename(self.locked, self.unlocked)


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
        self.deploy_uv()
        self.deploy_snakemake(snakemake_ver)
        self.unlock()

    def deploy_uv(self) -> None:
        if shutil.which("uv") is None:
            sp.run(
                "curl -LsSf https://astral.sh/uv/install.sh | sh",
                shell=True,
                check=True,
            )

    def deploy_snakemake(self, snakemake_ver: str) -> None:
        path = get_snakemake_venv(snakemake_ver)
        sp.run(
            f"test -d {path} || (uv venv {path} && "
            f"uv pip install snakemake=={snakemake_ver} pip)",
            shell=True,
            check=True,
        )


@dataclass
class HostInfo:
    version: int = 1
    cpu: int = field(default_factory=os.cpu_count)
    mem_mb: int = field(default_factory=lambda: psutil.virtual_memory().total)
    gpu: int = 0  # TODO determine if the system has a usable GPU


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

    @property
    def path_init(self) -> Path:
        return Path("/tmp/snakemake_host_info_init.txt")

    @property
    def locked(self) -> str:
        return self.path.format(f".{self.id}.locked.")

    @property
    def unlocked(self) -> str:
        return self.path.format("")

    def lock_and_read(self) -> str:
        self.lock()
        with open(self.locked, "r") as f:
            host_info = f.read()
        print(host_info)

    def write_and_unlock(self, host_info: HostInfo) -> None:
        with open(self.locked, "w") as f:
            json.dump(host_info.asdict(), f)
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
            host_info = HostInfo(**json.loads(data))
            host_info_manager.write_and_unlock(host_info)
        case "unlock":
            host_info_manager.unlock()
        case "locked-deploy":
            snakemake_ver = args[3]
            deploy_manager.locked_deploy(snakemake_ver)
