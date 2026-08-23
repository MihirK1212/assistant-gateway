"""
Launcher for the gateway runner.

Provides a single CLI entry point to launch both FastAPI and Celery worker
from a JSON config file.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from typing import List, Optional


def build_fastapi_command(
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = True,
) -> List[str]:
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "assistant_gateway.runner.bootstrap.fast_api:app",
        "--host",
        host,
        "--port",
        str(port),
    ]
    if reload:
        cmd.append("--reload")
    return cmd


def build_celery_command(
    pool: str = "prefork",
    concurrency: Optional[int] = None,
    log_level: str = "info",
    extra_args: Optional[List[str]] = None,
) -> List[str]:
    cmd = [
        sys.executable,
        "-m",
        "celery",
        "-A",
        "assistant_gateway.runner.bootstrap.celery_app:celery_app",
        "worker",
        "-E",
        "--pool",
        pool,
        "--loglevel",
        log_level,
    ]

    if concurrency is not None:
        cmd.extend(["--concurrency", str(concurrency)])

    if extra_args:
        cmd.extend(extra_args)

    return cmd


class GatewayRunner:
    CELERY_STARTUP_GRACE_PERIOD = 5.0  # seconds

    def __init__(
        self,
        config_path: str,
        *,
        working_dir: Optional[str] = None,
        fastapi_host: str = "127.0.0.1",
        fastapi_port: int = 8000,
        fastapi_reload: bool = True,
        celery_pool: str = "prefork",
        celery_concurrency: Optional[int] = None,
        celery_log_level: str = "info",
        celery_extra_args: Optional[List[str]] = None,
        fastapi: bool = False,
        celery: bool = False,
    ):
        if fastapi and celery:
            raise ValueError("Cannot specify both --fastapi and --celery")
        
        self.config_path = config_path
        self.working_dir = working_dir or os.getcwd()
        self.fastapi_host = fastapi_host
        self.fastapi_port = fastapi_port
        self.fastapi_reload = fastapi_reload
        self.celery_pool = celery_pool
        self.celery_concurrency = celery_concurrency
        self.celery_log_level = celery_log_level
        self.celery_extra_args = celery_extra_args
        self.fastapi = fastapi
        self.celery = celery

        self._processes: List[subprocess.Popen] = []
        self._fastapi_proc: Optional[subprocess.Popen] = None
        self._celery_proc: Optional[subprocess.Popen] = None
        self._celery_healthy = False
        self._shutting_down = False

    def run(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            self._validate_config()

            fastapi_proc = None
            if self.fastapi:
                fastapi_proc = self._start_fastapi()

            celery_proc = None
            if self.celery:
                celery_proc = self._start_celery()

            if celery_proc is not None:
                celery_started = self._check_celery_startup()
                if not celery_started:
                    self._handle_celery_failure()
                    self.stop()
                    return 1

            print("Gateway Runner Started!")
            if fastapi_proc is not None:
                print(f"  FastAPI: http://{self.fastapi_host}:{self.fastapi_port}")
            elif celery_proc is not None:
                print(f"  Celery:  Worker running with pool={self.celery_pool}")
            else:
                raise ValueError("No processes started")

            # process.poll() returns None if the process is still running, otherwise it returns the exit code
            while not self._shutting_down:
                for proc in list(self._processes):
                    if proc.poll() is not None:
                        is_celery = proc == self._celery_proc
                        is_fastapi = proc == self._fastapi_proc

                        if is_celery:
                            print(f"\nERROR: Celery worker exited with code {proc.returncode}")
                            if not self._shutting_down:
                                self._shutting_down = True
                                self.stop()
                                return proc.returncode
                        elif is_fastapi:
                            print(f"\nERROR: FastAPI server exited with code {proc.returncode}")
                            if not self._shutting_down:
                                self._shutting_down = True
                                self.stop()
                                return proc.returncode
                        else:
                            print(f"\nProcess {proc.pid} exited with code {proc.returncode}")
                            if not self._shutting_down:
                                self._shutting_down = True
                                self.stop()
                                return proc.returncode

                time.sleep(0.5)

        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

        return 0

    def _get_env(self) -> dict:
        env = os.environ.copy()
        env["GATEWAY_CONFIG_PATH"] = self.config_path
        env["GATEWAY_WORKING_DIR"] = self.working_dir
        return env

    def _start_fastapi(self) -> Optional[subprocess.Popen]:
        if not self.fastapi: 
            raise ValueError("Cannot start FastAPI if --fastapi is not set") 

        cmd = build_fastapi_command(
            host=self.fastapi_host,
            port=self.fastapi_port,
            reload=self.fastapi_reload,
        )

        print(f"Starting FastAPI: {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd,
            env=self._get_env(),
            cwd=self.working_dir,
        )
        self._processes.append(proc)
        self._fastapi_proc = proc
        return proc

    def _start_celery(self) -> Optional[subprocess.Popen]:
        if not self.celery:
            raise ValueError("Cannot start Celery if --celery is not set")

        cmd = build_celery_command(
            pool=self.celery_pool,
            concurrency=self.celery_concurrency,
            log_level=self.celery_log_level,
            extra_args=self.celery_extra_args,
        )

        print(f"Starting Celery: {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd,
            env=self._get_env(),
            cwd=self.working_dir,
        )
        self._processes.append(proc)
        self._celery_proc = proc
        return proc

    def _check_celery_startup(self) -> bool:
        if self._celery_proc is None:
            return True

        print(f"Waiting {self.CELERY_STARTUP_GRACE_PERIOD}s for Celery to start...")

        start_time = time.time()
        while time.time() - start_time < self.CELERY_STARTUP_GRACE_PERIOD:
            if self._celery_proc.poll() is not None:
                return False
            time.sleep(0.2)

        if self._celery_proc.poll() is not None:
            return False

        return True

    def _handle_celery_failure(self):
        exit_code = self._celery_proc.returncode if self._celery_proc else None
        print(f"ERROR: Celery worker failed to start!  Exit code: {exit_code}")
        if self._celery_proc in self._processes:
            self._processes.remove(self._celery_proc)
        self._celery_proc = None

    def _signal_handler(self, signum, frame):
        if self._shutting_down:
            return
        self._shutting_down = True
        print("\nShutting down...")
        self.stop()

    def stop(self):
        for proc in self._processes:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    def _validate_config(self):
        if self.working_dir not in sys.path:
            sys.path.insert(0, self.working_dir)

        if (self.fastapi and self.celery):
            raise ValueError("Cannot specify both --fastapi and --celery")

        from assistant_gateway.runner.config import parse_config

        result = parse_config(self.config_path)

        if result.gateway_config.clauq_btm is None and self.celery:
            raise ValueError(
                "clauq_btm is not configured in the JSON config. Celery requires it. "
                "Use --fastapi to run without Celery."
            )

        if result.app is None and self.fastapi:
            raise ValueError(
                "rest_api is not configured in the JSON config, "
                "but a FastAPI app is required unless --celery is set."
            )



def main(args: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(
        prog="assistant-gateway",
        description="launch FastAPI and Celery worker for the Assistant Gateway",
    )

    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="path to the JSON gateway config file.",
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="host to bind FastAPI server (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=8000,
        help="port for FastAPI server (default: 8000)",
    )
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="disable auto-reload for FastAPI",
    )

    parser.add_argument(
        "--celery-pool",
        default="prefork",
        choices=["prefork", "solo", "threads", "gevent", "eventlet"],
        help="celery worker pool type (default: prefork)",
    )
    parser.add_argument(
        "--celery-concurrency",
        type=int,
        help="number of celery worker processes/threads",
    )
    parser.add_argument(
        "--celery-loglevel",
        default="info",
        choices=["debug", "info", "warning", "error", "critical"],
        help="celery log level (default: info)",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--fastapi",
        action="store_true",
        help="start only FastAPI server (no Celery worker)",
    )
    mode_group.add_argument(
        "--celery",
        action="store_true",
        help="start only Celery worker (no FastAPI server)",
    )

    parser.add_argument(
        "--working-dir",
        "-w",
        help="working directory for both processes (default: current directory)",
    )

    parsed = parser.parse_args(args)

    runner = GatewayRunner(
        config_path=parsed.config,
        working_dir=parsed.working_dir,
        fastapi_host=parsed.host,
        fastapi_port=parsed.port,
        fastapi_reload=not parsed.no_reload,
        celery_pool=parsed.celery_pool,
        celery_concurrency=parsed.celery_concurrency,
        celery_log_level=parsed.celery_loglevel,
        fastapi=parsed.fastapi,
        celery=parsed.celery,
    )

    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
