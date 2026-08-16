"""
Launcher for the gateway runner.

Provides a single CLI entry point to launch both FastAPI and Celery worker
from a JSON config file.
"""

from __future__ import annotations

import argparse
import os
import platform
import signal
import subprocess
import sys
import time
from typing import List, Optional


def get_default_celery_pool() -> str:
    """Get the default Celery pool type based on the OS."""
    if platform.system() == "Windows":
        return "solo"  # prefork doesn't work on Windows
    return "prefork"


def build_fastapi_command(
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = True,
) -> List[str]:
    """Build the uvicorn command for FastAPI using the bootstrap runner module."""
    cmd = [
        sys.executable, "-m", "uvicorn",
        "assistant_gateway.runner._fastapi_runner:app",
        "--host", host,
        "--port", str(port),
    ]
    if reload:
        cmd.append("--reload")
    return cmd


def build_celery_command(
    pool: Optional[str] = None,
    concurrency: Optional[int] = None,
    log_level: str = "info",
    extra_args: Optional[List[str]] = None,
) -> List[str]:
    """Build the Celery worker command."""
    pool = pool or get_default_celery_pool()

    cmd = [
        sys.executable, "-m", "celery",
        "-A", "assistant_gateway.runner.celery_app:celery_app",
        "worker",
        "-E",  # Enable events
        "--pool", pool,
        "--loglevel", log_level,
    ]

    if concurrency is not None:
        cmd.extend(["--concurrency", str(concurrency)])

    if extra_args:
        cmd.extend(extra_args)

    return cmd


class GatewayRunner:
    """
    Manages the lifecycle of FastAPI and Celery worker processes.

    Supports graceful degradation: if Celery fails to start (e.g., Redis unavailable),
    FastAPI can continue running with sync-only task execution.
    """

    CELERY_STARTUP_GRACE_PERIOD = 5.0  # seconds

    def __init__(
        self,
        config_path: str,
        *,
        working_dir: Optional[str] = None,
        fastapi_host: str = "127.0.0.1",
        fastapi_port: int = 8000,
        fastapi_reload: bool = True,
        celery_pool: Optional[str] = None,
        celery_concurrency: Optional[int] = None,
        celery_log_level: str = "info",
        celery_extra_args: Optional[List[str]] = None,
        fastapi_only: bool = False,
        celery_only: bool = False,
        celery_optional: bool = True,
    ):
        self.config_path = config_path
        self.working_dir = working_dir or os.getcwd()
        self.fastapi_host = fastapi_host
        self.fastapi_port = fastapi_port
        self.fastapi_reload = fastapi_reload
        self.celery_pool = celery_pool
        self.celery_concurrency = celery_concurrency
        self.celery_log_level = celery_log_level
        self.celery_extra_args = celery_extra_args
        self.fastapi_only = fastapi_only
        self.celery_only = celery_only
        self.celery_optional = celery_optional

        self._processes: List[subprocess.Popen] = []
        self._fastapi_proc: Optional[subprocess.Popen] = None
        self._celery_proc: Optional[subprocess.Popen] = None
        self._celery_healthy = False
        self._shutting_down = False

    def _get_env(self) -> dict:
        """Get environment variables for subprocess."""
        env = os.environ.copy()
        env["GATEWAY_CONFIG_PATH"] = self.config_path
        env["GATEWAY_WORKING_DIR"] = self.working_dir
        return env

    def _start_fastapi(self) -> Optional[subprocess.Popen]:
        """Start the FastAPI dev server."""
        if self.celery_only:
            return None

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
        """Start the Celery worker."""
        if self.fastapi_only:
            return None

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
        """
        Wait for Celery startup grace period and check if it's still running.

        Returns:
            True if Celery started successfully, False if it failed.
        """
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

    def _handle_celery_failure(self) -> bool:
        """
        Handle Celery startup failure.

        Returns:
            True if we should continue (with FastAPI only), False if we should abort.
        """
        exit_code = self._celery_proc.returncode if self._celery_proc else None

        print("\n" + "=" * 60)
        print("WARNING: Celery worker failed to start!")
        print("=" * 60)
        print(f"  Exit code: {exit_code}")
        print("  Common causes:")
        print("    - Redis server not running")
        print("    - Invalid config path or config error")
        print("    - Missing dependencies")
        print("=" * 60)

        if self._celery_proc in self._processes:
            self._processes.remove(self._celery_proc)
        self._celery_proc = None

        if self.celery_only:
            print("ERROR: --celery-only mode requires Celery to start successfully.")
            return False

        if self.celery_optional and self._fastapi_proc is not None:
            print("Continuing with FastAPI only (sync tasks will still work).")
            print("Background/async tasks will NOT be available.")
            print("=" * 60 + "\n")
            return True
        else:
            print("ERROR: Celery is required. Use --celery-optional to allow fallback.")
            return False

    def _signal_handler(self, signum, frame):
        """Handle termination signals."""
        if self._shutting_down:
            return
        self._shutting_down = True
        print("\nShutting down...")
        self.stop()

    def stop(self):
        """Stop all running processes."""
        for proc in self._processes:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    def run(self):
        """Start all processes and wait for them to complete."""
        signal.signal(signal.SIGINT, self._signal_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            print(f"Validating config from: {self.config_path}")
            self._validate_config()
            print("Config validated successfully!\n")

            fastapi_proc = self._start_fastapi()
            celery_proc = self._start_celery()

            if celery_proc is not None:
                celery_started = self._check_celery_startup()
                if not celery_started:
                    should_continue = self._handle_celery_failure()
                    if not should_continue:
                        self.stop()
                        return 1
                else:
                    self._celery_healthy = True

            print("\n" + "=" * 60)
            print("Gateway Runner Started!")
            print("=" * 60)
            if fastapi_proc:
                print(f"  FastAPI: http://{self.fastapi_host}:{self.fastapi_port}")
            if self._celery_healthy:
                print(f"  Celery:  Worker running with pool={self.celery_pool or get_default_celery_pool()}")
            elif celery_proc is None and not self.fastapi_only:
                print("  Celery:  Not started (FastAPI-only mode)")
            print("=" * 60)
            print("Press Ctrl+C to stop all services\n")

            while not self._shutting_down:
                for proc in list(self._processes):
                    if proc.poll() is not None:
                        is_celery = proc == self._celery_proc
                        is_fastapi = proc == self._fastapi_proc

                        if is_celery and self.celery_optional and self._fastapi_proc:
                            print(f"\nCelery worker exited (code {proc.returncode}). "
                                  "Continuing with FastAPI only...")
                            self._processes.remove(proc)
                            self._celery_proc = None
                            self._celery_healthy = False
                        elif is_fastapi:
                            print(f"\nFastAPI server exited with code {proc.returncode}")
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

    def _validate_config(self):
        """Validate that the config can be loaded from the JSON file."""
        if self.working_dir not in sys.path:
            sys.path.insert(0, self.working_dir)

        try:
            from assistant_gateway.runner.parse_config import parse_config
            result = parse_config(self.config_path)

            if result.gateway_config.clauq_btm is None:
                if self.celery_only:
                    raise ValueError(
                        "clauq_btm is not configured in the JSON config, "
                        "but --celery-only mode requires it."
                    )
                elif not self.fastapi_only:
                    print(
                        "Warning: clauq_btm is not configured in the JSON config.\n"
                        "         Celery worker will not be started.\n"
                        "         Background tasks will not work (sync-only mode)."
                    )
                    self.fastapi_only = True

            if result.app is None and not self.celery_only:
                raise ValueError(
                    "rest_api is not configured in the JSON config, "
                    "but a FastAPI app is required unless --celery-only is set."
                )
        except Exception as e:
            print(f"Error loading config: {e}")
            raise


def main(args: Optional[List[str]] = None):
    """Main entry point for the launcher."""
    parser = argparse.ArgumentParser(
        prog="assistant-gateway",
        description="Launch FastAPI and Celery worker for the Assistant Gateway",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with both FastAPI and Celery (graceful fallback if Celery fails)
  python -m assistant_gateway.runner --config myapp.json

  # Require Celery to start (fail if Redis is unavailable)
  python -m assistant_gateway.runner --config myapp.json --celery-required

  # Run only FastAPI (for development without background tasks)
  python -m assistant_gateway.runner --config myapp.json --fastapi-only

  # Run only Celery worker (useful when scaling workers)
  python -m assistant_gateway.runner --config myapp.json --celery-only

  # Custom ports and settings
  python -m assistant_gateway.runner \\
      --config myapp.json \\
      --port 9000 \\
      --celery-pool threads \\
      --celery-concurrency 4

Graceful Degradation:
  By default, if Celery fails to start (e.g., Redis not running), the runner
  will continue with FastAPI only. Sync tasks will work, but background/async
  tasks will not be available. Use --celery-required to disable this behavior.
        """,
    )

    parser.add_argument(
        "--config", "-c",
        required=True,
        help="Path to the JSON gateway config file.",
    )

    # FastAPI options
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind FastAPI server (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=8000,
        help="Port for FastAPI server (default: 8000)",
    )
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable auto-reload for FastAPI",
    )

    # Celery options
    parser.add_argument(
        "--celery-pool",
        choices=["prefork", "solo", "threads", "gevent", "eventlet"],
        help="Celery worker pool type (default: solo on Windows, prefork otherwise)",
    )
    parser.add_argument(
        "--celery-concurrency",
        type=int,
        help="Number of Celery worker processes/threads",
    )
    parser.add_argument(
        "--celery-loglevel",
        default="info",
        choices=["debug", "info", "warning", "error", "critical"],
        help="Celery log level (default: info)",
    )

    # Mode options
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--fastapi-only",
        action="store_true",
        help="Only start FastAPI server (no Celery worker)",
    )
    mode_group.add_argument(
        "--celery-only",
        action="store_true",
        help="Only start Celery worker (no FastAPI server)",
    )

    parser.add_argument(
        "--celery-required",
        action="store_true",
        help="Fail if Celery worker cannot start (default: graceful fallback to FastAPI-only)",
    )

    parser.add_argument(
        "--working-dir", "-w",
        help="Working directory for both processes (default: current directory)",
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
        fastapi_only=parsed.fastapi_only,
        celery_only=parsed.celery_only,
        celery_optional=not parsed.celery_required,
    )

    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
