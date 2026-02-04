"""
Command-line interface for the gateway runner.

Provides a unified CLI to launch both FastAPI and Celery worker
from a single command.
"""

from __future__ import annotations

import argparse
import os
import platform
import signal
import subprocess
import sys
from typing import List, Optional


def get_default_celery_pool() -> str:
    """Get the default Celery pool type based on the OS."""
    if platform.system() == "Windows":
        return "solo"  # prefork doesn't work on Windows
    return "prefork"


def build_fastapi_command(
    app_path: str,
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = True,
) -> List[str]:
    """Build the FastAPI dev server command."""
    cmd = [
        sys.executable, "-m", "fastapi", "dev",
        app_path.replace(":", ".").rsplit(".", 1)[0].replace(".", "/") + ".py",
        "--host", host,
        "--port", str(port),
    ]
    if not reload:
        cmd.append("--no-reload")
    return cmd


def build_fastapi_uvicorn_command(
    app_path: str,
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = True,
) -> List[str]:
    """Build the uvicorn command for FastAPI."""
    cmd = [
        sys.executable, "-m", "uvicorn",
        app_path,
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
    """

    def __init__(
        self,
        config_path: str,
        app_path: str,
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
    ):
        self.config_path = config_path
        self.app_path = app_path
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

        self._processes: List[subprocess.Popen] = []
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

        cmd = build_fastapi_uvicorn_command(
            app_path=self.app_path,
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
        return proc

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
            if proc.poll() is None:  # Process is still running
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    def run(self):
        """Start all processes and wait for them to complete."""
        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        # SIGTERM is not available on Windows
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            # Validate that the config can be loaded before starting processes
            print(f"Validating config from: {self.config_path}")
            self._validate_config()
            print("Config validated successfully!")

            # Start processes
            fastapi_proc = self._start_fastapi()
            celery_proc = self._start_celery()

            print("\n" + "=" * 60)
            print("Gateway Runner Started!")
            print("=" * 60)
            if fastapi_proc:
                print(f"  FastAPI: http://{self.fastapi_host}:{self.fastapi_port}")
            if celery_proc:
                print(f"  Celery:  Worker running with pool={self.celery_pool or get_default_celery_pool()}")
            print("=" * 60)
            print("Press Ctrl+C to stop all services\n")

            # Wait for any process to exit
            while not self._shutting_down:
                for proc in self._processes:
                    if proc.poll() is not None:
                        print(f"Process {proc.pid} exited with code {proc.returncode}")
                        if not self._shutting_down:
                            self._shutting_down = True
                            self.stop()
                            return proc.returncode

                # Brief sleep to avoid busy waiting
                import time
                time.sleep(0.5)

        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

        return 0

    def _validate_config(self):
        """Validate that the config can be loaded."""
        # Add working dir to path temporarily for validation
        if self.working_dir not in sys.path:
            sys.path.insert(0, self.working_dir)

        try:
            from assistant_gateway.runner.loader import load_config
            config = load_config(self.config_path)

            if config.clauq_btm is None:
                print(
                    "Warning: clauq_btm is not configured in GatewayConfig. "
                    "Background tasks will not work."
                )
        except Exception as e:
            print(f"Error loading config: {e}")
            raise


def main(args: Optional[List[str]] = None):
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="assistant-gateway",
        description="Launch FastAPI and Celery worker for the Assistant Gateway",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with both FastAPI and Celery
  python -m assistant_gateway.runner \\
      --config myapp.config:build_gateway_config \\
      --app myapp.api:app

  # Run only FastAPI (for development without background tasks)
  python -m assistant_gateway.runner \\
      --config myapp.config:build_gateway_config \\
      --app myapp.api:app \\
      --fastapi-only

  # Run only Celery worker (useful when scaling workers)
  python -m assistant_gateway.runner \\
      --config myapp.config:build_gateway_config \\
      --app myapp.api:app \\
      --celery-only

  # Custom ports and settings
  python -m assistant_gateway.runner \\
      --config myapp.config:build_gateway_config \\
      --app myapp.api:app \\
      --port 9000 \\
      --celery-pool threads \\
      --celery-concurrency 4
        """,
    )

    # Required arguments
    parser.add_argument(
        "--config", "-c",
        required=True,
        help="Module path to GatewayConfig (format: module.path:attribute). "
             "Can be a config instance or a callable that returns one.",
    )
    parser.add_argument(
        "--app", "-a",
        required=True,
        help="Module path to FastAPI app (format: module.path:app)",
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

    # Other options
    parser.add_argument(
        "--working-dir", "-w",
        help="Working directory for both processes (default: current directory)",
    )

    parsed = parser.parse_args(args)

    runner = GatewayRunner(
        config_path=parsed.config,
        app_path=parsed.app,
        working_dir=parsed.working_dir,
        fastapi_host=parsed.host,
        fastapi_port=parsed.port,
        fastapi_reload=not parsed.no_reload,
        celery_pool=parsed.celery_pool,
        celery_concurrency=parsed.celery_concurrency,
        celery_log_level=parsed.celery_loglevel,
        fastapi_only=parsed.fastapi_only,
        celery_only=parsed.celery_only,
    )

    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
