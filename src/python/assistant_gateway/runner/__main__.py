"""
Entry point for running the gateway runner as a module.

Usage:
    python -m assistant_gateway.runner --help
    python -m assistant_gateway.runner --config myapp.config:build_config --app myapp.api:app
"""

import sys
from assistant_gateway.runner.cli import main

if __name__ == "__main__":
    sys.exit(main())
