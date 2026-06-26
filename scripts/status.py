#!/usr/bin/env python3
"""Progress overview over ga_index_state + typed tables."""
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.cli import print_status  # noqa: E402
from src.utils.logger import setup_logger  # noqa: E402

if __name__ == "__main__":
    setup_logger()
    print_status()
