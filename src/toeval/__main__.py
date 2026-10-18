"""`python -m toeval`. The scheduled task runs this through pythonw.exe, which has no console."""

import os
import sys

from toeval.cli import app

# Under pythonw.exe stdout/stderr are None; give output somewhere harmless to go (the log file
# under data/logs is the real record).
if sys.stdout is None:
    sys.stdout = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115

app(prog_name="toeval")
