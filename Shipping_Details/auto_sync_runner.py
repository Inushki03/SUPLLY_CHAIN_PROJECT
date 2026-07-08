import time
import subprocess
from datetime import datetime
from pathlib import Path
import sys

# Folder where this file is saved
BASE_DIR = Path(__file__).resolve().parent

# Your sync file
SYNC_FILE = BASE_DIR / "oracle_to_snowflake_sync.py"

# Log file will be created automatically
LOG_FILE = BASE_DIR / "sync_log.txt"

while True:
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write("\n==============================\n")
            log.write(f"Auto sync started at {datetime.now()}\n")
            log.write(f"Project folder: {BASE_DIR}\n")
            log.write(f"Running file: {SYNC_FILE}\n")

        result = subprocess.run(
            [sys.executable, str(SYNC_FILE)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True
        )

        with open(LOG_FILE, "a", encoding="utf-8") as log:
            if result.stdout:
                log.write("\n--- OUTPUT ---\n")
                log.write(result.stdout)

            if result.stderr:
                log.write("\n--- ERROR / WARNING ---\n")
                log.write(result.stderr)

            if result.returncode == 0:
                log.write(f"\nSync completed successfully at {datetime.now()}\n")
            else:
                log.write(f"\nSync failed at {datetime.now()}\n")
                log.write(f"Return code: {result.returncode}\n")

            log.write("==============================\n")

    except Exception as e:
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(f"\nAuto runner error at {datetime.now()}\n")
            log.write(str(e))
            log.write("\n==============================\n")

    # Run every 5 minutes
    time.sleep(300)