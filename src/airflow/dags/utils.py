# utils.py
import subprocess
import sys

def run_subprocess_with_logging(command):
    # Start the subprocess and stream output directly to Airflow logs
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    ) as proc:
        # Stream stdout and stderr to Airflow logs
        for line in proc.stdout:
            sys.stdout.write(line)  # Write stdout to Airflow log
        for line in proc.stderr:
            sys.stderr.write(line)  # Write stderr to Airflow log

    # Check for errors
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, proc.args)
