import sys
import time
import subprocess
import signal
import logging
import os
from aura_core import setup_logging

# Configure logging
setup_logging()
logger = logging.getLogger("ProcessManager")

SCRIPTS = ["keep_alive.py", "aura_apex_supreme.py", "aura_curator.py"]
PROCESSES = {}
WEB_PROCESS = None

def start_process(script_name):
    """Start a python script as a subprocess."""
    try:
        logger.info(f"Starting {script_name}...")
        # Start the process independently with a fresh environment
        env = os.environ.copy()
        p = subprocess.Popen([sys.executable, script_name], env=env)
        PROCESSES[script_name] = p
        return p
    except Exception as e:
        logger.error(f"Failed to start {script_name}: {e}")
        return None

def stop_process(script_name):
    """Gracefully stop a subprocess."""
    p = PROCESSES.get(script_name)
    if p:
        try:
            logger.info(f"Stopping {script_name} (PID: {p.pid})...")
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning(f"{script_name} did not terminate gracefully. Killing...")
                p.kill()
        except Exception as e:
            logger.error(f"Error stopping {script_name}: {e}")
        finally:
            if script_name in PROCESSES:
                del PROCESSES[script_name]

def signal_handler(sig, frame):
    """Handle termination signals."""
    logger.info("Shutdown signal received. Cleaning up...")
    for script in list(PROCESSES.keys()):
        stop_process(script)
    sys.exit(0)

def monitor():
    """Main monitor loop."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Process Manager Started.")

    # Initial start
    for script in SCRIPTS:
        start_process(script)
        time.sleep(2) # Delay between starts

    while True:
        try:
            for script in SCRIPTS:
                p = PROCESSES.get(script)
                # Check if process is dead (poll returns exit code if dead, None if alive)
                if p is None or p.poll() is not None:
                    exit_code = p.poll() if p else "None"
                    logger.warning(f"{script} is not running (Exit Code: {exit_code}). Restarting...")
                    start_process(script)
                    time.sleep(2) # Delay between restarts
            
            time.sleep(10)
        except KeyboardInterrupt:
            signal_handler(signal.SIGINT, None)
        except Exception as e:
            logger.error(f"Monitor loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    monitor()
