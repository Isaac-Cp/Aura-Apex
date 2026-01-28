import subprocess
import time
import sys

def start_process(cmd):
    try:
        return subprocess.Popen([sys.executable, cmd])
    except Exception:
        return None

def run():
    s = start_process("aura_apex_supreme.py")
    c = start_process("aura_curator.py")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        if s:
            s.kill()
        if c:
            c.kill()

if __name__ == "__main__":
    run()
