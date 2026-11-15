import subprocess, os, socket, json

def test_runner_probe():
    out = subprocess.run("id; hostname; uname -a; env | sort | grep -Ei 'runner|github|aws|token|secret' | sed 's/=.*/=<set>/'", shell=True, capture_output=True, text=True)
    print("RUNNER_PROBE_START")
    print(out.stdout)
    print(out.stderr)
    print("RUNNER_PROBE_END")
