import os
import re
import subprocess 

dhall = re.compile(".*\..*\.dhall")

env = re.compile(".*\.env.dhall")

def is_dhall_file(file):
    m = dhall.match(file)
    if m is not None:
        if env.match(file):
            return None 
    return m

def to_yaml(file):
    parts = file.split('.')[:-1]
    yaml_file = '.'.join(parts) + '.yaml'
    run_dhall_to_yaml(file, yaml_file)

def run_dhall_to_yaml(in_file, out_file):
    out = subprocess.check_output(["dhall-to-yaml", "--file", in_file])
    with open(out_file, "wb") as f:
        f.write(out)


def main():
    files = [f for f in os.listdir('.') if os.path.isfile(f) and (is_dhall_file(f) is not None)]
    for f in files:
        to_yaml(f)

if __name__ == '__main__':
    main()
