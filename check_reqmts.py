import subprocess

packages = subprocess.run(
    ['pip', 'list'],
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL
)
packages_list = packages.stdout.decode('utf-8').split('\n')
with open('requirements.txt') as fpr:
    reqmts_list = [(f"{r.split('>')[0]}", f"{r.split('=')[1].strip()}") for r in fpr]
max_len_pkg = max(map(len, [elem[0] for elem in reqmts_list]))
max_len_ver = max(map(len, [elem[1] for elem in reqmts_list]))
print(f"{'Required':{max_len_pkg+max_len_ver+3}s} | Installed")
print(f"{'--------':{max_len_pkg+max_len_ver+3}s} | ---------")
for pkg_name, pkg_ver in reqmts_list:
    print(f'{pkg_name:{max_len_pkg}s}   {pkg_ver:{max_len_ver}s} ', end='| ')
    match = ''
    for package in packages_list:
        if package.find(pkg_name) != -1:
            match = package
    if len(match):
        print(match)
    else:
        print()
