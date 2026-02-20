#!/usr/bin/env python3
# Compare major dependencies that changed
import re

def parse_requirements(file):
    deps = {}
    with open(file) as f:
        for line in f:
            line = line.strip()
            if '==' in line and not line.startswith('#'):
                name = line.split('==')[0]
                version = line.split('==')[1]
                deps[name] = version
    return deps

old_deps = parse_requirements('python/requirements_compiled_repo_original.txt')
new_deps = parse_requirements('python/requirements_compiled.txt')

# Find major version changes
major_changes = []
for pkg in old_deps:
    if pkg in new_deps:
        old_ver = old_deps[pkg]
        new_ver = new_deps[pkg]
        if old_ver != new_ver:
            try:
                old_major = int(old_ver.split('.')[0])
                new_major = int(new_ver.split('.')[0])
                old_minor = int(old_ver.split('.')[1]) if '.' in old_ver else 0
                new_minor = int(new_ver.split('.')[1]) if '.' in new_ver else 0
                
                # Flag major changes or significant minor changes
                if old_major != new_major or abs(new_minor - old_minor) > 5:
                    major_changes.append((pkg, old_ver, new_ver))
            except:
                major_changes.append((pkg, old_ver, new_ver))

print('Major version changes that could cause resolution issues:')
for pkg, old, new in sorted(major_changes)[:15]:  # Show first 15
    print(f'  {pkg}: {old} -> {new}')

# Focus on key packages
key_packages = ['torch', 'pytorch', 'numpy', 'scipy', 'scikit-learn', 'botorch', 'gpytorch', 'ax-platform']
print('\nKey package changes:')
for pkg in key_packages:
    if pkg in old_deps and pkg in new_deps:
        if old_deps[pkg] != new_deps[pkg]:
            print(f'  {pkg}: {old_deps[pkg]} -> {new_deps[pkg]}')