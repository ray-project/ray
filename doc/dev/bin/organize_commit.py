"""
After creating commit.txt using get_contributor.py,
you can organize them using this script.

EX)
# Create commits.txt using `get_contributor.py`.
Python organize_commit.py ../commits.txt
# This will produce organized_commit.txt.
"""

from collections import defaultdict

NO_CATEGORY = "[NO_CATEGORY]"

def get_category(line):
    if line[0] == '[':
        return line.split(']')[0] + ']'
    else:
        return NO_CATEGORY

commits = defaultdict(list)

with open("commits.txt") as file:
    for line in file.readlines():
        commits[get_category(line)].append(line.strip())

with open("organized_commit.txt", 'w') as file:
    for category, commit_msgs in commits.items():
        file.write(f'\n{category}\n')
        for commit_msg in commit_msgs:
            file.write(f'{commit_msg}\n')

