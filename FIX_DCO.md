# Fix DCO Sign-off Issue

## Problem
Your commit `96a1ca8` is missing the required `Signed-off-by` line.

## Solution: Amend and Sign-off the Commit

### Step 1: Amend the last commit with sign-off
```bash
cd /Users/purushotham.p/personal/ray
git commit --amend --signoff --no-edit
```

This will add the `Signed-off-by: pushpavanthar <pushpavanthar@gmail.com>` line to your commit.

### Step 2: Verify the sign-off was added
```bash
git log -1 --format=full
```

You should see:
```
Signed-off-by: pushpavanthar <pushpavanthar@gmail.com>
```

### Step 3: Force push to your branch
```bash
# If your branch is named 'master' (replace with your actual branch name if different)
git push --force-with-lease origin master

# Or if you're on a feature branch:
# git push --force-with-lease origin YOUR_BRANCH_NAME
```

⚠️ **Important:** Use `--force-with-lease` instead of `--force` for safety. It will fail if someone else pushed changes to the branch.

## Alternative: If you have multiple commits to sign-off

If you have multiple commits that need sign-off:

```bash
# Rebase the last N commits (replace N with number of commits)
git rebase HEAD~N --signoff

# Then force push
git push --force-with-lease origin YOUR_BRANCH_NAME
```

## Verification

After pushing, check your PR on GitHub. The DCO check should pass and show:
✅ All commits have been signed off

## For Future Commits

Always use the `-s` flag when committing:
```bash
git commit -s -m "Your commit message"
```

This automatically adds the `Signed-off-by` line.

