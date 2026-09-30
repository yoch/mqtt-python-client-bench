#!/usr/bin/env bash
# Drop the v1 result corpus from git history (1.9 GB of the 1.9 GB pack).
#
# Destructive: every commit id changes and collaborators must re-clone. Run it
# only after `scripts/cleanup_branches.sh --apply`, so no branch still points
# at the old history.
#
#   1. writes a full bundle of the current history (branches + tags) next to
#      the checkout, so the v1 corpus stays recoverable byte for byte;
#   2. removes every path under results/ except results/v2/ from all commits;
#   3. prints the force-push commands instead of running them.
#
# Needs git-filter-repo (pip install git-filter-repo).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
if ! command -v git-filter-repo >/dev/null; then
  echo "git-filter-repo not found (pip install git-filter-repo)" >&2
  exit 1
fi
if [[ "${1:-}" != "--apply" ]]; then
  echo "dry run: pass --apply to bundle and rewrite" >&2
  git rev-list --objects --all \
    | git cat-file --batch-check='%(objecttype) %(objectsize:disk) %(rest)' \
    | awk '$1=="blob" && $3 ~ /^results\// && $3 !~ /^results\/v2\// {s+=$2} END {printf "would drop %.1f MB of packed history\n", s/1e6}'
  exit 0
fi

bundle="../$(basename "$PWD")-history-$(date +%Y%m%d).bundle"
git bundle create "$bundle" --all
echo "backup: $bundle"

# --invert-paths would apply to results/v2/ as well, so filter by callback.
git filter-repo --force --filename-callback '
if filename.startswith(b"results/") and not filename.startswith(b"results/v2/"):
    return None
return filename'

git gc --prune=now --aggressive
du -sh .git
cat <<'EOF'
Review, then publish the rewritten history:
  git remote add origin <url>        # filter-repo removes the remote
  git push --force origin main
  git push --force origin 'refs/tags/*'
EOF
