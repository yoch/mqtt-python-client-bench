#!/usr/bin/env bash
# Retire every branch except main, keeping each tip reachable as an archive/* tag.
#
# Every branch listed here is either squash-merged into main (PRs #9-#35),
# superseded by the rc17 adapter commits cherry-picked onto main, or carries
# only v1 A/B result blobs. Nothing is lost: the tags point at the exact tips.
#
# Dry run by default. Pass --apply to push the tags and delete the branches.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
apply=0
[[ "${1:-}" == "--apply" ]] && apply=1

run() {
  if [[ $apply -eq 1 ]]; then "$@"; else echo "+ $*"; fi
}

git fetch --prune origin

mapfile -t remote_branches < <(git for-each-ref --format='%(refname:short)' refs/remotes/origin \
  | grep -vE '^origin(/HEAD|/main)?$' | sed 's#^origin/##')
mapfile -t local_branches < <(git for-each-ref --format='%(refname:short)' refs/heads | grep -vxE 'main|v2')

for b in "${remote_branches[@]}" "${local_branches[@]}"; do
  ref="$b"
  git show-ref --verify --quiet "refs/remotes/origin/$b" && ref="origin/$b"
  run git tag -f "archive/$b" "$ref"
done

# GitHub rejects a push that updates too many refs at once, so one tag per push.
for b in "${remote_branches[@]}"; do
  run git push origin "refs/tags/archive/$b:refs/tags/archive/$b"
done
# A local-only branch may carry files over GitHub's 100 MB limit: its tag then
# stays local, and rewrite_results_history.sh bundles it before the rewrite.
for b in "${local_branches[@]}"; do
  run git push origin "refs/tags/archive/$b:refs/tags/archive/$b" \
    || echo "archive/$b kept local only" >&2
done

for b in "${remote_branches[@]}"; do
  run git push origin --delete "$b"
done
for b in "${local_branches[@]}"; do
  run git branch -D "$b"
done
