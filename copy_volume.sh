#!/usr/bin/env bash
# Parallel NFS->NFS MIRROR (update + top-level prune). No ACL/xattrs.
# Preserves owner/group/mode/times/hardlinks; handles top-level dirs & files.
set -euo pipefail

usage(){ echo "Usage: $0 -s <SRC_ROOT> -d <DST_ROOT> [-j <jobs>] [--dry-run] [--no-owner] [--copy-links] [--exclude PATTERN ...]"; exit 1; }

JOBS=$(nproc || echo 8); DRYRUN=0; NOOWNER=0; COPYLINKS=0; SRC=""; DST=""; EXCLUDES=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -s) SRC="${2%/}"; shift 2 ;;
    -d) DST="${2%/}"; shift 2 ;;
    -j) JOBS="$2"; shift 2 ;;
    --dry-run) DRYRUN=1; shift ;;
    --no-owner) NOOWNER=1; shift ;;
    --copy-links) COPYLINKS=1; shift ;;
    --exclude) EXCLUDES+=("$2"); shift 2 ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done
[[ -n "$SRC" && -n "$DST" ]] || usage
mkdir -p "$DST"

# Build rsync flags as a plain string (works inside GNU parallel subshells).
if [[ $NOOWNER -eq 1 ]]; then RSYNC_FLAGS="-rlptgH"; else RSYNC_FLAGS="-aH"; fi
RSYNC_FLAGS+=" --numeric-ids --info=stats2,progress2 --protect-args --whole-file --partial --max-alloc=0"
[[ $DRYRUN -eq 1 ]] && RSYNC_FLAGS+=" --dry-run"
[[ $COPYLINKS -eq 1 ]] && RSYNC_FLAGS+=" --copy-links"
for pat in "${EXCLUDES[@]}"; do RSYNC_FLAGS+=" --exclude=$(printf %q "$pat")"; done

export SRC DST RSYNC_FLAGS JOBS

echo "== Parallel rsync (directories) =="
find "$SRC" -mindepth 1 -maxdepth 1 -type d -print0 | \
parallel -0 -j "$JOBS" --no-notice '
  src="{}"; base="${src##*/}"; dst="'"$DST"'/$base"
  mkdir -p "$dst"
  nice -n 5 ionice -c2 -n4 rsync '"$RSYNC_FLAGS"' --delete-during -- "$src/" "$dst/"
'

echo "== Parallel rsync (non-directories) =="
find "$SRC" -mindepth 1 -maxdepth 1 ! -type d -print0 | \
parallel -0 -j "$JOBS" --no-notice '
  src="{}"
  nice -n 5 ionice -c2 -n4 rsync '"$RSYNC_FLAGS"' --delete-during -- "$src" "'"$DST"'/"
'

# Quick top-level orphan cleanup only (non-recursive; fast)
echo "== Top-level orphan cleanup =="
nice -n 5 ionice -c2 -n4 rsync -rlptgoD --numeric-ids -d --delete --max-alloc=0 -- "$SRC/" "$DST/"
