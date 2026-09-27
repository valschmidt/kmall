#!/usr/bin/env bash
#
# Copyright (c) 2026, Val Schmidt, University of New Hampshire
# SPDX-License-Identifier: BSD-2-Clause
# See the LICENSE file at the top of this repository for the full text.
#
# Build pykmall and upload it to PyPI, then tag the release in git. With
# --test, upload to TestPyPI instead, then install the upload into a fresh
# virtual environment and test it there.
# Run ./release.sh -h for usage and the full release procedure.

# Treat unset variables and failures anywhere in a pipeline as errors.
# Not -e: each step checks its own result and calls fail() with a message.
set -uo pipefail

usage() {
    cat <<'EOF'
Usage: ./release.sh [--test [--skip-upload] | -h]

Build pykmall and upload it to PyPI, then tag the release in git.

Options:
  (none)         Build, check, upload to PyPI, then create the tag vX.Y.Z
                 and push master and the tag to origin.
  --test         Build, check, and upload to TestPyPI; then install that
                 upload into a fresh virtual environment and test the
                 installed package there (see below). No tag.
  --test --skip-upload
                 Don't build or upload; rerun the install-and-test checks
                 on the version already on TestPyPI that matches setup.py.
  -h, --help     Show this help and exit.

The version released is whatever setup.py declares. The script must be
run on the master branch, refuses to run with uncommitted changes, and
asks for confirmation before uploading anything.

After a TestPyPI upload, --test checks the installed package, not the
source tree: that KMALL imports from the fresh environment at the right
version, that the kmall and kmall.py commands run, and (if the sample
file data/0004_20200917_014959_HiResPhase_subset.kmall exists) that
'kmall -n -ii 1' writes non-empty NADIR and PingInfo CSV files from it.

Release procedure:
  1. Make changes on master and commit them; repeat as needed.
  2. Edit the version in setup.py to X.Y.Z, and commit that change on
     its own. Do not tag it; this script creates the tag.
  3. ./release.sh --test
  4. Optionally, look over the project page on test.pypi.org.
  5. ./release.sh

If step 3 fails after uploading, fix the problem and commit. TestPyPI
never accepts the same version twice, so test the fix under a
pre-release version: set setup.py to X.Y.Zrc1 (then rc2, and so on),
commit, and repeat step 3 with that version. Once it passes, set
setup.py back to X.Y.Z, commit, and run step 5. TestPyPI and PyPI are
separate indexes, so X.Y.Z having been used on TestPyPI does not stop
it going to PyPI.

Environment:
  PYTHON   The interpreter used to create the virtual environments
           (default: python3).

Credentials: uploads run non-interactively, so they must be configured
beforehand, either in ~/.pypirc ([pypi] and [testpypi] sections, listed
under index-servers, each with username __token__ and an API token as
the password), or in the TWINE_USERNAME and TWINE_PASSWORD environment
variables.

On success the temporary work directory is removed. On failure it is
kept, and its location printed, so the log, build, and environments can
be inspected.
EOF
}

# --- Arguments -------------------------------------------------------------

# Choose the upload target: PyPI by default, TestPyPI with --test.
repository=pypi
skip_upload=0
case "${1:-}" in
    "")        ;;
    --test)    repository=testpypi ;;
    -h|--help) usage; exit 0 ;;
    *)         usage >&2; exit 2 ;;
esac
case "${2:-}" in
    "")            ;;
    --skip-upload) [[ "$repository" == testpypi ]] || { usage >&2; exit 2; }
                   skip_upload=1 ;;
    *)             usage >&2; exit 2 ;;
esac
[[ $# -le 2 ]] || { usage >&2; exit 2; }

PYTHON="${PYTHON:-python3}"

# Run from the repository root, wherever the script is invoked from.
REPO="$(cd "$(dirname "$0")" && pwd -P)"
cd "$REPO" || exit 1
TESTDATA="$REPO/data/0004_20200917_014959_HiResPhase_subset.kmall"

# --- Helpers ---------------------------------------------------------------

# Every step's output goes to a log in a temporary work directory, which is
# kept on failure. Resolve its path fully (macOS TMPDIR ends in "/" and /var
# is a symlink to /private/var) so it compares cleanly with the paths
# Python reports.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/kmall-release.XXXXXX")" || { echo "FAIL: mktemp failed" >&2; exit 1; }
WORK="$(cd "$WORK" && pwd -P)"
LOG="$WORK/log.txt"
TOOLS="$WORK/tools-venv"
TESTENV="$WORK/test-venv"
DIST="$WORK/dist"
OUT="$WORK/output"

step() {
    echo "==> $1"
    echo "==> $1" >> "$LOG"
}

fail() {
    echo
    echo "FAIL: $1"
    # A failure before any step ran (a pre-flight check) leaves nothing
    # worth keeping.
    if [[ ! -s "$LOG" ]]; then
        rm -rf "$WORK"
        exit 1
    fi
    echo "  Last lines of the log:"
    tail -n 20 "$LOG" | sed 's/^/    /'
    echo
    echo "  Full log:        $LOG"
    echo "  Kept for review: $WORK"
    exit 1
}

# Run a command, sending its output to the log.
run() {
    "$@" >> "$LOG" 2>&1
}

# --- Checks before anything is built ---------------------------------------

# setup.py is the single source of the package name and version.
NAME="$(sed -nE 's/^[[:space:]]*name[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' setup.py)"
VERSION="$(sed -nE 's/^[[:space:]]*version[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' setup.py)"
[[ -n "$NAME" && -n "$VERSION" ]] || fail "could not read name and version from setup.py"
TAG="v$VERSION"

if [[ "$skip_upload" -eq 1 ]]; then
    echo "Testing $NAME $VERSION already on TestPyPI (work dir: $WORK)"
else
    echo "Releasing $NAME $VERSION to $repository (work dir: $WORK)"
fi

# The tag is created on the checked-out commit but only master is pushed,
# so releasing from any other branch would tag a commit not on master.
branch="$(git branch --show-current)"
[[ "$branch" == master ]] || fail "on branch '$branch'; releases are made from master"

# Only release committed code, so the uploaded package matches the tag.
[[ -z "$(git status --porcelain)" ]] || fail "working tree is not clean; commit or stash first"

# PyPI never accepts the same version twice, so an existing tag means the
# version in setup.py has not been bumped since the last release.
if [[ "$repository" == pypi ]] && git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
    fail "tag $TAG already exists; bump the version in setup.py first"
fi

# The upload must be confirmed at a prompt, which needs a terminal to read
# from. Without one the answer would be empty and read as "no", so stop
# now rather than after the build.
if [[ "$skip_upload" -eq 0 && ! -t 0 ]]; then
    fail "no terminal to confirm the upload from; run this from a terminal"
fi

# --- Build and upload ------------------------------------------------------

if [[ "$skip_upload" -eq 0 ]]; then
    # Build and upload with tools from their own environment, so the result
    # doesn't depend on whatever happens to be installed in $PYTHON.
    step "Creating build tools environment"
    run "$PYTHON" -m venv "$TOOLS" || fail "could not create build tools venv with $PYTHON"
    run "$TOOLS/bin/pip" install --upgrade pip build twine || fail "could not install build and twine"

    # Build into the work directory, so no stale files from an earlier
    # build in the repository's dist/ can be uploaded by mistake.
    step "Building sdist and wheel"
    run "$TOOLS/bin/python" -m build --outdir "$DIST" "$REPO" || fail "package build failed"

    # Validate the package metadata, including that the README renders.
    run "$TOOLS/bin/twine" check "$DIST"/* || fail "twine check failed on the built files"

    # Uploads cannot be undone, so confirm before sending anything.
    echo "    Built: $(cd "$DIST" && echo *)"
    read -r -p "Upload $NAME $VERSION to $repository? [y/N] " answer
    [[ "$answer" == [yY] ]] || fail "upload declined"

    step "Uploading to $repository"
    if ! run "$TOOLS/bin/twine" upload --non-interactive --repository "$repository" "$DIST"/*; then
        if grep -qi "File already exists" "$LOG"; then
            fail "$NAME $VERSION is already on $repository. Bump the version in setup.py$( \
                [[ "$repository" == testpypi ]] && echo ", or rerun with --test --skip-upload to test the existing upload")"
        fi
        fail "upload to $repository failed (check the credentials described in -h)"
    fi
fi

# --- Real release: tag and push --------------------------------------------

if [[ "$repository" == pypi ]]; then
    step "Tagging $TAG and pushing master and the tag to origin"
    run git tag "$TAG" || fail "$NAME $VERSION is on PyPI, but creating tag $TAG failed; finish with: git tag $TAG && git push origin master $TAG"
    run git push origin master "$TAG" \
        || fail "$NAME $VERSION is on PyPI, but the push failed; finish with: git push origin master $TAG"
    rm -rf "$WORK"
    echo
    echo "PASS: released $NAME $VERSION to PyPI and pushed tag $TAG."
    exit 0
fi

# --- TestPyPI: install the upload into a fresh environment and test it -----

# TestPyPI can take a little while before a new release can be installed.
step "Waiting for $NAME $VERSION to appear on TestPyPI"
for i in $(seq 1 24); do
    curl -sf -o /dev/null "https://test.pypi.org/pypi/$NAME/$VERSION/json" && break
    [[ "$i" -eq 24 ]] && fail "$NAME $VERSION did not appear on TestPyPI within 2 minutes"
    sleep 5
done

step "Creating test environment"
run "$PYTHON" -m venv "$TESTENV" || fail "could not create test venv with $PYTHON"

# Dependencies (pandas, numpy, pyproj, scipy) come from the real PyPI,
# since TestPyPI's copies are often missing or stale. The simple index can
# lag the JSON API checked above, so retry for a while.
step "Installing $NAME==$VERSION from TestPyPI"
installed=0
for i in 1 2 3 4 5 6; do
    if run "$TESTENV/bin/pip" install --no-cache-dir \
            -i https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple/ \
            "$NAME==$VERSION"; then
        installed=1
        break
    fi
    sleep 10
done
[[ "$installed" -eq 1 ]] || fail "could not install $NAME==$VERSION from TestPyPI"

# Work from inside the work directory from here on, so the KMALL package in
# the repository can't be imported by mistake instead of the installed one.
cd "$WORK" || fail "could not change to $WORK"

step "Checking the installed package is the TestPyPI one"
LOCATION="$("$TESTENV/bin/python" -c 'import KMALL, os; print(os.path.realpath(os.path.dirname(KMALL.__file__)))' 2>> "$LOG")" \
    || fail "'import KMALL' failed in the test environment"
case "$LOCATION" in
    "$TESTENV"/*) ;;
    *) fail "KMALL was imported from $LOCATION, not from the test environment" ;;
esac
GOT="$("$TESTENV/bin/pip" show "$NAME" 2>> "$LOG" | sed -n 's/^Version: //p')"
[[ "$GOT" == "$VERSION" ]] || fail "installed $NAME version is '$GOT', expected '$VERSION'"

# The console-script entry points declared in setup.py.
step "Running 'kmall -h' and 'kmall.py -h'"
run "$TESTENV/bin/kmall" -h || fail "'kmall -h' failed"
run "$TESTENV/bin/kmall.py" -h || fail "'kmall.py -h' failed"

if [[ -f "$TESTDATA" ]]; then
    step "Extracting nadir depth and ping info from $(basename "$TESTDATA")"
    mkdir -p "$OUT"
    run "$TESTENV/bin/kmall" -f "$TESTDATA" -n -ii 1 -o "$OUT" \
        || fail "'kmall -n -ii 1' failed on $TESTDATA"
    NADIR="$(ls "$OUT"/NADIR_*.csv 2>/dev/null | head -n 1)"
    [[ -n "$NADIR" ]] || fail "no NADIR_*.csv file was written"
    ROWS=$(($(wc -l < "$NADIR") - 1))
    [[ "$ROWS" -gt 0 ]] || fail "$(basename "$NADIR") has no data rows"
    ls "$OUT"/PingInfo_*.csv > /dev/null 2>&1 || fail "no PingInfo_*.csv file was written"
    echo "    $(basename "$NADIR"): $ROWS rows"
else
    step "Skipping sample-file check: $TESTDATA not found"
fi

cd "$REPO" || true
rm -rf "$WORK"
echo
echo "PASS: $NAME $VERSION installs from TestPyPI and runs. Temporary files removed."
