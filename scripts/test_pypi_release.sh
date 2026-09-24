#!/usr/bin/env bash
#
# Build the package defined in setup.py, upload it to TestPyPI, install it into a fresh
# virtual environment and check that it works.
#
# Usage: scripts/test_pypi_release.sh [--skip-upload]
#
#   --skip-upload   Don't build or upload; test the version already on
#                   TestPyPI that matches setup.py.
#
# Requires a [testpypi] entry in ~/.pypirc (listed under index-servers).
# Set PYTHON to choose the interpreter (default: python3).
#
# On success the temporary directory is removed. On failure it is kept so
# the logs, build and environment can be inspected.

set -uo pipefail

PYTHON="${PYTHON:-python3}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
TESTDATA="$REPO/data/0004_20200917_014959_HiResPhase_subset.kmall"

SKIP_UPLOAD=0
case "${1:-}" in
    --skip-upload) SKIP_UPLOAD=1 ;;
    "") ;;
    *) echo "Usage: $0 [--skip-upload]" >&2; exit 2 ;;
esac

NAME="$(sed -nE 's/^[[:space:]]*name[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' "$REPO/setup.py")"
VERSION="$(sed -nE 's/^[[:space:]]*version[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' "$REPO/setup.py")"
if [ -z "$NAME" ] || [ -z "$VERSION" ]; then
    echo "FAIL: could not read name and version from setup.py" >&2
    exit 1
fi

# Resolve the path fully (macOS TMPDIR ends in "/" and /var is a symlink to
# /private/var) so it can be compared with the path Python reports.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/kmall-release-test.XXXXXX")" || { echo "FAIL: mktemp failed" >&2; exit 1; }
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

echo "Testing $NAME $VERSION via TestPyPI (work dir: $WORK)"

if [ "$SKIP_UPLOAD" -eq 0 ]; then
    step "Creating build tools environment"
    run "$PYTHON" -m venv "$TOOLS" || fail "could not create build tools venv with $PYTHON"
    run "$TOOLS/bin/pip" install --upgrade pip build twine || fail "could not install build and twine"

    step "Building sdist and wheel"
    run "$TOOLS/bin/python" -m build --outdir "$DIST" "$REPO" || fail "package build failed"
    run "$TOOLS/bin/twine" check "$DIST"/* || fail "twine check failed on the built files"

    step "Uploading to TestPyPI"
    if ! run "$TOOLS/bin/twine" upload --repository testpypi "$DIST"/*; then
        if grep -qi "File already exists" "$LOG"; then
            fail "$NAME $VERSION is already on TestPyPI. Bump the version in setup.py, or rerun with --skip-upload to test the existing upload"
        fi
        fail "upload to TestPyPI failed (check the [testpypi] entry in ~/.pypirc)"
    fi

    # TestPyPI can take a little while before a new release can be installed.
    step "Waiting for $NAME $VERSION to appear on TestPyPI"
    for i in $(seq 1 24); do
        if curl -sf -o /dev/null "https://test.pypi.org/pypi/$NAME/$VERSION/json"; then
            break
        fi
        [ "$i" -eq 24 ] && fail "$NAME $VERSION did not appear on TestPyPI within 2 minutes"
        sleep 5
    done
fi

step "Creating test environment"
run "$PYTHON" -m venv "$TESTENV" || fail "could not create test venv with $PYTHON"

step "Installing $NAME==$VERSION from TestPyPI"
installed=0
for i in 1 2 3 4 5 6; do
    if run "$TESTENV/bin/pip" install --no-cache-dir \
            -i https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple \
            "$NAME==$VERSION"; then
        installed=1
        break
    fi
    sleep 10
done
[ "$installed" -eq 1 ] || fail "could not install $NAME==$VERSION from TestPyPI"

# Run the tests from inside the work directory so that the KMALL package in
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
[ "$GOT" = "$VERSION" ] || fail "installed $NAME version is '$GOT', expected '$VERSION'"

step "Running 'kmall -h' and 'kmall.py -h'"
run "$TESTENV/bin/kmall" -h || fail "'kmall -h' failed"
run "$TESTENV/bin/kmall.py" -h || fail "'kmall.py -h' failed"

if [ -f "$TESTDATA" ]; then
    step "Extracting nadir depth and ping info from $(basename "$TESTDATA")"
    mkdir -p "$OUT"
    run "$TESTENV/bin/kmall" -f "$TESTDATA" -n -ii 1 -o "$OUT" \
        || fail "kmall -n -ii 1 failed on the test file"
    NADIR="$(ls "$OUT"/NADIR_*.csv 2>/dev/null | head -n 1)"
    [ -n "$NADIR" ] || fail "no NADIR_*.csv file was written"
    ROWS=$(($(wc -l < "$NADIR") - 1))
    [ "$ROWS" -gt 0 ] || fail "$(basename "$NADIR") has no data rows"
    ls "$OUT"/PingInfo_*.csv > /dev/null 2>&1 || fail "no PingInfo_*.csv file was written"
    echo "    $(basename "$NADIR"): $ROWS rows"
else
    step "Skipping data test: $TESTDATA not found"
fi

cd "$REPO" || true
rm -rf "$WORK"
echo
echo "PASS: $NAME $VERSION installs from TestPyPI and runs. Temporary files removed."
