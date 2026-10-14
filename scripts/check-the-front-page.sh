#!/bin/sh
# check-the-front-page.sh -- clone this repository, follow README.md, and type the commands it
# publishes.
#
# Why it exists. Every other control here reads the tree: two gates sweep tracked files, commit
# messages, refs, metadata and the remote build; the suite exercises the modules; several rounds of
# review read the source and the prose. Nothing cloned the repository, followed the front page and
# ran what it says. That is why a published `until` loop that was a no-op on a restarted container,
# a readiness line nobody could tell apart from a container that had already exited, a `-q` claim
# one invocation refutes, and a first-run file nothing named all survived six rounds of reading:
# each is invisible to a reader and immediate to a user.
#
# What it does, and the one thing that makes it a gate rather than a test. It does not hold a copy
# of the published commands -- it EXTRACTS the fenced blocks from README.md and runs those, so the
# page and the check cannot drift apart. Where a command cannot run unchanged in a scratch
# directory (the port, the container's names) the substitution is declared in the table below, each
# with its anchor; a substitution whose anchor is no longer in the page fails the run, because that
# means the page changed under the check. Anything else in a block runs exactly as published.
#
# What it checks:
#   0. The tree it checks: a fresh clone of HEAD with the working tree copied over it, so an
#      uncommitted edit to the page is read here rather than only by CI after the push.
#   1. The quickstart's own setup block, run in a fresh clone: the venv and the dev install.
#   2. The quickstart's own run block: the server started as published, and `listening on` read
#      off its standard output, which is the line the page tells a reader to wait for.
#   3. The three `redis-cli` commands of the published transcript, compared against the transcript's
#      own replies rather than against what this script thinks they should be.
#   4. The suite, by the page's own invocation, and the ending the page promises.
#   5. The source-distribution block, the suite the distribution ships, and every file the
#      page links to being inside the tarball, since README.md is the long description.
#   6. The container block, verbatim, then a stop and a start over the same volume with the
#      keyspace checked -- only when CONTAINER=1, because a local run must not touch the owner's
#      daemon, and because the recipe creates an image, a container and a volume. CI sets it.
#
# Which arms have been demonstrated, by breaking something and watching this fail rather than by
# reasoning: the transcript publishing a reply the server does not give (FRONT PAGE DIRTY, naming
# the command, what came back and what the page publishes); the page no longer publishing the
# setup command this check runs, which is exit 2 and says the page changed under the check rather
# than quietly testing a copy; and the server no longer printing the line the page tells a reader
# to wait for. The snapshot arm is demonstrated differently -- the file is absent before the stop
# and present after it, measured -- because removing one call site does not kill it: there are
# three, and a mutation narrow enough to be interesting leaves the others standing.
# The working-tree arm is demonstrated by reproducing the regression that made it necessary:
# the `--ignore-snapshot` row deleted from README.md and left uncommitted, which this fails
# on and which the same check against the clone alone had passed.
# The link arm is demonstrated by taking docs/MEASUREMENTS.md out of MANIFEST.in, which
# leaves the page's own link dead in the distribution and is reported by name.
#
# Usage:  sh scripts/check-the-front-page.sh [--keep]
#         CONTAINER=1 sh scripts/check-the-front-page.sh      # adds step 6
# Exit 0 = the page's own commands did what the page says. Exit 1 = one did not, and it is named.
# Exit 2 = the check could not run (no git, no python3, a missing block, a stale substitution).

set -u
LC_ALL=C
export LC_ALL

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1
CONTAINER=${CONTAINER:-0}
PORT=${PORT:-}

REPO=$(cd "$(dirname "$0")/.." && pwd)
[ -r "$REPO/README.md" ] || { printf 'fail  no README.md beside %s\n' "$0"; exit 2; }
command -v git >/dev/null || { printf 'fail  no git\n'; exit 2; }
command -v python3 >/dev/null || { printf 'fail  no python3\n'; exit 2; }

WORK=$(mktemp -d) || { printf 'fail  mktemp -d failed\n'; exit 2; }
cleanup() {
    if [ "$KEEP" = 1 ]; then
        printf 'note  left %s in place\n' "$WORK"
    else
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

FAIL=0
fail() { FAIL=1; printf 'FAIL  %s\n' "$1"; }
ok() { printf 'ok    %s\n' "$1"; }

# a free port, since the page's own 7000 is held by ControlCenter on a stock macOS -- which the
# page itself discloses -- and a gate must not depend on who else is listening
if [ -z "$PORT" ]; then
    PORT=$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)
fi

# ---------------------------------------------------------------------------------------------
# 1. the tree under test: a fresh clone, brought up to the working tree
# ---------------------------------------------------------------------------------------------
if ! git clone -q "$REPO" "$WORK/fresh" 2>"$WORK/clone.err"; then
    fail "git clone of this repository failed: $(tr -d '\n' < "$WORK/clone.err")"
    printf 'FRONT PAGE DIRTY\n'; exit 1
fi
ok "cloned into a scratch directory"

# A clone carries HEAD, and HEAD is not what is about to be published. One regression got through
# exactly there: the document cut dropped the `--ignore-snapshot` row from the page, this check
# read the committed page and reported clean, and CI -- which checks out the pushed commit -- went
# red on all five jobs. So every path git tracks or holds staged is copied over the clone at its
# working-tree contents, and a path HEAD carries that the index no longer does is removed. In CI
# the checkout is clean and the overlay is a no-op, which is the point: the bytes checked here
# before a push are the bytes checked there after it. An untracked file is deliberately NOT copied
# -- a clone cannot have it either -- and is named in a note, so a page that leans on a file
# nobody staged fails here rather than at someone else's clone.
( cd "$REPO" && git ls-files -z ) > "$WORK/index.z" \
    || { printf 'fail  git ls-files failed\n'; exit 2; }
( cd "$REPO" && git ls-files -z --deleted ) > "$WORK/deleted.z" \
    || { printf 'fail  git ls-files --deleted failed\n'; exit 2; }
( cd "$REPO" && git ls-files -z --others --exclude-standard ) > "$WORK/others.z" \
    || { printf 'fail  git ls-files --others failed\n'; exit 2; }
( cd "$REPO" && git ls-tree -r -z --name-only HEAD ) > "$WORK/head.z" \
    || { printf 'fail  git ls-tree HEAD failed\n'; exit 2; }
python3 - "$WORK" "$WORK/fresh" <<'PY' || exit 2
import pathlib, sys
work, fresh = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
def names(stem):
    raw = (work / (stem + ".z")).read_bytes()
    return [n.decode("utf-8", "surrogateescape") for n in raw.split(b"\0") if n]
index, deleted, head, others = names("index"), names("deleted"), names("head"), names("others")
present = [n for n in index if n not in set(deleted)]
if not present:
    print("fail  git lists no tracked paths at all, so there is nothing to put over the clone")
    sys.exit(2)
(work / "present.z").write_bytes(b"".join(n.encode("utf-8", "surrogateescape") + b"\0" for n in present))
removed = 0
for name in sorted(set(head) - set(present)):
    path = fresh / name
    if path.is_symlink() or path.is_file():
        path.unlink()
        removed += 1
print("note  %d tracked path(s) to copy over the clone, %d of HEAD's removed" % (len(present), removed))
if others:
    print("note  %d untracked file(s) left out, which a clone would not have either: %s"
          % (len(others), ", ".join(sorted(others)[:4])))
PY
if ! ( cd "$REPO" && tar -cf - --null -T "$WORK/present.z" ) | ( cd "$WORK/fresh" && tar -xf - ); then
    printf 'fail  copying the working tree over the clone failed\n'; exit 2
fi
# and the copy is verified rather than assumed, because a tar that wrote nothing would leave this
# checking HEAD again in silence -- which is the whole defect this step exists for
python3 - "$REPO" "$WORK/fresh" "$WORK/present.z" <<'PY' || exit 2
import hashlib, pathlib, sys
repo, fresh = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
raw = pathlib.Path(sys.argv[3]).read_bytes()
names = [n.decode("utf-8", "surrogateescape") for n in raw.split(b"\0") if n]
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
bad = [n for n in names if digest(repo / n) != digest(fresh / n)]
if bad:
    print("fail  %d path(s) in the clone are not the working tree's: %s"
          % (len(bad), ", ".join(bad[:4])))
    sys.exit(2)
print("ok    the clone is the working tree over HEAD, %d path(s) compared by digest" % len(names))
PY

# ---------------------------------------------------------------------------------------------
# the extraction. every fenced block is pulled out of README.md by index, so a block that moves
# is still the same block and a block that disappears fails the run rather than being skipped.
# It is read out of the clone and not out of $REPO, so the page extracted and the page run are
# one artifact and cannot be two versions of the same file.
# ---------------------------------------------------------------------------------------------
blocks=$WORK/blocks
mkdir -p "$blocks"
python3 - "$WORK/fresh/README.md" "$blocks" <<'PY'
import pathlib, re, sys
text = pathlib.Path(sys.argv[1]).read_text()
out = pathlib.Path(sys.argv[2])
n = 0
for m in re.finditer(r"```(\w*)\n(.*?)```", text, re.S):
    n += 1
    line = text[:m.start()].count("\n") + 1
    (out / ("%02d.%s" % (n, m.group(1) or "plain"))).write_text(m.group(2))
    (out / ("%02d.line" % n)).write_text(str(line))
print("note  %d fenced block(s) read out of README.md" % n)
PY
[ -d "$blocks" ] || { printf 'fail  the extraction produced nothing\n'; exit 2; }

# find a block by a phrase that must be in it. the phrase IS the anchor: if the page stops
# publishing that command, this check stops rather than testing a copy of it
block_with() {
    for f in "$blocks"/*.bash "$blocks"/*.plain; do
        [ -r "$f" ] || continue
        if grep -qF -- "$1" "$f"; then printf '%s' "$f"; return 0; fi
    done
    return 1
}

SETUP=$(block_with "python3 -m venv") || {
    printf 'fail  no published block runs "python3 -m venv", so the setup the page documents\n'
    printf 'fail  is not where this check expects it; the page changed under the check\n'; exit 2; }
# "$ redis-cli" and not "redis-cli": the container block runs redis-cli too, and the first
# version of this anchor picked that one up and then found no $-prefixed commands in it
TRANSCRIPT=$(block_with '$ redis-cli') || {
    printf 'fail  no published block holds the $-prefixed redis-cli transcript\n'; exit 2; }
SUITE=$(block_with "pytest") || { printf 'fail  no published block runs pytest\n'; exit 2; }
SDIST=$(block_with "python -m build") || { printf 'fail  no published block builds a distribution\n'; exit 2; }

ok "the page publishes the setup, transcript, suite and distribution blocks this check runs"

# the setup block holds the venv line and the server line; only the first is setup
sed -n '1p' "$SETUP" > "$WORK/setup.sh"
if ! grep -q "python3 -m venv" "$WORK/setup.sh"; then
    printf 'fail  the first line of the setup block is no longer the venv command\n'; exit 2
fi
if ( cd "$WORK/fresh" && sh "$WORK/setup.sh" ) >"$WORK/setup.log" 2>&1; then
    ok "the page's own setup command created a venv and installed the dev extra"
else
    fail "the page's own setup command failed: $(tail -2 "$WORK/setup.log" | tr -d '\n')"
fi
VENV=$WORK/fresh/.venv
[ -x "$VENV/bin/python" ] || { fail "no $VENV/bin/python after the documented setup"; }

# ---------------------------------------------------------------------------------------------
# 2 and 3. the server as published, the readiness line, and the published transcript
# ---------------------------------------------------------------------------------------------
if [ -x "$VENV/bin/python" ]; then
    # the server line of the setup block, with the page's own port substituted for a free one
    SERVER_LINE=$(sed -n '2p' "$SETUP")
    case "$SERVER_LINE" in
        *server.py*--port*) : ;;
        *) printf 'fail  the second line of the setup block no longer starts server.py with a port\n'; exit 2 ;;
    esac
    PUBLISHED_PORT=$(printf '%s' "$SERVER_LINE" | sed -E 's/.*--port[= ]([0-9]+).*/\1/')
    printf 'note  substituting port %s for the published %s, which the page itself says a stock\n' \
        "$PORT" "$PUBLISHED_PORT"
    printf 'note  macOS may already hold\n'
    RUN=$(printf '%s' "$SERVER_LINE" | sed "s/$PUBLISHED_PORT/$PORT/")
    # exec so the recorded pid is the interpreter's own and a stop reaches the server rather
    # than a shell that spawned it -- the first cut of this line left no snapshot behind,
    # because the kill went to a subshell that had already gone
    ( cd "$WORK/fresh" && exec sh -c "exec $RUN" >"$WORK/server.out" 2>"$WORK/server.err" ) &
    SERVER_PID=$!
    i=0
    while [ "$i" -lt 100 ]; do
        grep -q "listening on" "$WORK/server.out" 2>/dev/null && break
        kill -0 "$SERVER_PID" 2>/dev/null || break
        i=$((i + 1)); sleep 0.1
    done
    if grep -q "listening on 127.0.0.1:$PORT" "$WORK/server.out" 2>/dev/null; then
        ok "the server printed its readiness line on standard output, naming the port it bound"
    else
        fail "no 'listening on 127.0.0.1:$PORT' on standard output: $(head -2 "$WORK/server.out" "$WORK/server.err" | tr -d '\n')"
    fi

    # the published transcript, run as published and compared against its own replies
    if command -v redis-cli >/dev/null; then
        python3 - "$TRANSCRIPT" "$PORT" "$PUBLISHED_PORT" > "$WORK/transcript.sh" <<'PY'
import pathlib, sys
lines = pathlib.Path(sys.argv[1]).read_text().splitlines()
port, published = sys.argv[2], sys.argv[3]
pairs = []
i = 0
while i < len(lines):
    if lines[i].startswith("$ "):
        cmd = lines[i][2:]
        want = lines[i + 1] if i + 1 < len(lines) and not lines[i + 1].startswith("$ ") else ""
        pairs.append((cmd.replace(published, port), want))
        i += 2
    else:
        i += 1
if not pairs:
    print("echo 'fail  the transcript block holds no $-prefixed commands'; exit 2")
    raise SystemExit(0)
for cmd, want in pairs:
    print("got=$(%s 2>&1)" % cmd)
    print("if [ \"$got\" = %r ]; then echo 'ok    %s -> %s'; else echo \"FAIL  %s -> $got, where the page publishes %s\"; exit 1; fi"
          % (want, cmd, want, cmd, want))
PY
        if ( cd "$WORK/fresh" && sh "$WORK/transcript.sh" ) >"$WORK/transcript.log" 2>&1; then
            sed 's/^/      /' "$WORK/transcript.log"
            ok "every command in the published transcript answered what the transcript publishes"
        else
            sed 's/^/      /' "$WORK/transcript.log"
            fail "the published transcript does not reproduce"
        fi
        # the snapshot the page says the quickstart leaves, where the page says it is
        if [ -f "$WORK/fresh/dump.mrdb" ]; then
            ok "the quickstart left a snapshot at ./dump.mrdb, as --snapshot-path's default says"
        else
            printf 'note  no ./dump.mrdb yet; the default interval had not come due\n'
        fi
    else
        printf 'note  no redis-cli on PATH, so the published transcript was not run -- which the\n'
        printf 'note  page names as a prerequisite for following it\n'
    fi
    kill "$SERVER_PID" 2>/dev/null
    i=0
    while kill -0 "$SERVER_PID" 2>/dev/null && [ "$i" -lt 100 ]; do i=$((i + 1)); sleep 0.1; done
    wait "$SERVER_PID" 2>/dev/null
    if [ -f "$WORK/fresh/dump.mrdb" ]; then
        ok "a stop wrote the snapshot the page says every stop writes"
    else
        fail "no ./dump.mrdb after a stop, which the page says a clean stop always writes"
    fi
fi

# ---------------------------------------------------------------------------------------------
# 4. the suite, by the page's own invocation and the ending it promises
# ---------------------------------------------------------------------------------------------
if [ -x "$VENV/bin/python" ] && [ "${SKIP_SUITE:-0}" != 1 ]; then
    if ( cd "$WORK/fresh" && sh "$SUITE" ) >"$WORK/suite.log" 2>&1; then
        tail -1 "$WORK/suite.log" | sed 's/^/      /'
        # the deselected count is the promise and the skip count is not, which took two red
        # builds to get right: the page said "1 skipped, 4 deselected" as though it were
        # universal, where macOS skips one and Linux skips nine -- the BSD flag, ACL and
        # device-flush facilities one platform has and the other does not. Asserting the macOS
        # ending went red on the runner, and asserting no skip at all on Linux went red again.
        # So the count is reported and the deselected count is what is asserted, which is the
        # one ending the page can promise on every platform. Nothing here reads a skip's reason:
        # that would want -rs and a list of the reasons each platform is allowed, which is a
        # second copy of what the page already says and would go stale against it
        if grep -qE "4 deselected" "$WORK/suite.log"; then
            SKIPS=$(sed -nE 's/.*[^0-9]([0-9]+) skipped.*/\1/p' "$WORK/suite.log" | head -1)
            printf 'note  %s skip(s) on %s, which the page says belongs to the platform\n' \
                "${SKIPS:-0}" "$(uname -s)"
            ok "the suite ended '4 deselected', which is what the page promises"
        else
            fail "the suite passed but did not end '4 deselected' as the page promises"
        fi
    else
        tail -3 "$WORK/suite.log" | sed 's/^/      /'
        fail "the page's own pytest invocation did not pass in a fresh clone"
    fi
fi

# ---------------------------------------------------------------------------------------------
# 5. the distribution block, and the suite the distribution ships
# ---------------------------------------------------------------------------------------------
if [ -x "$VENV/bin/python" ] && [ "${SKIP_SDIST:-0}" != 1 ]; then
    if ( cd "$WORK/fresh" && sh "$SDIST" ) >"$WORK/sdist.log" 2>&1; then
        ok "the page's own commands built a source distribution"
        TARBALL=$(ls "$WORK/fresh"/dist/*.tar.gz 2>/dev/null | head -1)
        if [ -n "$TARBALL" ]; then
            python3 -m venv "$WORK/sdistvenv" >/dev/null 2>&1
            if "$WORK/sdistvenv/bin/pip" install -q "$TARBALL[dev]" >"$WORK/sdistinstall.log" 2>&1; then
                mkdir -p "$WORK/unpacked"
                tar -xzf "$TARBALL" -C "$WORK/unpacked"
                UNPACKED=$(ls -d "$WORK/unpacked"/*/ | head -1)
                # every file the page links to, inside the distribution. README.md ships as the
                # long description, so a link it carries is rendered wherever the package is
                # published, and MANIFEST.in is the only thing putting a linked file in the
                # tarball -- a directive nothing had ever re-read. An anchor is skipped and so is
                # anything with a scheme; what is left is a path a reader would click
                python3 - "$UNPACKED" "$WORK/fresh/README.md" >"$WORK/links.log" 2>&1 <<'LINKPY'
import pathlib, re, sys
unpacked, page = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
links = []
for target in re.findall(r"\]\(([^)]+)\)", page.read_text()):
    if target.startswith("#") or "://" in target or target.startswith("mailto:"):
        continue
    links.append(target.split("#", 1)[0])
if not links:
    print("fail  the page links to no file at all, so this check has nothing to follow")
    raise SystemExit(2)
missing = [t for t in links if not (unpacked / t).exists()]
for t in sorted(set(links)):
    print("note  %s %s" % ("MISSING" if t in missing else "shipped", t))
raise SystemExit(1 if missing else 0)
LINKPY
                LINKS=$?
                sed 's/^/      /' "$WORK/links.log"
                if [ "$LINKS" -eq 0 ]; then
                    ok "every file the page links to is in the distribution"
                elif [ "$LINKS" -eq 1 ]; then
                    fail "the distribution is missing a file the page links to, so the published"
                    fail "long description carries a dead link; MANIFEST.in is what ships it"
                else
                    printf 'fail  the link check could not run\n'; exit 2
                fi
                if ( cd "$UNPACKED" && "$WORK/sdistvenv/bin/python" -m pytest -q ) \
                        >"$WORK/sdistsuite.log" 2>&1; then
                    tail -1 "$WORK/sdistsuite.log" | sed 's/^/      /'
                    ok "the suite the distribution ships passed out of the distribution"
                else
                    tail -3 "$WORK/sdistsuite.log" | sed 's/^/      /'
                    fail "the suite the distribution ships did not pass, which is what MANIFEST.in is for"
                fi
            else
                fail "installing the distribution failed: $(tail -1 "$WORK/sdistinstall.log")"
            fi
        else
            fail "the distribution block left nothing in dist/"
        fi
    else
        tail -2 "$WORK/sdist.log" | sed 's/^/      /'
        fail "the page's own distribution commands failed in a fresh clone"
    fi
fi

# ---------------------------------------------------------------------------------------------
# 6. the container block, verbatim, and the volume it promises
# ---------------------------------------------------------------------------------------------
if [ "$CONTAINER" = 1 ]; then
    CTR=$(block_with "docker build") || { fail "no published block builds an image"; }
    if [ -n "${CTR:-}" ] && command -v docker >/dev/null; then
        TAG=frontpage-$$
        HOSTPORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
        PUB_CTR_PORT=$(grep -oE '127\.0\.0\.1:[0-9]+:6379' "$CTR" | head -1 | cut -d: -f2)
        sed -e "s/mini-redis-data/$TAG-data/g" \
            -e "s/--name mini-redis/--name $TAG/g" \
            -e "s/-t mini-redis /-t $TAG-img /" \
            -e "s/ mini-redis ||/ $TAG-img ||/" \
            -e "s/' mini-redis/' $TAG/g" \
            -e "s/\" mini-redis/\" $TAG/g" \
            -e "s/docker logs mini-redis/docker logs $TAG/g" \
            -e "s/docker stop mini-redis/docker stop $TAG/g" \
            -e "s/127.0.0.1:$PUB_CTR_PORT:6379/127.0.0.1:$HOSTPORT:6379/" \
            -e "s/redis-cli -p $PUB_CTR_PORT/redis-cli -p $HOSTPORT/" \
            "$CTR" > "$WORK/container.sh"
        if grep -q "mini-redis" "$WORK/container.sh"; then
            fail "a published container command still names mini-redis after the substitutions, so"
            fail "this check would touch a container it did not create"
        elif ( cd "$WORK/fresh" && sh "$WORK/container.sh" ) >"$WORK/container.log" 2>&1; then
            ok "the published container block ran and answered"
            docker start "$TAG" >/dev/null 2>&1
            i=0
            while [ "$i" -lt 100 ]; do
                docker logs --since "$(docker inspect -f '{{.State.StartedAt}}' "$TAG")" "$TAG" 2>&1 |
                    grep -q 'listening on' && break
                i=$((i + 1)); sleep 0.1
            done
            redis-cli -p "$HOSTPORT" SET frontpage-key survived >/dev/null 2>&1
            docker stop "$TAG" >/dev/null 2>&1
            docker start "$TAG" >/dev/null 2>&1
            i=0
            while [ "$i" -lt 100 ]; do
                docker logs --since "$(docker inspect -f '{{.State.StartedAt}}' "$TAG")" "$TAG" 2>&1 |
                    grep -q 'listening on' && break
                i=$((i + 1)); sleep 0.1
            done
            if [ "$(redis-cli -p "$HOSTPORT" GET frontpage-key 2>&1)" = "survived" ]; then
                ok "the keyspace survived a stop and a start over the same volume"
            else
                fail "the keyspace did not survive a stop and start over the volume, which is what"
                fail "the container section says the volume is for"
            fi
            docker stop "$TAG" >/dev/null 2>&1
            # and the second run of the same block, which must refuse rather than report ready
            if ( cd "$WORK/fresh" && sh "$WORK/container.sh" ) >"$WORK/container2.log" 2>&1; then
                fail "the published block run a second time reported success over a container it"
                fail "could not create, which is how a PING gets a refused connection"
            else
                ok "the published block run a second time stopped at the name it cannot reuse"
            fi
            docker rm -f "$TAG" >/dev/null 2>&1
            docker volume rm "$TAG-data" >/dev/null 2>&1
            docker rmi "$TAG-img" >/dev/null 2>&1
        else
            tail -3 "$WORK/container.log" | sed 's/^/      /'
            fail "the published container block did not run to a PONG"
            docker rm -f "$TAG" >/dev/null 2>&1
            docker volume rm "$TAG-data" >/dev/null 2>&1
            docker rmi "$TAG-img" >/dev/null 2>&1
        fi
    elif [ -n "${CTR:-}" ]; then
        printf 'note  CONTAINER=1 but no docker on PATH, so the container block was not run\n'
    fi
else
    printf 'note  the container block was not run; CONTAINER=1 adds it, and a local run leaves it\n'
    printf 'note  out so that nothing here touches a daemon this check does not own\n'
fi

if [ "$FAIL" -ne 0 ]; then
    printf 'FRONT PAGE DIRTY\n'
    exit 1
fi
printf 'FRONT PAGE CLEAN\n'
exit 0
