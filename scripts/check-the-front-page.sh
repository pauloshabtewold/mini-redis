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
#   1. The quickstart's own setup block, run in a fresh clone: the venv and the dev install.
#   2. The quickstart's own run block: the server started as published, and `listening on` read
#      off its standard output, which is the line the page tells a reader to wait for.
#   3. The three `redis-cli` commands of the published transcript, compared against the transcript's
#      own replies rather than against what this script thinks they should be.
#   4. The suite, by the page's own invocation, and the ending the page promises.
#   5. The source-distribution block, and the suite the distribution ships.
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
# the extraction. every fenced block is pulled out of README.md by index, so a block that moves
# is still the same block and a block that disappears fails the run rather than being skipped.
# ---------------------------------------------------------------------------------------------
blocks=$WORK/blocks
mkdir -p "$blocks"
python3 - "$REPO/README.md" "$blocks" <<'PY'
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

# ---------------------------------------------------------------------------------------------
# 1. a fresh clone, and the page's own setup commands
# ---------------------------------------------------------------------------------------------
if ! git clone -q "$REPO" "$WORK/fresh" 2>"$WORK/clone.err"; then
    fail "git clone of this repository failed: $(tr -d '\n' < "$WORK/clone.err")"
    printf 'ONE USER DIRTY\n'; exit 1
fi
ok "cloned into a scratch directory"

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
        # the deselected count always, and the skip count by platform, because the page says the
        # one skip is a macOS loopback and that the test runs in CI. The first cut asserted the
        # macOS ending everywhere and went red on the Linux runner -- which is this check finding
        # a published claim that was false on the platform CI runs, and the page now says so
        case "$(uname -s)" in
            Darwin) WANT="1 skipped, 4 deselected" ;;
            *)      WANT="4 deselected" ;;
        esac
        if grep -qE "$WANT" "$WORK/suite.log" &&
                { [ "$(uname -s)" = Darwin ] || ! grep -qE "[0-9]+ skipped" "$WORK/suite.log"; }; then
            ok "the suite ended the way the page says a healthy run ends on this platform"
        else
            fail "the suite passed but did not end '$WANT' as the page promises for $(uname -s)"
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
