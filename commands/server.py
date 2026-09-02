"""Connection and server commands: PING, ECHO and HELLO.

DBSIZE, KEYS, FLUSHALL, INFO and CONFIG belong to this group and are not written
yet. They are named here rather than left out because this file, unlike the empty
stubs, already has working code, so a reader has no other signal that the group is
incomplete -- and no signal at all that these five are where it continues.
"""

from commands.registry import Kind, Reply, command, wrong_arity
import resp

# kept as a constant rather than read from installed metadata, so the reply does not depend on the project being installed
SERVER_VERSION = b"0.1.0"

# named so a follower has one place to change
REPLICATION_ROLE = b"master"


@command(b"PING", arity=-1, kind=Kind.OTHER)
def ping(store, conn, argv: list[bytes]) -> Reply:
    # the arity integer expresses a minimum only, not a range, so the two-token upper bound is checked here
    if len(argv) > 2:
        return wrong_arity(b"PING"), []
    if len(argv) == 1:
        return resp.encode_simple_string(b"PONG"), []
    return resp.encode_bulk_string(argv[1]), []


@command(b"ECHO", arity=2, kind=Kind.OTHER)
def echo(store, conn, argv: list[bytes]) -> Reply:
    return resp.encode_bulk_string(argv[1]), []


@command(b"HELLO", arity=-1, kind=Kind.OTHER)
def hello(store, conn, argv: list[bytes]) -> Reply:
    # same minimum-only arity as ping, hence the explicit upper bound here too
    if len(argv) > 2:
        return wrong_arity(b"HELLO"), []
    if len(argv) == 2 and argv[1] != b"2":
        # every argument other than b"2" answers NOPROTO, integer or not -- diverges from real Redis's own parse error, deliberately
        return resp.encode_error(b"NOPROTO unsupported protocol version"), []
    # RESP2 has no map type, so this is a flat array of alternating field/value pairs -- what a real server returns on RESP2
    return resp.encode_array([
        resp.encode_bulk_string(b"server"),
        resp.encode_bulk_string(b"redis"),
        resp.encode_bulk_string(b"version"),
        resp.encode_bulk_string(SERVER_VERSION),
        resp.encode_bulk_string(b"proto"),
        resp.encode_integer(2),
        resp.encode_bulk_string(b"id"),
        resp.encode_integer(conn.id),
        resp.encode_bulk_string(b"mode"),
        resp.encode_bulk_string(b"standalone"),
        resp.encode_bulk_string(b"role"),
        # the replication role, not Connection.role -- the two vocabularies are disjoint
        resp.encode_bulk_string(REPLICATION_ROLE),
        resp.encode_bulk_string(b"modules"),
        resp.encode_array([]),
    ]), []
