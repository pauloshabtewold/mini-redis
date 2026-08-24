"""The command layer: one module per type, dispatched through the registry. Its presence is what makes commands a package rather than a module."""

from commands.registry import dispatch

# a decorator registers only when its module is imported; without this, the registry is empty in the running server, though every test that imports commands.server directly still passes
from commands import server as _server_commands
