"""The command layer: one module per type, dispatched through the registry. Its presence is what makes commands a package rather than a module."""

from commands.registry import dispatch

# a decorator registers only when its module is imported; without this the registry is empty in the running server, and only a test that reaches the package rather than a handler module can see it
from commands import server as _server_commands
from commands import string as _string_commands
from commands import list as _list_commands
