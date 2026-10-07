FROM python:3.13-slim

# no bytecode is written, so no __pycache__ ends up in the image or beside the source at
# run time, and stdout and stderr reach `docker logs` as they are written
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

# The server runs as this user and owns /data. It proves its snapshot path writable at
# startup by creating and removing a file beside it, so a /data this user cannot write to
# is a refused start rather than a failure at the first save. A named volume mounted at
# /data for the first time takes its ownership from this directory. The uid is fixed so
# that a volume written by one build is still writable by the next.
RUN groupadd --gid 10001 mini-redis \
    && useradd --uid 10001 --gid 10001 --no-create-home \
        --shell /usr/sbin/nologin mini-redis \
    && mkdir /data \
    && chown mini-redis:mini-redis /data

# Dependency layer. The project has no runtime dependencies today; the list is read from
# pyproject.toml so that one added later is installed here, and this layer is rebuilt
# only when pyproject.toml changes, not when the source does.
# pyproject.toml alone, and not README.md and LICENSE beside it: they are what `pip install .`
# reads for the long description and the licence, the source layer's own COPY brings them,
# and copying them here instead busts this layer on every edit to the published front page,
# which is the file this project changes most. The comment above is only true of a COPY that
# names the one file the RUN below actually reads.
COPY pyproject.toml ./
RUN python -c 'import tomllib; print("\n".join(tomllib.load(open("pyproject.toml", "rb"))["project"]["dependencies"]))' > /tmp/requirements.txt \
    && pip install --no-compile -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# Source layer. The server runs from /app; installing the package as well shows that
# pyproject.toml builds from the files the build context carries. The build and egg-info
# directories that leaves in /app are removed.
# The last command asserts what the install carried, because nothing else fails a build
# whose context lacks README.md or LICENSE: the dependency layer's COPY used to, and
# setuptools builds the wheel without a long description or without the licence file and
# says nothing, so a .dockerignore edit that dropped either would ship an image that
# looks right. It is here and not back on that COPY because a COPY that names those files
# rebuilds the dependency layer on every edit to the front page, and because the installed
# metadata is what the wheel holds, which the presence of the files in the context does
# not show.
# The distribution name is read from pyproject.toml rather than written out here, because a
# hardcoded one turns a rename into a build that fails with PackageNotFoundError and names
# neither file, when both had in fact reached the image. And the long description is read as
# `get("Description") or get_payload()`: METADATA 2.4 may carry it in the message body or folded
# into a Description header, importlib.metadata synthesizes the header from the body when it is
# absent, and asserting on the body alone would fail a correct build under a setuptools that
# writes the other shape -- which `requires` does not pin an upper bound against.
COPY . .
RUN pip install --no-compile . \
    && rm -rf build ./*.egg-info \
    && python -c 'import tomllib, importlib.metadata as m; name = tomllib.load(open("pyproject.toml", "rb"))["project"]["name"]; d = m.metadata(name); assert d.get("Description") or d.get_payload(), "README.md did not reach the image: the installed package has no long description"; assert d.get_all("License-File"), "LICENSE did not reach the image: the installed package carries no licence file"'

VOLUME /data
USER mini-redis
EXPOSE 6379

# Exec form: the server is PID 1, so the SIGTERM `docker stop` sends reaches a handler
# rather than being discarded -- but only once main() has installed it, and the
# interpreter's own startup and this module's imports run before that. A stop inside that
# window is still discarded: the container serves on to the end of the stop timeout and
# dies on SIGKILL with no save, and a write it answers `OK` before then is gone after a
# restart. Measured on a 400k-key volume, a `SET` acked and then lost, with `docker stop`
# taking 10.28 s and exit 137. The window ends when main()'s one handler is armed, which
# is interpreter startup plus imports and does not grow with the keyspace: from the
# daemon's StartedAt to that handler it was 209.0 to 607.4 ms over 14 fresh starts on an
# empty volume, median 237.2, at load average 6.1 to 7.4, and the median on a 400k-key
# volume was 262.2 ms. The largest of 14 is not a ceiling. run()'s two handlers are armed
# later, after the snapshot load, and that point does grow: median 242.7 ms on the empty
# volume, 1,257.8 ms on the 400k-key one. A stop between the two is recorded and honoured.
# Waiting for `listening on` before the first command avoids it. Under a shell form PID 1
# is /bin/sh, which does not forward the signal, and the server is killed after the stop
# timeout without its shutdown save having run.
CMD ["python", "server.py", "--host", "0.0.0.0", "--snapshot-path", "/data/dump.mrdb"]
