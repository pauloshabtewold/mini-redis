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
COPY pyproject.toml README.md LICENSE ./
RUN python -c 'import tomllib; print("\n".join(tomllib.load(open("pyproject.toml", "rb"))["project"]["dependencies"]))' > /tmp/requirements.txt \
    && pip install --no-compile -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# Source layer. The server runs from /app; installing the package as well shows that
# pyproject.toml builds from the files the build context carries. The build and egg-info
# directories that leaves in /app are removed.
COPY . .
RUN pip install --no-compile . \
    && rm -rf build ./*.egg-info

VOLUME /data
USER mini-redis
EXPOSE 6379

# Exec form: the server is PID 1 and receives the SIGTERM `docker stop` sends. Under a
# shell form PID 1 is /bin/sh, which does not forward it, and the server is killed after
# the stop timeout without its shutdown save having run.
CMD ["python", "server.py", "--host", "0.0.0.0", "--snapshot-path", "/data/dump.mrdb"]
