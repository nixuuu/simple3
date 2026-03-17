FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/ceph/s3-tests.git /s3-tests

WORKDIR /s3-tests
RUN pip install --no-cache-dir tox

# Keep container alive for exec-based test execution.
ENTRYPOINT ["tail", "-f", "/dev/null"]
