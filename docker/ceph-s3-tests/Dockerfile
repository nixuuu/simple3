FROM python:3.12-slim

ARG S3_TESTS_REF=master
ARG TOX_VERSION=4.25.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/ceph/s3-tests.git /s3-tests && \
    cd /s3-tests && git checkout "${S3_TESTS_REF}"

WORKDIR /s3-tests
RUN pip install --no-cache-dir "tox==${TOX_VERSION}"

RUN useradd --create-home --uid 10001 s3tests && \
    chown -R s3tests:s3tests /s3-tests
USER s3tests

# Keep container alive for exec-based test execution.
ENTRYPOINT ["tail", "-f", "/dev/null"]
