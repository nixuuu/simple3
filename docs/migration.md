# Migration to simple3

simple3 speaks the S3 HTTP API, so any S3-aware tool can copy data into it. The reference flow is `rclone sync`; `aws s3 sync` works too with the AWS CLI configured against a simple3 endpoint.

## Prerequisites

- A running simple3 server with an access key (the root key is printed on first start; you can create more with `simple3 keys create`).
- The source endpoint, access key, and secret key.
- For multipart-eligible source objects (`>5 MiB`), TLS or a private network — the migration moves cleartext credentials and data otherwise.

## rclone (recommended)

`rclone` retries on transient failures, runs concurrent transfers, and exposes a `check` subcommand for verification.

### Configure remotes

`~/.config/rclone/rclone.conf`:

```ini
[source-minio]
type = s3
provider = Minio
access_key_id = SRC_AK
secret_access_key = SRC_SK
endpoint = http://old-minio:9000
force_path_style = true

[source-aws]
type = s3
provider = AWS
region = eu-west-1
access_key_id = SRC_AK
secret_access_key = SRC_SK

[simple3]
type = s3
provider = Other
access_key_id = AK
secret_access_key = SK
endpoint = http://simple3:8080
force_path_style = true
```

### Copy a single bucket

```bash
rclone sync \
  --transfers 16 --checkers 16 \
  --s3-chunk-size 16M --s3-upload-cutoff 16M \
  --progress \
  source-minio:bucket-a simple3:bucket-a
```

Tune `--transfers` to match the simple3 CPU count. `--s3-chunk-size` controls multipart part size; 16 MiB is a balanced default. For objects below the cutoff, rclone uses a single PUT.

### Copy every bucket

rclone has no built-in "all buckets" mode; list and loop:

```bash
for b in $(rclone lsd source-minio: | awk '{print $NF}'); do
  rclone mkdir simple3:$b
  rclone sync source-minio:$b simple3:$b --transfers 16
done
```

### Resume an interrupted transfer

`rclone sync` is idempotent — re-running the command skips files whose size and modtime already match the destination.

## aws s3 sync

```bash
AWS_ACCESS_KEY_ID=AK AWS_SECRET_ACCESS_KEY=SK \
  aws --endpoint-url http://simple3:8080 \
  s3 sync s3://bucket-a s3://bucket-a \
  --source-region eu-west-1
```

The two `--endpoint-url` form is awkward (`aws s3 sync` only takes one). Prefer rclone unless the toolchain mandates the AWS CLI.

## What carries over

| Aspect | Preserved | Notes |
|---|---|---|
| Object body | yes | Verified by ETag (single-part) or MD5 (multipart) |
| `Content-Type` | yes | Round-trips through `x-amz-meta-*` headers |
| Custom `x-amz-meta-*` user metadata | yes | |
| Single-part ETag (MD5) | yes | Identical on both sides |
| Multipart ETag (`md5-N`) | **format-compatible, value differs** | See below |
| Last-modified | **no** | Destination records the time of the migration write |
| Storage class | no | simple3 has a single tier |
| Object ACLs | no | Replaced by IAM-style policies |
| Object Lock / Legal Hold | no | Not implemented |
| Versioning state | manual | Re-enable on the destination before sync (`simple3 ... put-bucket-versioning Enabled`) |

### Multipart ETag

Both MinIO and simple3 use the AWS form `<hex>-<part-count>`. The hex part is `md5(concat(md5_of_each_part))`, so it depends on how the upload was chunked. When the migration tool re-chunks (e.g. `--s3-chunk-size 16M` vs. an original 8 MiB upload), the destination ETag is computed over the new boundaries and won't equal the source ETag. The object contents are identical; only the ETag string differs.

This means:

- A direct ETag-only comparison after migration will report false mismatches for multipart objects.
- Use `rclone check --download` (re-hashes both sides) or simple3's `verify` (recomputes MD5 from segments).

## Verification

### Object count and size

```bash
rclone size source-minio:bucket-a
rclone size simple3:bucket-a
```

Both numbers must match.

### Per-object spot check

```bash
rclone check source-minio:bucket-a simple3:bucket-a --one-way
```

`check` compares sizes and (single-part) ETags. Multipart objects will warn about ETag mismatches — that is expected. Add `--download` to re-hash the bytes:

```bash
rclone check source-minio:bucket-a simple3:bucket-a --download --one-way
```

### Server-side integrity

After migration, run simple3's full-bucket verify. It reads every stored object from segments and recomputes MD5 and CRC32C:

```bash
simple3 verify bucket-a
# or against the live server:
curl -H "Authorization: Bearer $AK:$SK" \
  http://simple3:8080/_/verify/bucket-a
```

## Tested scenarios

The migration flow has been exercised against the following sources:

- MinIO single-node (RELEASE.2024-01-01) — bucket of 1000 small objects (1 KB each).
- MinIO single-node — single 5.5 GiB object, multipart with 16 MiB parts.
- MinIO single-node — objects carrying `x-amz-meta-author` and `Content-Type: application/pdf`.
- AWS S3 (`eu-west-1`) — empty bucket (only metadata, no objects).
- AWS S3 — bucket of 200 objects ranging 1 KB – 100 MiB.

The mint and ceph s3-tests suites (`tests/s3_compat/`) further cover the destination-side semantics that rclone and aws-cli rely on.

## Gotchas

- **Bucket name restrictions.** Both simple3 and AWS use the standard S3 naming rules (3–63 chars, lowercase, alphanumerics + hyphens). Buckets that exist in MinIO with wider names need renaming first.
- **Path-style addressing.** simple3 does not implement virtual-host routing. Always set `force_path_style = true` (rclone) or `--endpoint-url` with path-style (aws-cli, `--addressing-style path`).
- **Empty `Content-Type`.** S3 defaults to `binary/octet-stream`; some tools omit the header and simple3 will record `None`. If the downstream consumer relies on the header, set it explicitly.
- **Inflight multipart on the source.** Abort or complete every in-progress multipart upload on the source before sync; rclone does not migrate `MultipartUploads` listings.
