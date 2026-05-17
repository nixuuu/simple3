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
list_version = 2          # simple3 implements ListObjectsV2 only — see Gotchas
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

Local end-to-end run against `minio/minio:latest` (Docker) and `rclone/rclone:latest`
(Docker). Source bucket contained:

- 100 small objects (`k-001.txt` … `k-100.txt`, single PUT each)
- 1 × 50 MiB random object (`big.bin`, transferred as a 7-part multipart upload)
- 1 object with custom `Content-Type: application/pdf` and a user-metadata header

Sync command (`rclone/rclone:latest`, `list_version = 2` set on the destination remote):

```text
$ rclone sync src:mig-source dst:mig-dest --transfers 8 --s3-chunk-size 8M
Transferred:   	   50.002 MiB / 50.002 MiB, 100%
Checks:                 0 / 0, -, Listed 102
Transferred:          102 / 102, 100%
Elapsed time:         0.9s
```

Verification:

- `aws s3 ls --recursive` count: src = dst = 102.
- SHA-256 over every transferred file: 102/102 byte-identical (`diff` clean).
- `HeadObject` on `big.bin`: `ETag: "c5bc0ff942ae79e5b186ddf8e79dcb38-7"` —
  multipart format preserved (`<md5>-<parts>`), 50 MiB / 7 parts ⇒ 8 MiB chunk
  matches the rclone setting.
- `HeadObject` on `doc.pdf`: `ContentType: "application/pdf"` plus the
  `x-amz-meta-mtime` user metadata rclone adds — both round-tripped.

AWS S3 source not tested directly. The protocol surface is the same; only
credential handling and endpoint syntax differ from MinIO.

The mint and ceph s3-tests suites (`tests/s3_compat/`) further cover the
destination-side semantics that rclone and aws-cli rely on.

## Gotchas

- **`ListObjects` v1 not implemented.** simple3 only exposes `ListObjectsV2`.
  rclone defaults to the v1 API when it inspects the destination bucket for
  pre-existing objects (used by `sync` to compute which files to delete).
  Without `list_version = 2`, `rclone sync` will copy the data correctly and
  then fail at the end with `501 NotImplemented: ListObjects`. Always set
  `list_version = 2` on the simple3 remote.
- **Bucket name restrictions.** Both simple3 and AWS use the standard S3 naming rules (3–63 chars, lowercase, alphanumerics + hyphens). Buckets that exist in MinIO with wider names need renaming first.
- **Path-style addressing.** simple3 does not implement virtual-host routing. Always set `force_path_style = true` (rclone) or `--endpoint-url` with path-style (aws-cli, `--addressing-style path`).
- **Empty `Content-Type`.** S3 defaults to `binary/octet-stream`; some tools omit the header and simple3 will record `None`. If the downstream consumer relies on the header, set it explicitly.
- **Inflight multipart on the source.** Abort or complete every in-progress multipart upload on the source before sync; rclone does not migrate `MultipartUploads` listings.
