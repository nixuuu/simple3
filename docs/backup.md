# Backup and restore

simple3 stores all state on disk inside a single `data_dir`. A consistent copy of that directory is a complete backup.

## File layout

```text
data_dir/
  _auth.redb              # access keys and IAM policies
  bucket-a/
    index.redb            # object metadata (redb database)
    seg_000000.bin        # append-only data segments
    seg_000001.bin
    ...
  bucket-b/
    index.redb
    seg_000000.bin
    ...
```

**Required for backup:** `_auth.redb`, every `{bucket}/index.redb`, and all `seg_*.bin` files.

**Safe to skip:** `.tmp_*`, `.mpu_*`, `*.bin.tmp`. These are transient files from in-progress uploads or compactions. The server removes them automatically on startup.

## Cold backup

1. Stop the server.
2. Copy the entire `data_dir`:

```bash
cp -a /path/to/data_dir /path/to/backup/
```

3. Restart the server.

The copy is consistent because no process is writing to the files.

## Filesystem snapshot backup

If stopping the server is not acceptable, use a filesystem that supports atomic snapshots (ZFS, Btrfs, LVM).

**ZFS example:**

```bash
zfs snapshot tank/simple3@backup-$(date +%Y%m%d)
# mount or send the snapshot elsewhere
zfs send tank/simple3@backup-20260315 > /backup/simple3.zfs
```

**LVM example:**

```bash
lvcreate --size 1G --snapshot --name snap /dev/vg/simple3
mount /dev/vg/snap /mnt/snap
cp -a /mnt/snap/data_dir /path/to/backup/
umount /mnt/snap
lvremove /dev/vg/snap
```

Snapshots are crash-consistent: simple3 treats a snapshot the same way it treats a crash. On open, it cleans up temp files, truncates orphaned appends, and recovers interrupted compactions.

## Restore

1. Make sure the server is stopped.
2. Copy the backup into the target `data_dir`:

```bash
cp -a /path/to/backup/ /path/to/data_dir/
```

3. Verify integrity (offline, server not running):

```bash
simple3 --data-dir /path/to/data_dir verify
```

4. Start the server. On startup it will:
   - Remove leftover `.tmp_*`, `.mpu_*`, and `*.bin.tmp` files
   - Truncate orphaned data at the end of the active segment
   - Recover any interrupted compactions

## What the verify command checks

For every stored object:
- Reads the full data from the segment file
- Computes MD5 and compares against stored `content_md5` / ETag
- Computes CRC32C and compares against stored `content_crc32c`
- Reads the on-disk CRC trailer and compares against the expected value

A clean verify means every object in the backup is intact.
