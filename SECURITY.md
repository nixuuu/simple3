# Security policy

## Supported versions

| Version | Supported |
|---------|-----------|
| Latest release on `main` | Yes |
| Older releases | No |

simple3 is pre-v1. Only the latest release receives security fixes. There are no backport branches.

## Reporting a vulnerability

Please report security vulnerabilities privately through [GitHub Security Advisories](../../security/advisories/new). This keeps the details confidential until a fix is available.

If you cannot use GitHub Advisories, email the repository owner directly (address on the GitHub profile).

We will acknowledge your report within 72 hours and work with you on coordinated disclosure: fix first, then public advisory.

There is no bug bounty program.

## Scope

In scope:

- Storage engine (data corruption, unauthorized access to segments)
- Authentication and IAM policy evaluation bypass
- S3 and gRPC API vulnerabilities
- CLI credential handling (leaking keys, insecure storage)

Out of scope:

- Denial of service against self-hosted instances
- Vulnerabilities in third-party dependencies (please report those upstream)
