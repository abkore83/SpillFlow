# Security Policy

## Supported Versions

SpillFlow is currently an alpha project. Only the latest published alpha is
eligible for security fixes.

## Reporting a Vulnerability

Do not open a public issue for a suspected vulnerability involving temporary
data exposure, encryption, path traversal, recovery manifests, denial of
service, or unsafe generated code. Use GitHub private vulnerability reporting
for the repository. If private reporting is unavailable, contact the repository
owner privately before disclosing details.

Include affected versions, reproduction steps, impact, and any suggested
mitigation. You should receive an acknowledgement within seven days. No response
deadline or fix date is guaranteed while the project remains pre-1.0.

SpillFlow does not provide secure deletion, process isolation, or protection
against a compromised host. Applications handling regulated data must perform
their own threat assessment and key-management review.

## Temporary Files

By default, each new Windows session directory is created atomically with a
protected ACL that grants inheritable full control only to the current user.
Unix session directories use owner-only `0700` permissions. Recovery reapplies
these permissions before reading preserved files. If permission enforcement
fails, session creation or recovery fails closed.

`ProtectTemporaryDirectory = false` disables this protection for compatibility
with unusual filesystems. Regulated workloads should not disable it without an
equivalent external access-control policy. Directory permissions do not provide
secure deletion, host isolation, or protection from administrators, backups,
snapshots, or a compromised account.
