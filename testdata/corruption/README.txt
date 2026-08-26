QMAX corruption fixtures are generated from a real WAL at test runtime.

The integration harness must preserve the source and produce variants with:
- truncation at every byte offset;
- one-bit flips for every bit in every byte;
- appended garbage;
- duplicated, removed, and reordered complete frames;
- checksum-valid records containing invalid transaction transitions.

Artifacts belong under artifacts/corruption/<test>/<seed>/ and must include the
source WAL, modified WAL, byte offset and bit, process stdout/stderr, exit code,
and recovery classification. Static opaque WAL fixtures are deliberately not
checked in because they drift when the versioned record format changes.
