# Format Diff: /sample-data vs /test

## Email separation
| | sample-data | test |
|---|---|---|
| Separator | `From <addr> <timestamp>` envelope line (mbox) | `----------------------------------------` (40 dashes) |
| Thread structure | Flat — each email is independent | Nested — quoted replies embedded in one doc |

## Headers
| | sample-data | test |
|---|---|---|
| Envelope line | `From alice@example.com Mon Jan  1 09:00:00 2024` | None |
| Message-ID | Absent | Present |
| Date format | RFC 2822: `Mon, 1 Jan 2024 09:00:00 +0000` | `09/12/2024 05:59 AM` or `May 08, 2024 at 05:37 AM` |

## Quoted reply prefix
| | sample-data | test |
|---|---|---|
| Format | N/A — replies are separate flat messages | `| ` or `> ` prefixed lines inside the doc |

## Signature
Include signature as part of `content` — do not strip.

## Summary
- **sample-data**: standard mbox format — one file can contain multiple independent emails delimited by the `From ` envelope line. No thread nesting.
- **test**: thread-dump format — one file is one thread, top email is bare, older replies are quoted inline with a prefix and separated by dashes. Two sub-variants exist (`| ` vs `> ` prefix, different date formats).
