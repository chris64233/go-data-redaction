// Package godataredaction implements resumable data-redaction tasks whose
// results are published only after every shard has succeeded and all output
// digests have been verified.
//
// Guarantees:
//
//   - Frozen inputs. Creating a task freezes the source data version, the
//     redaction-rule version and the shard set. Resubmitting the same external
//     ID with the same inputs returns the original task; the same external ID
//     with a changed data range or rule set is a conflict.
//   - Leased work with fencing. Workers claim shards under expiring leases.
//     Each (re)lease gets a strictly increasing fencing token. After a lease
//     expires another worker may take over, and late receipts from the old
//     worker are rejected instead of overwriting the new result. A different
//     digest for an already succeeded shard is a conflict, not a retry.
//   - Publication once. All shards must succeed and stored bytes must
//     re-verify against their digests before a single immutable manifest is
//     written and the task becomes downloadable. While processing, no result
//     data is readable externally; concurrent completion of the final shards
//     can publish at most once.
//   - Resumable and clean. Failed shards retry at the original frozen task
//     version; cancellation stops new claims and discards staged output.
//     Cancel, late receipts and publication race under one state lock, so a
//     task ends in exactly one terminal state. Outputs not referenced by the
//     successful manifest are garbage collected.
package godataredaction
