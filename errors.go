package godataredaction

import "errors"

// Sentinel errors. Callers should use errors.Is to detect them; wrapped
// context may be attached.
var (
	// ErrNotFound means no task/shard/manifest matched the reference.
	ErrNotFound = errors.New("not found")
	// ErrConflict means an external ID was reused with different inputs
	// (frozen data range or rule/source versions changed).
	ErrConflict = errors.New("task conflict: same external id with different input")
	// ErrInvalidInput marks malformed create/receipt/claim parameters.
	ErrInvalidInput = errors.New("invalid input")
	// ErrTaskTerminal means the task already reached cancelled/published and
	// the requested mutation (claim, new receipt, retry) is impossible.
	ErrTaskTerminal = errors.New("task already in terminal state")
	// ErrTaskCancelled is the cancellation flavour of ErrTaskTerminal.
	ErrTaskCancelled = errors.New("task cancelled")
	// ErrTaskPublished is the publication flavour of ErrTaskTerminal.
	ErrTaskPublished = errors.New("task published")
	// ErrNoAvailableShard means no shard is currently claimable (all are
	// leased/succeeded/failed-but-not-retried).
	ErrNoAvailableShard = errors.New("no shard available to claim")
	// ErrVersionMismatch means a receipt carried the wrong task version.
	ErrVersionMismatch = errors.New("task version mismatch")
	// ErrLeaseFenced means the receipt belongs to a superseded lease: the
	// lease expired and another worker took over, so the late receipt cannot
	// overwrite the newer result.
	ErrLeaseFenced = errors.New("lease fenced: superseded by a newer lease")
	// ErrDigestMismatch means a successful receipt's claimed digest does not
	// match the delivered payload, or a same-shard redelivery claimed a
	// different digest than the accepted one. Different outputs for the same
	// shard are a conflict, never a silent retry/overwrite.
	ErrDigestMismatch = errors.New("output digest mismatch")
	// ErrNotLeaser means the receipt worker is not the current lease holder.
	ErrNotLeaser = errors.New("worker is not the current lease holder")
	// ErrShardNotFailed means a retry was requested for a shard that is not
	// in the failed state.
	ErrShardNotFailed = errors.New("shard is not in failed state")
	// ErrNotPublished blocks result access: the manifest does not exist yet,
	// so external readers cannot observe partial redaction output.
	ErrNotPublished = errors.New("result not published")
	// ErrVerificationFailed means publication-time re-verification found a
	// stored output missing or corrupt; publication is refused.
	ErrVerificationFailed = errors.New("manifest verification failed")
)

func terminalErr(status TaskStatus) error {
	switch status {
	case TaskCancelled:
		return ErrTaskCancelled
	case TaskPublished:
		return ErrTaskPublished
	default:
		return ErrTaskTerminal
	}
}
