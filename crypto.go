package godataredaction

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// hexDigest returns the SHA-256 hex digest of the concatenated parts.
func hexDigest(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// fingerprintInput is the canonical JSON shape used to detect duplicate
// submissions and input conflicts. Shard order is normalized away so that the
// same shard set submitted in a different order is the same task input.
type fingerprintInput struct {
	ExternalID    string   `json:"external_id"`
	SourceVersion string   `json:"source_version"`
	RuleVersion   string   `json:"rule_version"`
	ShardIDs      []string `json:"shard_ids"`
}

// inputFingerprint freezes the logical CreateInput into one hash.
func inputFingerprint(in CreateInput) (string, error) {
	if in.ExternalID == "" || in.SourceVersion == "" || in.RuleVersion == "" || len(in.ShardIDs) == 0 {
		return "", fmt.Errorf("%w: external id, source/rule versions and shards are required", ErrInvalidInput)
	}
	seen := make(map[string]struct{}, len(in.ShardIDs))
	shards := make([]string, 0, len(in.ShardIDs))
	for _, s := range in.ShardIDs {
		if s == "" {
			return "", fmt.Errorf("%w: empty shard id", ErrInvalidInput)
		}
		if _, dup := seen[s]; dup {
			return "", fmt.Errorf("%w: duplicate shard id %q", ErrInvalidInput, s)
		}
		seen[s] = struct{}{}
		shards = append(shards, s)
	}
	sort.Strings(shards)
	b, err := json.Marshal(fingerprintInput{
		ExternalID:    in.ExternalID,
		SourceVersion: in.SourceVersion,
		RuleVersion:   in.RuleVersion,
		ShardIDs:      shards,
	})
	if err != nil {
		return "", err
	}
	return hexDigest(b), nil
}

// taskID derives the internal task ID from the frozen input fingerprint, so
// identical submissions resolve to the same task even across restarts.
func taskID(fingerprint string) string {
	return "task_" + fingerprint[:16]
}

// manifestDigest is the content hash over the immutable result list. It binds
// the frozen task version, source and rule versions and every shard output
// digest, in the task's frozen shard order.
func manifestDigest(t *Task, entries []ManifestEntry) (string, error) {
	type canonical struct {
		TaskID        string          `json:"task_id"`
		TaskAttempt   int             `json:"task_attempt"`
		SourceVersion string          `json:"source_version"`
		RuleVersion   string          `json:"rule_version"`
		Entries       []ManifestEntry `json:"entries"`
	}
	b, err := json.Marshal(canonical{
		TaskID:        t.ID,
		TaskAttempt:   t.Attempt,
		SourceVersion: t.SourceVersion,
		RuleVersion:   t.RuleVersion,
		Entries:       entries,
	})
	if err != nil {
		return "", err
	}
	return hexDigest(b), nil
}

// validDigest checks a SHA-256 hex string.
func validDigest(d string) bool {
	if len(d) != 64 {
		return false
	}
	_, err := hex.DecodeString(d)
	return err == nil
}
