package godataredaction

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// stateVersion is the on-disk snapshot format version.
const stateVersion = 1

type persistedState struct {
	Version       int               `json:"version"`
	Tasks         map[string]*Task  `json:"tasks"`
	ExternalIndex map[string]string `json:"external_index"`
}

// store is the durable state plus the output/manifest object store.
//
// Durability model: every state mutation is followed by an atomic
// write-and-rename of state.json while mu is held. Output objects are
// content-addressed and immutable; manifests are write-once files. The single
// rw mutex also orders filesystem mutations (blob writes, GC) against state
// transitions.
type store struct {
	dir string
	mu  sync.RWMutex

	tasks map[string]*Task
	// extIndex maps external task id -> internal task id.
	extIndex map[string]string
}

func newStore(dir string) (*store, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: storage directory is required", ErrInvalidInput)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	st := &store{dir: dir, tasks: map[string]*Task{}, extIndex: map[string]string{}}
	if err := st.load(); err != nil {
		return nil, err
	}
	if err := st.reconcileLocked(); err != nil {
		return nil, err
	}
	return st, nil
}

// reconcileLocked heals filesystem state after a crash between the state
// snapshot and filesystem operations, so re-opening the store never leaves
// externally visible inconsistencies:
//   - cancelled tasks keep no output;
//   - published tasks retain exactly the manifest-referenced objects;
//   - running tasks keep only outputs referenced by succeeded shards
//     (crash after a blob write but before its receipt committed leaves an
//     orphan that is collected here).
func (s *store) reconcileLocked() error {
	for id, t := range s.tasks {
		stored, err := s.listOutputDigestsLocked(id)
		if err != nil {
			return err
		}
		live := map[string]struct{}{}
		switch t.Status {
		case TaskCancelled:
			if err := s.removeAllOutputsLocked(id); err != nil {
				return err
			}
			continue
		case TaskPublished:
			if t.ManifestID == "" {
				return fmt.Errorf("task %s published without manifest id", id)
			}
			m, err := s.readManifestLocked(id, t.ManifestID)
			if err != nil {
				return fmt.Errorf("task %s published but manifest unreadable: %w", id, err)
			}
			for _, e := range m.Entries {
				live[e.Digest] = struct{}{}
			}
		default:
			for _, sh := range t.Shards {
				if sh.Status == ShardSucceeded && sh.Digest != "" {
					live[sh.Digest] = struct{}{}
				}
			}
		}
		for _, d := range stored {
			if _, ok := live[d]; ok {
				continue
			}
			if err := s.removeOutputLocked(id, d); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *store) statePath() string        { return filepath.Join(s.dir, "state.json") }
func (s *store) taskDir(id string) string { return filepath.Join(s.dir, "tasks", id) }
func (s *store) blobsDir(id string) string {
	return filepath.Join(s.taskDir(id), "blobs")
}
func (s *store) manifestsDir(id string) string {
	return filepath.Join(s.taskDir(id), "manifests")
}
func (s *store) blobPath(tid, digest string) string {
	return filepath.Join(s.blobsDir(tid), digest)
}
func (s *store) manifestPath(tid, manifestID string) string {
	return filepath.Join(s.manifestsDir(tid), manifestID+".json")
}

func (s *store) load() error {
	b, err := os.ReadFile(s.statePath())
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var p persistedState
	if err := json.Unmarshal(b, &p); err != nil {
		return fmt.Errorf("corrupt state file: %w", err)
	}
	if p.Version != stateVersion {
		return fmt.Errorf("unsupported state version %d", p.Version)
	}
	s.tasks = p.Tasks
	if s.tasks == nil {
		s.tasks = map[string]*Task{}
	}
	s.extIndex = map[string]string{}
	for id, t := range s.tasks {
		if t.ExternalID != "" {
			s.extIndex[t.ExternalID] = id
		}
	}
	return nil
}

// saveLocked atomically rewrites state.json. Caller must hold mu (write).
func (s *store) saveLocked() error {
	p := persistedState{
		Version:       stateVersion,
		Tasks:         s.tasks,
		ExternalIndex: s.extIndex,
	}
	b, err := json.MarshalIndent(&p, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(s.statePath(), b, 0o644)
}

// atomicWriteFile writes data to a temp file in the same directory and renames
// it into place, so readers never observe a partially written file.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// ---- content-addressed output objects (caller holds mu) ----

// putOutputLocked stores payload under its digest. An identical object already
// present is a no-op; the digest must have been verified by the caller.
func (s *store) putOutputLocked(tid, digest string, payload []byte) error {
	if !validDigest(digest) {
		return fmt.Errorf("%w: digest is not a sha256 hex string", ErrInvalidInput)
	}
	path := s.blobPath(tid, digest)
	if _, err := os.Stat(path); err == nil {
		return nil // already stored; content-addressed => identical bytes
	}
	if err := atomicWriteFile(path, payload, 0o644); err != nil {
		return err
	}
	return nil
}

func (s *store) readOutputLocked(tid, digest string) ([]byte, error) {
	b, err := os.ReadFile(s.blobPath(tid, digest))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: output %s", ErrNotFound, digest)
	}
	return b, err
}

// listOutputDigestsLocked returns the digest-named objects currently stored
// for a task.
func (s *store) listOutputDigestsLocked(tid string) ([]string, error) {
	entries, err := os.ReadDir(s.blobsDir(tid))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && validDigest(e.Name()) {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

func (s *store) removeOutputLocked(tid, digest string) error {
	err := os.Remove(s.blobPath(tid, digest))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// removeAllOutputsLocked deletes every staged output of a task (used on
// cancellation). Missing directory is fine.
func (s *store) removeAllOutputsLocked(tid string) error {
	dir := s.blobsDir(tid)
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o755)
}

// ---- immutable manifests (caller holds mu) ----

// putManifestLocked writes the manifest once. Because the manifest ID is
// derived from its content, an existing file with identical content (e.g. a
// retry after a crash between the manifest write and the state snapshot) is
// accepted as an idempotent no-op. A same-ID/different-content file is
// refused as an immutability violation.
func (s *store) putManifestLocked(tid string, m *Manifest) error {
	path := s.manifestPath(tid, m.ID)
	if b, err := os.ReadFile(path); err == nil {
		var existing Manifest
		if jsonErr := json.Unmarshal(b, &existing); jsonErr == nil && existing.Digest == m.Digest {
			return nil
		}
		return fmt.Errorf("manifest %s already exists with different content", m.ID)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(path, b, 0o444)
}

func (s *store) readManifestLocked(tid, manifestID string) (*Manifest, error) {
	b, err := os.ReadFile(s.manifestPath(tid, manifestID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: manifest %s", ErrNotFound, manifestID)
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("corrupt manifest: %w", err)
	}
	return &m, nil
}
