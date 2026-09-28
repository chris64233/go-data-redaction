package godataredaction

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// outputStore 是内容寻址（content-addressed）的脱敏输出存储。
//
// 关键性质：
//   - PutOutput 时由内容计算 sha256 摘要，文件名即摘要 —— 相同内容天然去重；
//   - 输出先落盘，但在任务发布（进入成功清单）之前，对外接口无法读取它们；
//   - 删除按摘要进行：只有未被任何已发布清单引用的输出才可能被 GC 回收。
type outputStore struct {
	dir   string
	clock Clock
}

func newOutputStore(root string, clock Clock) *outputStore {
	return &outputStore{dir: filepath.Join(root, "outputs"), clock: clock}
}

// put 写入一段输出内容，返回引用与摘要。采用临时文件 + rename 保证原子落盘。
func (s *outputStore) put(data []byte) (ref, digest string, size int64, err error) {
	sum := sha256.Sum256(data)
	digest = hex.EncodeToString(sum[:])
	path := s.path(digest)
	if _, statErr := os.Stat(path); statErr == nil {
		return digest, digest, int64(len(data)), nil // 相同内容已存在，天然幂等
	}
	if err = os.MkdirAll(s.dir, 0o755); err != nil {
		return "", "", 0, err
	}
	tmp, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return "", "", 0, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后 Remove 只是无害的 no-op
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return "", "", 0, err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return "", "", 0, err
	}
	if err = tmp.Close(); err != nil {
		return "", "", 0, err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return "", "", 0, err
	}
	// 以注入时钟为准记录落盘时间，供 GC 宽限期判定（生产时钟即墙钟）。
	if err = os.Chtimes(path, s.clock.Now(), s.clock.Now()); err != nil {
		return "", "", 0, err
	}
	return digest, digest, int64(len(data)), nil
}

// open 按摘要打开输出；摘要非法或不存在返回 ErrNotFound。
func (s *outputStore) open(digest string) (io.ReadCloser, int64, error) {
	if !isHexDigest(digest) {
		return nil, 0, ErrNotFound
	}
	f, err := os.Open(s.path(digest))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

// exists 判断摘要对应的输出是否存在。
func (s *outputStore) exists(digest string) bool {
	if !isHexDigest(digest) {
		return false
	}
	_, err := os.Stat(s.path(digest))
	return err == nil
}

// verify 重新计算输出内容的摘要并与期望摘要比对，用于发布前的摘要校验。
func (s *outputStore) verify(expectedDigest string) error {
	f, _, err := s.open(expectedDigest)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != expectedDigest {
		return fmt.Errorf("%w: %s", ErrOutputMismatch, expectedDigest)
	}
	return nil
}

// list 枚举存储中全部输出摘要。
func (s *outputStore) list() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".tmp-") {
			continue
		}
		if isHexDigest(e.Name()) {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// modTime 返回输出落盘（最后修改）时间，供 GC 宽限期判定。
func (s *outputStore) modTime(digest string) (time.Time, error) {
	st, err := os.Stat(s.path(digest))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return time.Time{}, ErrNotFound
		}
		return time.Time{}, err
	}
	return st.ModTime(), nil
}

// delete 删除一个输出。不存在视为成功（幂等）。
func (s *outputStore) delete(digest string) error {
	if !isHexDigest(digest) {
		return fmt.Errorf("%w: %q", ErrInvalidArgument, digest)
	}
	err := os.Remove(s.path(digest))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *outputStore) path(digest string) string {
	return filepath.Join(s.dir, digest)
}

func isHexDigest(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}
