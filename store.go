package godataredaction

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// stateStore 抽象任务状态与结果清单的持久化。
// fileStore 为磁盘实现（生产使用，崩溃可恢复）；memStore 为测试使用的内存实现，
// 两者承载相同的序列化边界：管理器持有的状态一律以整体 JSON 原子落盘。
type stateStore interface {
	saveTask(t *Task) error
	loadTasks() ([]*Task, error)
	// saveManifest 写不可变清单。清单按内容寻址，相同 ID 必然内容相同，重复写视为成功。
	saveManifest(m *Manifest) error
	loadManifests() ([]*Manifest, error)
}

// envelope 是磁盘上的版本化封装，便于将来演进存储格式。
type envelope struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

func atomicWriteJSON(path string, v any) (err error) {
	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return err
	}
	// 同步目录项，保证崩溃后 rename 不丢。
	if dir, derr := os.Open(filepath.Dir(path)); derr == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

func readJSONFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// fileStore 是基于本地目录的 stateStore。
//
// 目录布局：
//
//	root/tasks/<task-id>.json       —— 每个任务一个文件，整体覆盖写（原子 rename）
//	root/manifests/<manifest-id>.json —— 不可变结果清单，只写一次
//	root/outputs/<sha256>           —— 内容寻址的分片输出（见 outputStore）
type fileStore struct {
	root string
}

func newFileStore(root string) *fileStore {
	return &fileStore{root: root}
}

func (f *fileStore) taskPath(id string) string { return filepath.Join(f.root, "tasks", id+".json") }
func (f *fileStore) manifestPath(id string) string {
	return filepath.Join(f.root, "manifests", id+".json")
}

func (f *fileStore) saveTask(t *Task) error {
	return atomicWriteJSON(f.taskPath(t.ID), envelope{Kind: "task/v1", Data: mustMarshal(t)})
}

func (f *fileStore) loadTasks() ([]*Task, error) {
	dir := filepath.Join(f.root, "tasks")
	names, err := readDirNames(dir)
	if err != nil {
		return nil, err
	}
	out := make([]*Task, 0, len(names))
	for _, name := range names {
		var env envelope
		if err := readJSONFile(filepath.Join(dir, name), &env); err != nil {
			return nil, fmt.Errorf("load task %s: %w", name, err)
		}
		var t Task
		if err := json.Unmarshal(env.Data, &t); err != nil {
			return nil, fmt.Errorf("decode task %s: %w", name, err)
		}
		out = append(out, &t)
	}
	return out, nil
}

func (f *fileStore) saveManifest(m *Manifest) error {
	path := f.manifestPath(m.ID)
	if _, err := os.Stat(path); err == nil {
		return nil // 内容寻址：同 ID 即同内容，清单不可变
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return atomicWriteJSON(path, envelope{Kind: "manifest/v1", Data: mustMarshal(m)})
}

func (f *fileStore) loadManifests() ([]*Manifest, error) {
	dir := filepath.Join(f.root, "manifests")
	names, err := readDirNames(dir)
	if err != nil {
		return nil, err
	}
	out := make([]*Manifest, 0, len(names))
	for _, name := range names {
		var env envelope
		if err := readJSONFile(filepath.Join(dir, name), &env); err != nil {
			return nil, fmt.Errorf("load manifest %s: %w", name, err)
		}
		var m Manifest
		if err := json.Unmarshal(env.Data, &m); err != nil {
			return nil, fmt.Errorf("decode manifest %s: %w", name, err)
		}
		out = append(out, &m)
	}
	return out, nil
}

func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // 仅可能发生于结构体设计错误
	}
	return b
}
