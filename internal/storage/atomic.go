package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func writeBytes(path string, data []byte, mode os.FileMode) error {
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := handle.Write(data); err != nil {
		_ = handle.Close()
		return err
	}
	if err := handle.Sync(); err != nil {
		_ = handle.Close()
		return err
	}
	return handle.Close()
}

func writeJSONAtomic(path string, value any, mode os.FileMode) error {
	payload, err := marshalJSON(value)
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	temp, err := os.CreateTemp(directory, ".write-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer func() {
		_ = os.Remove(tempName)
	}()
	if err := writeTempJSON(temp, payload, mode); err != nil {
		return err
	}
	if err := os.Rename(tempName, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

// writeJSONExclusive publishes a JSON file that must not already exist. The
// final link step is atomic, so a concurrent creator either wins alone or
// receives os.ErrExist; no partially written record is ever visible.
func writeJSONExclusive(path string, value any, mode os.FileMode) error {
	payload, err := marshalJSON(value)
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	temp, err := os.CreateTemp(directory, ".write-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer func() {
		_ = os.Remove(tempName)
	}()
	if err := writeTempJSON(temp, payload, mode); err != nil {
		return err
	}
	if err := os.Link(tempName, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func marshalJSON(value any) ([]byte, error) {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(payload, '\n'), nil
}

func writeTempJSON(temp *os.File, payload []byte, mode os.FileMode) error {
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(payload); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	return temp.Close()
}

func readJSON(path string, target any) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		return err
	}
	return nil
}
