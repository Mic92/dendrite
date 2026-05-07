package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestPathLayouts(t *testing.T) {
	if got, _ := dendriteMediaPath("/d", "qwerty"); got != filepath.Join("/d", "q", "w", "erty", "file") {
		t.Fatalf("dendriteMediaPath = %q", got)
	}
	if _, ok := dendriteMediaPath("/d", "ab"); ok {
		t.Fatal("dendriteMediaPath: short hash accepted")
	}
	if got, _ := synapseLocalPath("/s", "abcdef"); got != filepath.Join("/s", "local_content", "ab", "cd", "ef") {
		t.Fatalf("synapseLocalPath = %q", got)
	}
	if got, _ := synapseRemotePath("/s", "matrix.org", "abcdef"); got != filepath.Join("/s", "remote_content", "matrix.org", "ab", "cd", "ef") {
		t.Fatalf("synapseRemotePath = %q", got)
	}
	if _, ok := synapseLocalPath("/s", "abcd"); ok {
		t.Fatal("synapseLocalPath: short id accepted")
	}
}

func TestSha256HexFromBase64Hash(t *testing.T) {
	sum := sha256.Sum256([]byte("hello"))
	b64 := base64.RawURLEncoding.EncodeToString(sum[:])
	got := sha256HexFromBase64Hash(b64)
	if got != hex.EncodeToString(sum[:]) {
		t.Fatalf("got %v want %v", got, hex.EncodeToString(sum[:]))
	}
	if sha256HexFromBase64Hash("not base64!!!") != nil {
		t.Fatal("malformed input must return nil")
	}
	if sha256HexFromBase64Hash(base64.RawURLEncoding.EncodeToString([]byte("short"))) != nil {
		t.Fatal("wrong-length input must return nil")
	}
}

func TestPlaceFunctions(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	if err := os.WriteFile(src, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	for name, fn := range map[string]func(string, string) error{
		"copy":     placeCopy,
		"hardlink": placeHardlink,
		"symlink":  placeSymlink,
	} {
		t.Run(name, func(t *testing.T) {
			dst := filepath.Join(tmp, "dst-"+name)
			if err := fn(src, dst); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			b, err := os.ReadFile(dst)
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			if string(b) != "payload" {
				t.Fatalf("%s: content = %q", name, b)
			}
			if name == "copy" {
				// Ensure no leftover .tmp file.
				if _, err := os.Stat(dst + ".tmp"); !os.IsNotExist(err) {
					t.Fatal("copy left .tmp file behind")
				}
			}
		})
	}
}
