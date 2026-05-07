// convert-key turns a Dendrite matrix_key.pem into a Synapse signing.key.
//
// Dendrite stores the ed25519 seed in a PEM block of type "MATRIX PRIVATE KEY"
// with a "Key-ID: ed25519:XXXX" header. Synapse (via signedjson) wants a single
// line "ed25519 XXXX <base64-seed>".
package main

import (
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	in := flag.String("in", "", "path to Dendrite matrix_key.pem")
	out := flag.String("out", "", "path to write Synapse signing.key (default: stdout)")
	flag.Parse()
	if *in == "" {
		flag.Usage()
		os.Exit(2)
	}
	data, err := os.ReadFile(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "MATRIX PRIVATE KEY" {
		fmt.Fprintln(os.Stderr, "no MATRIX PRIVATE KEY PEM block found")
		os.Exit(1)
	}
	keyID := block.Headers["Key-ID"]
	keyID = strings.TrimPrefix(keyID, "ed25519:")
	seed := block.Bytes
	if len(seed) == 64 {
		// Some Dendrite versions write the full private key (seed||pub).
		seed = seed[:32]
	}
	line := fmt.Sprintf("ed25519 %s %s\n", keyID, base64.RawStdEncoding.EncodeToString(seed))
	if *out == "" {
		fmt.Print(line)
		return
	}
	if err := os.WriteFile(*out, []byte(line), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
