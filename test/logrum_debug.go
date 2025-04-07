package test

import (
	"fmt"
	"os"
)

// DebugPrint outputs debug information to stderr.
// This is used for debugging tests and is not part of the production code.
// It can be removed when the tests are stable.
func DebugPrint(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "DEBUG: "+msg+"\n", args...)
}
