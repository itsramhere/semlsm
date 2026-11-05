package semlsm

import "testing"

// Trivial smoke test required by P0.4. Real API tests arrive with P5.
func TestBootstrap(t *testing.T) {
	if 1+1 != 2 {
		t.Fatal("arithmetic broken")
	}
}
