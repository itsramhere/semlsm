# Decisions log

Append only. One line per decision with task reference.

- P0.1: go1.26.3 present (>= 1.22); go.mod set to `go 1.26.3` as created by `go mod init`. No feature beyond 1.22 planned.
- P0.1: CGO_ENABLED=1, gcc 14.2.0 present; race detector verified working in P0.4; `-race` used at gates.
- fix(P1): contract test Stat-after-close asserts "must fail" only: os.File returns os.ErrClosed on Unix but an invalid-handle error on Windows (observed). MemFS always returns os.ErrClosed.
- fix(P1): truncate+write contract expectation is the zero-padded os.File result: Truncate does not reset the handle offset on either OS.
- fix(P2): R4 exception logged per M-006: TestWALTornAtEveryOffsetOfFinalFrame previously demanded ErrCorruption at a cut exactly on a frame boundary; correct semantics (replay stops cleanly at EOF) is no error and the earlier frames in both modes. Test fixed accordingly.
- 2026-11-06T23:30+05:30 no verdict for G-025 after two bounded A10 waits (32 min); continuing per A10 step 3 with P8.2. Late CHANGES on G-025 still applies before further new tasks; TO_GLM.md checked at each task boundary.
