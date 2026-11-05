# Decisions log

Append only. One line per decision with task reference.

- P0.1: go1.26.3 present (>= 1.22); go.mod set to `go 1.26.3` as created by `go mod init`. No feature beyond 1.22 planned.
- P0.1: CGO_ENABLED=1, gcc 14.2.0 present; race detector verified working in P0.4; `-race` used at gates.
