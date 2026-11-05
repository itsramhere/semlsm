# Environment

Recorded at P0.1 (2026-11-05, ISO: 2026-11-05T00:00+00 local run start).

- OS: Windows 11 (build 10.0.26200), amd64
- Shell: Git Bash (MINGW64); plan also targets PowerShell
- Go: go1.26.3 windows/amd64 (>= 1.22 required: OK)
- git: 2.52.0.windows.2
- cgo: CGO_ENABLED=1
- gcc: 14.2.0 (MinGW-W64 x86_64-ucrt-posix-seh) on PATH
- Race detector: AVAILABLE (CGO_ENABLED=1 + gcc present; verified by running a
  race test during P0.4 — see PROGRESS.md). `-race` is used at gates; if it ever
  fails, fallback is `-count=20` per A5.
- SEMLSM_LLM_API_KEY: not set (P15 will be skipped per A9)
- Token budget: 300M total; A8 budget table applies.
