# Maintenance policy

- This is `JavaLyHn/DanmakuFlow-Java`. Java development is paused as of 2026-11-06.
- The active Go repository is `JavaLyHn/DanmakuFlow`.
- Do not change Java behavior, refactor Java source, or automatically port Go changes until the user explicitly requests Java work or synchronization.
- On a synchronization request, determine the requested Go revision and feature scope first; keep small, reviewable commits.
- Preserve existing licenses and copyright notices. Do not rewrite prior commit history.
- Resource constraints remain in effect: no automatic infrastructure startup or load tests; bound and serialize any explicitly authorized future validation.
