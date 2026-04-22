# Project Structure Notes

This repository is organized to support clean local development and upcoming CI/CD.

## Source Layout

- `src/` - application source code
- `main.py` - runtime entrypoint
- `tests/` - automated tests (placeholder folder for CI)
- `.github/workflows/` - CI/CD pipeline definitions (placeholder folder)
- `docs/` - project and operational documentation

## Runtime Files

The bot generates runtime artifacts during execution (logs, sessions, trade books).
These are intentionally ignored in git via `.gitignore`.

If you need to keep a local snapshot for debugging, store it outside git-tracked files
or commit only intentionally curated samples.
