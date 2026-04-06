# Contributing

## Before Opening A Pull Request

- keep private customer data, metadata dumps, and SQLite files out of Git
- do not commit `.env` or connection strings
- prefer changes that improve the reusable framework, not one-off project hacks
- if a change depends on a concrete 1C configuration, document that assumption clearly

## Development

1. Create a branch from `main`.
2. Make focused changes.
3. Run at least:

```bash
python -m compileall .
```

4. Update documentation when behavior changes.
5. Open a pull request with context on source and target assumptions.

## Pull Request Expectations

- explain what changed
- explain why the change is generic enough for the public repository
- mention any configuration-specific behavior or residual risks
