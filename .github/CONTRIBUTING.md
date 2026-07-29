# Contributing to Trein Video

## Development Workflow

This project uses Git hooks to enforce code quality standards automatically.

### Git Hooks

Two Git hooks are configured in `.git/hooks/`:

#### 1. **pre-commit** (Auto-fixes formatting)
Runs automatically before each commit:
- ✅ Runs `cargo fmt` to auto-fix formatting issues
- ✅ Stages formatted changes automatically if needed
- No manual action required

#### 2. **pre-push** (Quality gate before push)
Runs automatically before pushing to GitHub:
- ✅ Checks code format with `cargo fmt --check`
- ✅ Runs clippy linting with `-D warnings` (treats warnings as errors)
- ✅ Runs full test suite with `cargo test --lib`

**If any check fails**, the push is blocked until you fix the issues.

### Common Commands

```bash
# Run all tests locally
cargo test --lib

# Auto-fix formatting issues
cargo fmt

# Check code quality (same as pre-push hook)
cargo clippy --all-targets -- -D warnings

# Force push (bypass hooks if absolutely necessary)
git push --no-verify
```

### Workflow

1. **Make changes** to your code
2. **Commit** (`git commit`) - pre-commit hook runs automatically
   - Formats your code automatically if needed
3. **Push** (`git push`) - pre-push hook runs automatically
   - Verifies tests, clippy, and format
   - Blocks push if checks fail
4. **Fix any issues** shown by pre-push hook
5. **Commit fixes** and **push again**

### Skipping Hooks (Emergency Only)

If you absolutely need to bypass hooks:

```bash
# Skip pre-commit hook
git commit --no-verify

# Skip pre-push hook  
git push --no-verify
```

⚠️ **Use `--no-verify` only in emergencies.** CI/CD will still catch issues.

## Code Quality Standards

- **Format**: Rust standard via `cargo fmt`
- **Linting**: All clippy warnings treated as errors (`-D warnings`)
- **Testing**: All unit tests must pass
- **Commits**: Reference ticket numbers when applicable (e.g., `[#26] Fix issue`)

## GitHub Actions

Two workflows run automatically on all pushes and pull requests:

- **ci.yml** - Tests, clippy, format checks (per commit)
- **release.yml** - Multi-platform binary builds (on release publish)

Local pre-push verification matches these CI checks, so if it passes locally, it will pass CI.
