# GitHub Actions CI/CD Setup

This repository includes comprehensive GitHub Actions workflows for continuous integration and deployment.

## Workflows

### 1. CI/CD Pipeline (`.github/workflows/ci.yml`)

**Triggers:**
- Push to `master` or `main` branches
- Pull requests to `master` or `main`
- Version tags (`v*`)

**Jobs:**

#### Format Check
- Runs `cargo fmt --all -- --check`
- Ensures code follows Rust formatting standards

#### Test Suite
- Runs `cargo test --all` on both stable and nightly Rust
- Uses caching for faster builds
- Matrix strategy for multiple Rust versions

#### Build Release
- Builds release binary (`cargo build --release`)
- Uploads binary as artifact for releases
- Only runs after format check and tests pass

#### Docker Build
- Builds multi-arch Docker images (linux/amd64, linux/arm64)
- Pushes to Google Container Registry (GCR)
- Builds both Debian and Distroless variants
- Only runs on pushes (not PRs)
- Requires GCR secrets to be configured

#### Release
- Automatically creates GitHub release when version tag is pushed
- Generates changelog from git commits
- Uploads release artifacts
- Only runs on version tags (`v*`)

### 2. Release Workflow (`.github/workflows/release.yml`)

**Manual workflow** for creating releases:
- Go to Actions → Release Workflow → Run workflow
- Enter version number (e.g., `1.0.0`)
- Creates tag `v1.0.0` which triggers the CI/CD pipeline

## Setup Instructions

### 1. GitHub Secrets

Add these secrets in Repository Settings → Secrets and variables → Actions:

- `GCR_PROJECT_ID` - Your Google Cloud Project ID
- `GCR_SA_KEY` - Service account JSON key (see `.github/workflows/README.md`)

### 2. GCP Service Account Setup

See `.github/workflows/README.md` for detailed instructions on:
- Creating a service account
- Granting necessary permissions
- Generating and adding the key

### 3. Testing the Workflow

1. **Test format check:**
   ```bash
   cargo fmt --all -- --check
   ```

2. **Test locally:**
   ```bash
   cargo test --all
   ```

3. **Test Docker build:**
   ```bash
   docker build -t rsedis:test .
   ```

### 4. Creating a Release

**Option 1: Manual workflow**
1. Go to Actions → Release Workflow
2. Click "Run workflow"
3. Enter version (e.g., `1.0.0`)
4. This creates tag `v1.0.0`

**Option 2: Git tag**
```bash
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

## Workflow Features

✅ **Format checking** - Ensures code style consistency  
✅ **Multi-version testing** - Tests on stable and nightly Rust  
✅ **Caching** - Faster builds with dependency caching  
✅ **Multi-arch Docker** - Builds for AMD64 and ARM64  
✅ **Automatic releases** - Creates GitHub releases on tags  
✅ **Artifact uploads** - Stores build artifacts  
✅ **GCR integration** - Pushes to Google Container Registry  

## Docker Images

After a successful build, images are available at:
- `gcr.io/YOUR_PROJECT_ID/rsedis:latest`
- `gcr.io/YOUR_PROJECT_ID/rsedis:v1.0.0` (on releases)
- `gcr.io/YOUR_PROJECT_ID/rsedis:distroless-v1.0.0` (distroless variant)

## Troubleshooting

### Docker build fails
- Check that `GCR_PROJECT_ID` and `GCR_SA_KEY` secrets are set
- Verify service account has `storage.admin` and `artifactregistry.writer` roles
- Check GCR authentication: `gcloud auth configure-docker gcr.io`

### Tests fail
- Run tests locally: `cargo test --all`
- Check Rust version compatibility
- Review test output in Actions logs

### Format check fails
- Run `cargo fmt --all` locally to auto-format
- Commit the formatted code

