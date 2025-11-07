# GitHub Actions Secrets Required

## Required Secrets

### GCR (Google Container Registry)
- `GCR_PROJECT_ID` - Your GCP project ID (e.g., `my-project-id`)
- `GCR_SA_KEY` - Service account JSON key with permissions to push to GCR

## Setting up GCR Secrets

1. **Create a GCP Service Account:**
   ```bash
   gcloud iam service-accounts create github-actions \
     --display-name="GitHub Actions Service Account"
   ```

2. **Grant necessary permissions:**
   ```bash
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:github-actions@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/storage.admin"
   
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:github-actions@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/artifactregistry.writer"
   ```

3. **Create and download key:**
   ```bash
   gcloud iam service-accounts keys create key.json \
     --iam-account=github-actions@YOUR_PROJECT_ID.iam.gserviceaccount.com
   ```

4. **Add to GitHub Secrets:**
   - Go to Repository Settings → Secrets and variables → Actions
   - Add `GCR_PROJECT_ID` with your project ID
   - Add `GCR_SA_KEY` with the contents of `key.json`

## Workflow Features

- **Format Check**: Runs `cargo fmt --check` on every push/PR
- **Tests**: Runs `cargo test` on stable and nightly Rust
- **Build**: Builds release binary and uploads as artifact
- **Docker**: Builds and pushes multi-arch images to GCR
- **Release**: Automatically creates GitHub release on version tags

## Manual Release

To create a release manually:
1. Go to Actions → Release Workflow
2. Click "Run workflow"
3. Enter version (e.g., `1.0.0`)
4. This will create tag `v1.0.0` which triggers the release

