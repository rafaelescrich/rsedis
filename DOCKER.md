# Dockerfile Options for rsedis

## Available Dockerfiles

### 1. Dockerfile (Default - Debian Slim)
**Best for**: Production use with health checks
- **Base**: Debian Bookworm Slim
- **Size**: ~80-100MB
- **Features**: Health checks, shell access, debugging tools
- **Use**: `docker build -t rsedis:latest .`

### 2. Dockerfile.distroless (Ultra-Minimal)
**Best for**: Maximum security and minimal footprint
- **Base**: Google Distroless
- **Size**: ~20-30MB
- **Features**: No shell, no package manager, minimal attack surface
- **Use**: `docker build -f Dockerfile.distroless -t rsedis:distroless .`
- **Note**: No health checks (no shell), use external monitoring

### 3. Dockerfile.debian (Alternative Debian)
**Best for**: Compatibility testing
- **Base**: Debian Bookworm Slim
- **Size**: ~80-100MB
- **Features**: Same as default Dockerfile
- **Use**: `docker build -f Dockerfile.debian -t rsedis:debian .`

## Quick Start

```bash
# Build default (Debian Slim)
docker build -t rsedis:latest .

# Build distroless (ultra-minimal)
docker build -f Dockerfile.distroless -t rsedis:distroless .

# Run
docker run -d -p 6379:6379 -v rsedis-data:/data rsedis:latest

# Or use docker-compose
docker-compose up -d
```

## Image Size Comparison

- **Distroless**: ~20-30MB (no shell, minimal)
- **Debian Slim**: ~80-100MB (with health checks, debugging)
- **Alpine**: ~15-20MB (but musl compatibility issues)

## Recommendations

- **Production**: Use `Dockerfile` (Debian Slim) for health checks and debugging
- **Security-critical**: Use `Dockerfile.distroless` for minimal attack surface
- **Development**: Use `Dockerfile` for easier debugging

