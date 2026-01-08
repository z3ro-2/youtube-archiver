Docker assets for youtube-archiver.

Quick start
- Build: `docker build -f docker/Dockerfile -t youtube-archiver:latest .`
- Compose: use `docker/docker-compose.yml.example` as your base.

Ports
- Internal: 8000
- Suggested host mapping: 8090

Volumes + paths
- `/config` → config JSON
- `/downloads` → completed media
- `/data` → SQLite + temp dirs
- `/logs` → logs
- `/tokens` → OAuth tokens + client secrets

Use relative paths inside `config.json` (e.g. `folder: "YouTube/Channel"`).

Version build arg
```bash
docker build -f docker/Dockerfile --build-arg YT_ARCHIVER_VERSION=1.1.0 -t youtube-archiver:latest .
```

Notes
- Bind to all interfaces in containers with `YT_ARCHIVER_HOST=0.0.0.0` if needed.
- Consider running as a non-root user with a fixed UID/GID to match volume permissions.
