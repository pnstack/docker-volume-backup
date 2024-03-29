# docker-volume-backup

example

```yaml
version: "3.8"
services:
  app:
    image: ubuntu
    volumes:
      - app:/app
      - app1:/app1
    working_dir: /app
    stdin_open: true
    tty: true
    command: tail -F anything

  backup:
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - app:/tmp/backups/app:ro
      - app1:/tmp/backups/app1:ro
      - ./outputs:/tmp/outputs
      # save logs to host

    environment:
      - BACKUP_DIR=/tmp/backups
      - OUTPUT_DIR=/tmp/outputs
      - S3_ENDPOINT=
      - S3_BUCKET=backups
      - S3_ACCESS_KEY=
      - S3_SECRET_KEY=
      - S3_PREFIX=docker
      - SECOND_INTERVAL=60 // backup interval in seconds

volumes:
  app:
  app1:
```
