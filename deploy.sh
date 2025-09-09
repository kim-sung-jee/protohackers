#!/usr/bin/env bash
set -euo pipefail

REMOTE="${1:-}"
SSH_OPTS="${2:-}"
PROFILE="${3:-}"

if [[ -z "${REMOTE}" || -z "${SSH_OPTS}" || -z "${PROFILE}" ]]; then
  exit 1
fi

IMAGE="proto"
PORT="7007"
TAR="${IMAGE}.tar.gz"
REMOTE_DIR="/tmp"
PLATFORM="linux/amd64"

echo "[1/5] Building Docker image ${IMAGE} ..."
if [ -n "$PLATFORM" ]; then
  docker buildx build --platform "$PLATFORM" -t "${IMAGE}" . --load
else
  docker build -t "${IMAGE}" .
fi

echo "[2/5] Saving & compressing image -> ${TAR} ..."
docker save "${IMAGE}" | gzip > "${TAR}"

echo "[3/5] Copying to ${REMOTE}:${REMOTE_DIR}/ ..."
echo "scp ${SSH_OPTS} \"${TAR}\" \"${REMOTE}:${REMOTE_DIR}/\""
scp $SSH_OPTS "${TAR}" "${REMOTE}:${REMOTE_DIR}/"

echo "[4/5] Loading & running on remote ..."
ssh $SSH_OPTS "${REMOTE}" bash -s <<EOF
set -euo pipefail

# Install Docker if missing (Debian/Ubuntu)
if ! command -v docker >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update
    sudo apt-get install -y docker.io
    sudo systemctl enable --now docker
  else
    echo "Docker not found and apt-get not available. Please install Docker manually." >&2
    exit 1
  fi
fi

sudo docker load -i "${REMOTE_DIR}/${TAR}"
sudo docker rm -f ${IMAGE} 2>/dev/null || true
sudo docker run -d --name ${IMAGE} \
  --restart unless-stopped \
  -e SPRING_PROFILES_ACTIVE=${PROFILE} \
  -p 7007:7007/tcp \
  -p 7007:7007/udp \
  ${IMAGE}

# Optional cleanup of the uploaded tar
# rm -f "${REMOTE_DIR}/${TAR}" || true
EOF
