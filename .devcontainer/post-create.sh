#!/usr/bin/env bash
set -euo pipefail

# Named volumes are created root-owned; hand the Claude and gh config dirs
# to vscode.
sudo chown -R vscode:vscode /home/vscode/.claude /home/vscode/.config/gh

# The base image ships a Yarn apt repo with an expired signing key, which
# makes apt-get update fail. Yarn isn't needed here, so drop the source.
sudo rm -f /etc/apt/sources.list.d/yarn.list
sudo apt-get update
sudo apt-get install -y --no-install-recommends sqlite3
sudo rm -rf /var/lib/apt/lists/*

python -m pip install --upgrade pip

# Superpowers lives only inside the container. The config volume keeps it
# across rebuilds, so a failure here (e.g. no network) must not break setup.
if ! claude plugin list 2>/dev/null | grep -q superpowers; then
  claude plugin marketplace add obra/superpowers-marketplace || true
  claude plugin install superpowers@superpowers-marketplace || true
fi
