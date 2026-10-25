#!/usr/bin/env bash
set -euo pipefail

# Named volumes (and the ~/.config parent Docker creates for the gh mount)
# are root-owned; hand them to vscode.
sudo chown vscode:vscode /home/vscode/.config /home/vscode/.cache
sudo chown -R vscode:vscode /home/vscode/.claude /home/vscode/.config/gh \
  /home/vscode/.cache/uv

# The base image ships a Yarn apt repo with an expired signing key, which
# makes apt-get update fail. Yarn isn't needed here, so drop the source.
sudo rm -f /etc/apt/sources.list.d/yarn.list
sudo apt-get update
sudo apt-get install -y --no-install-recommends sqlite3
sudo rm -rf /var/lib/apt/lists/*

python -m pip install --upgrade pip
python -m pip install --user uv==0.13.5
export PATH="$HOME/.local/bin:$PATH"
(cd "$(dirname "$0")/.." && uv sync --extra dev --extra loadtest)

# Superpowers lives only inside the container. The config volume keeps it
# across rebuilds, so a failure here (e.g. no network) must not break setup.
if ! claude plugin list 2>/dev/null | grep -q superpowers; then
  claude plugin marketplace add obra/superpowers-marketplace || true
  claude plugin install superpowers@superpowers-marketplace || true
fi
