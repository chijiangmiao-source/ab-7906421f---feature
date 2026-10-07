"""容器健康检查探针：以退出码反映健康状态。"""

from __future__ import annotations

import os
import sys
import urllib.request

port = os.environ.get("PORT", "8080")
try:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2) as resp:
        sys.exit(0 if resp.status == 200 else 1)
except OSError:
    sys.exit(1)
