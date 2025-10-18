from __future__ import annotations
import subprocess
import shlex
from pathlib import Path

def run(
    cmd: str | list[str] | tuple[str, ...],
    cwd: str | Path | None = None,
    capture: bool = False,
    env: dict[str, str] | None = None,
    check: bool = True,
    timeout: float | int | None = None,
    input_data: bytes | None = None
) -> tuple[int, str, str]:
    """Run a shell command.

    Args:
        cmd: The command to execute (string or list of arguments).
        cwd: Working directory to run the command in.
        capture: If True, capture stdout/stderr and return them.
        env: Optional environment variables to set.
        check: If True, raise RuntimeError on non-zero exit code.
        timeout: Optional timeout in seconds.
        input_data: Optional bytes to send to stdin.

    Returns:
        If capture=False → int (return code)
        If capture=True → (returncode, stdout, stderr)
    """
    if not isinstance(cmd, (list, tuple)):
        cmd = shlex.split(cmd)

    try:
        if capture:
            p = subprocess.run(
                cmd,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,  # we'll handle the error ourselves
                timeout=timeout,
                input=input_data,
            )
            out = p.stdout.decode(errors="ignore")
            err = p.stderr.decode(errors="ignore")
            if check and p.returncode != 0:
                raise RuntimeError(
                    f"Command failed: {' '.join(cmd)}\n"
                    f"exit={p.returncode}\n"
                    f"stdout={out}\n"
                    f"stderr={err}"
                )
            return p.returncode, out, err
        else:
            p = subprocess.run(
                cmd,
                cwd=cwd,
                env=env,
                check=check,
                timeout=timeout,
                input=input_data,
            )
            return p.returncode, '', ''
    except subprocess.TimeoutExpired:
        raise
