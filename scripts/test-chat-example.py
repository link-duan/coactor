#!/usr/bin/env python3

import signal
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(*command: str) -> None:
    subprocess.run(command, cwd=ROOT, check=True)


def read(path: Path) -> str:
    return path.read_text(errors="replace")


def wait_for(path: Path, text: str, process: subprocess.Popen[str], timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if text in read(path):
            return
        if process.poll() is not None:
            raise RuntimeError(f"process exited while waiting for {text!r}:\n{read(path)}")
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {text!r}:\n{read(path)}")


def wait_for_line(
    path: Path,
    parts: tuple[str, ...],
    process: subprocess.Popen[str],
    timeout: float = 10,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(all(part in line for part in parts) for line in read(path).splitlines()):
            return
        if process.poll() is not None:
            raise RuntimeError(f"process exited while waiting for {parts!r}:\n{read(path)}")
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {parts!r}:\n{read(path)}")


def wait_success(process: subprocess.Popen[str], name: str, log: Path) -> None:
    code = process.wait(timeout=10)
    if code != 0:
        raise RuntimeError(f"{name} exited with code {code}:\n{read(log)}")


def stop(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.kill()
    process.wait()


def main() -> None:
    server = alice = bob = charlie = None
    passed = False
    with tempfile.TemporaryDirectory() as directory:
        logs = Path(directory)
        server_path = logs / "server.log"
        alice_path = logs / "alice.log"
        bob_path = logs / "bob.log"
        charlie_path = logs / "charlie.log"
        server_log = alice_log = bob_log = charlie_log = None

        run("docker", "compose", "down", "--volumes", "--remove-orphans")
        try:
            run("docker", "compose", "up", "--detach", "--wait")
            run("cargo", "build", "--locked", "-p", "coactor", "--examples")

            server_log = server_path.open("w")
            alice_log = alice_path.open("w")
            bob_log = bob_path.open("w")
            charlie_log = charlie_path.open("w")
            server = subprocess.Popen(
                [ROOT / "target/debug/examples/chat_server"],
                cwd=ROOT,
                stdout=server_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            time.sleep(1)
            if server.poll() is not None:
                raise RuntimeError(f"chat Server failed:\n{read(server_path)}")

            bob = subprocess.Popen(
                [ROOT / "target/debug/examples/chat_client", "integration", "bob"],
                cwd=ROOT,
                stdin=subprocess.PIPE,
                stdout=bob_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            wait_for(server_path, "username=bob", server)

            alice = subprocess.Popen(
                [ROOT / "target/debug/examples/chat_client", "integration", " alice "],
                cwd=ROOT,
                stdin=subprocess.PIPE,
                stdout=alice_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            wait_for(server_path, "username=alice", server)

            duplicate = subprocess.run(
                [ROOT / "target/debug/examples/chat_client", "integration", "alice"],
                cwd=ROOT,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=10,
            )
            if duplicate.returncode == 0 or "username is already in use" not in duplicate.stdout:
                raise AssertionError(f"duplicate username was not rejected:\n{duplicate.stdout}")

            assert alice.stdin is not None
            alice.stdin.write("\x1b[2J\n")
            alice.stdin.flush()
            wait_for(alice_path, "message must contain 1-1000 printable characters", alice)
            alice.stdin.write("hello from alice\n")
            alice.stdin.flush()
            wait_for(bob_path, "<alice> hello from alice", bob)
            if "\x1b[2J" in read(bob_path):
                raise AssertionError(f"Bob received a rejected control sequence:\n{read(bob_path)}")

            alice.send_signal(signal.SIGINT)
            wait_success(alice, "Alice", alice_path)
            wait_for(alice_path, "* alice left", alice)
            wait_for(bob_path, "* alice left", bob)
            wait_for_line(server_path, ("member left", "username=alice"), server)

            charlie = subprocess.Popen(
                [ROOT / "target/debug/examples/chat_client", "integration", "charlie"],
                cwd=ROOT,
                stdin=subprocess.PIPE,
                stdout=charlie_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            wait_for(server_path, "username=charlie", server)
            assert charlie.stdin is not None
            charlie.stdin.close()
            wait_success(charlie, "Charlie", charlie_path)
            wait_for(charlie_path, "* charlie left", charlie)
            wait_for(bob_path, "* charlie left", bob)
            wait_for_line(server_path, ("member left", "username=charlie"), server)

            assert bob.stdin is not None
            bob.stdin.write("/quit\n")
            bob.stdin.flush()
            wait_success(bob, "Bob", bob_path)
            wait_for(bob_path, "* bob left", bob)
            wait_for_line(server_path, ("member left", "username=bob"), server)

            server.send_signal(signal.SIGINT)
            wait_success(server, "chat Server", server_path)

            passed = True
            print("chat example integration test passed")
        finally:
            stop(alice)
            stop(bob)
            stop(charlie)
            stop(server)
            for log in (server_log, alice_log, bob_log, charlie_log):
                if log is not None:
                    log.close()
            if not passed:
                subprocess.run(
                    ["docker", "compose", "logs", "--no-color"],
                    cwd=ROOT,
                    check=False,
                )
                for path in (server_path, alice_path, bob_path, charlie_path):
                    if path.exists():
                        print(f"--- {path.name} ---\n{read(path)}")
            subprocess.run(
                ["docker", "compose", "down", "--volumes", "--remove-orphans"],
                cwd=ROOT,
                check=False,
            )


if __name__ == "__main__":
    main()
