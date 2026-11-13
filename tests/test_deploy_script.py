"""Hermetic checks for scripts/deploy.sh.

Docker, the health endpoints, and the source tarball are local fakes. Nothing
here contacts CT105 or GitHub.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import socket
import stat
import sqlite3
import subprocess
import tarfile
import tempfile
import textwrap
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "deploy.sh"
SHA = "0123456789abcdef0123456789abcdef01234567"
CANARY_SECRET = "canary-secret-value"
CANARY_OVERRIDE = "canary-override-secret"
CANARY_TOKEN = "canary-token-value"

FAKE_DOCKER = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json, os, sys
    from pathlib import Path

    argv = sys.argv[1:]
    log_path = Path(os.environ["FAKE_DOCKER_LOG"])
    tags_path = Path(os.environ["FAKE_TAGS_FILE"])
    with log_path.open("a") as fh:
        fh.write(json.dumps(argv) + "\\n")

    def tags():
        return set(json.loads(tags_path.read_text() or "[]"))

    def save(values):
        tags_path.write_text(json.dumps(sorted(values)))

    if not argv:
        sys.exit(2)

    if argv[0] == "compose":
        if len(argv) > 1 and argv[1] == "version":
            print("Docker Compose version v2")
            sys.exit(0)
        if "down" in argv or "prune" in argv:
            sys.exit("refusing dangerous compose command")
        if "up" in argv:
            project = ""
            if "--project-directory" in argv:
                project = argv[argv.index("--project-directory") + 1]
            state = Path(os.environ["FAKE_STATE_FILE"])
            if project == os.environ.get("FAKE_NEW_DIR"):
                state.write_text("new")
            elif project == os.environ.get("FAKE_OLD_DIR"):
                state.write_text("old")
            else:
                state.write_text("other")
            sys.exit(0)
        sys.exit("unexpected compose args")

    if argv[0] == "port":
        output = os.environ.get("FAKE_DOCKER_PORT", "FAIL")
        if output == "FAIL":
            sys.exit(1)
        print(output)
        sys.exit(0)

    if argv[0] == "inspect":
        if "--format" in argv:
            print(os.environ.get("FAKE_OLD_DIR", ""))
        sys.exit(0)

    if argv[0] == "image" and len(argv) > 1 and argv[1] == "inspect":
        ref = argv[-1]
        if ref not in tags():
            sys.exit(1)
        if "--format" in argv:
            print("sha256:" + ("ab" * 32))
        sys.exit(0)

    if argv[0] == "tag":
        src, dst = argv[1], argv[2]
        have = tags()
        if src not in have:
            sys.exit(1)
        have.add(dst)
        save(have)
        sys.exit(0)

    if argv[0] == "build":
        have = tags()
        index = 1
        while index < len(argv):
            if argv[index] == "-t" and index + 1 < len(argv):
                have.add(argv[index + 1])
                index += 2
                continue
            index += 1
        save(have)
        sys.exit(0)

    if argv[0] == "run":
        if "10001:10001" not in argv:
            sys.exit("docker run is missing --user 10001:10001")
        if "restore" in argv:
            sys.exit("restore must not be executed")
        if "verify-backup" in argv:
            print("ok")
            sys.exit(0)
        if "backup" in argv:
            dest = argv[argv.index("--dest") + 1]
            mounts = {}
            index = 0
            while index < len(argv):
                if argv[index] == "-v" and index + 1 < len(argv):
                    host, rest = argv[index + 1].split(":", 1)
                    mounts[rest.split(":", 1)[0]] = host
                    index += 2
                    continue
                index += 1
            host_dir = mounts.get("/backups")
            if not dest.startswith("/backups/") or not host_dir:
                sys.exit("backup destination was not mounted")
            path = Path(host_dir) / dest.rsplit("/", 1)[-1]
            path.write_bytes(b"neurio-backup-bytes\\n")
            print(dest)
            sys.exit(0)
        sys.exit("unexpected docker run")

    sys.exit("unexpected docker command")
    """
)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        path = urlparse(self.path).path
        mode = self.server.mode
        state_path = Path(self.server.state_file)
        fresh = state_path.is_file() and state_path.read_text(encoding="utf-8").strip() == "new"
        if path == "/healthz":
            self._json({"status": "ok"})
            return
        if path == "/readyz":
            ready = not (fresh and mode == "not-ready")
            self._json({"ready": ready, "db_ok": True, "device_fresh": False})
            return
        if path == "/api/v1/status":
            schema = 4 if mode == "numeric" else "4"
            db_ok = True
            if fresh and mode == "upgrade":
                schema = "5"
            elif fresh and mode == "regress":
                schema = "3"
            elif fresh and mode == "db-down":
                db_ok = False
            self._json({
                "db_ok": db_ok,
                "schema_version": schema,
                "device_fresh": False,
            })
            return
        self._json({"error": "not found"}, 404)

    def _json(self, body, code=200):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        return


class DeployScriptTests(unittest.TestCase):
    def test_help_and_argument_errors(self):
        env = {"PATH": "/usr/bin:/bin"}
        help_run = subprocess.run(
            ["bash", str(SCRIPT), "--help"],
            check=False, capture_output=True, text=True, env=env,
        )
        self.assertEqual(help_run.returncode, 0)
        self.assertIn("--dry-run", help_run.stdout)

        missing = subprocess.run(
            ["bash", str(SCRIPT)],
            check=False, capture_output=True, text=True, env=env,
        )
        self.assertEqual(missing.returncode, 2)

        unknown = subprocess.run(
            ["bash", str(SCRIPT), "--apply", SHA],
            check=False, capture_output=True, text=True, env=env,
        )
        self.assertNotEqual(unknown.returncode, 0)
        self.assertIn("unknown option", unknown.stderr)

        bad = subprocess.run(
            ["bash", str(SCRIPT), "not-a-sha"],
            check=False, capture_output=True, text=True, env=env,
        )
        self.assertNotEqual(bad.returncode, 0)
        self.assertIn("hex", bad.stderr)

    def test_script_does_not_name_volume_removal(self):
        for line in SCRIPT.read_text(encoding="utf-8").splitlines():
            code = line.split("#", 1)[0]
            self.assertNotIn("compose down", code)
            self.assertNotRegex(code, r"\bprune\b")
            self.assertNotIn("volume rm", code)

    def test_same_schema_deploys(self):
        result = self._deploy("same")
        self.assertEqual(result.returncode, 0, result.output)
        self.assertIn("schema 4 >= 4", result.output)
        self.assertIn("4 -> 4", result.output)
        self.assertEqual(result.ups, [("new",)])
        self._assert_backup_artifacts(result)
        self._assert_safe_docker(result)

    def test_newer_schema_deploys(self):
        result = self._deploy("upgrade", wrapped=False)
        self.assertEqual(result.returncode, 0, result.output)
        self.assertIn("schema 5 >= 4", result.output)
        self.assertIn("4 -> 5", result.output)
        self._assert_safe_docker(result)

    def test_numeric_schema_deploys(self):
        result = self._deploy("numeric")
        self.assertEqual(result.returncode, 0, result.output)
        self.assertIn("4 -> 4", result.output)

    def test_older_schema_rolls_the_image_back_without_restoring_the_database(self):
        result = self._deploy("regress")
        self.assertEqual(result.returncode, 1, result.output)
        self.assertIn("schema 3 is older than the previous schema 4", result.output)
        self.assertIn("rollback health ok", result.output)
        self.assertIn("Database restore is not applied", result.output)
        self.assertEqual(result.ups, [("new",), ("old",)])
        tags = [argv for argv in result.invocations if argv and argv[0] == "tag"]
        self.assertEqual(tags[0][1:], [
            "neurio-lan-collector:local",
            f"neurio-lan-collector:pre-{SHA}",
        ])
        self.assertEqual(tags[-1][1:], [
            f"neurio-lan-collector:pre-{SHA}",
            "neurio-lan-collector:local",
        ])
        self._assert_safe_docker(result)
        self._assert_backup_artifacts(result)

    def test_db_ok_false_rolls_back(self):
        result = self._deploy("db-down")
        self.assertEqual(result.returncode, 1, result.output)
        self.assertIn("status db_ok is not true", result.output)
        self.assertIn("rollback health ok", result.output)
        self.assertEqual(result.ups, [("new",), ("old",)])
        self._assert_safe_docker(result)

    def test_ready_false_rolls_back(self):
        result = self._deploy("not-ready")
        self.assertEqual(result.returncode, 1, result.output)
        self.assertIn("readyz ready is not true", result.output)
        self.assertEqual(result.ups, [("new",), ("old",)])
        self._assert_safe_docker(result)

    def test_existing_pre_tag_is_not_replaced(self):
        pre = f"neurio-lan-collector:pre-{SHA}"
        result = self._deploy("same", extra_tags=[pre])
        self.assertEqual(result.returncode, 0, result.output)
        tags = [argv for argv in result.invocations if argv and argv[0] == "tag"]
        self.assertEqual(tags, [])
        self.assertIn("keeping existing rollback tag", result.output)

    def test_published_address_comes_from_docker_port(self):
        result = self._deploy(
            None,
            health_base="",
            docker_port="192.168.86.5:8095",
        )
        self.assertEqual(result.returncode, 1, result.output)
        self.assertIn("http://192.168.86.5:8095", result.output)
        self.assertNotIn("http://127.0.0.1", result.output)
        self.assertNotIn("http://192.168.86.20", result.output)
        self.assertEqual(result.ups, [("new",), ("old",)])
        self._assert_safe_docker(result)

    def test_publish_address_in_env_file_is_used_when_docker_port_is_absent(self):
        result = self._deploy(None, health_base="", docker_port="FAIL")
        self.assertEqual(result.returncode, 1, result.output)
        self.assertIn("http://192.168.86.5:8095", result.output)
        self.assertNotIn("http://192.168.86.20", result.output)
        self._assert_safe_docker(result)

    def test_missing_publish_address_stops_before_recreate(self):
        result = self._deploy(
            None,
            health_base="",
            docker_port="FAIL",
            publish_address="",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("192.168.86.5", result.output)
        self.assertNotIn("http://192.168.86.20", result.output)
        self.assertEqual(result.ups, [])
        self.assertFalse(any(argv and argv[0] in {"tag", "build", "run"} for argv in result.invocations))
        self._assert_safe_docker(result)

    def test_example_publish_address_warns(self):
        result = self._deploy(
            None,
            health_base="",
            docker_port="192.168.86.20:8095",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("192.168.86.20", result.output)
        self.assertIn("CT105 is 192.168.86.5", result.output)

    def test_dry_run_writes_nothing(self):
        result = self._deploy("same", dry_run=True)
        self.assertEqual(result.returncode, 0, result.output)
        self.assertIn("DRY-RUN:", result.output)
        self.assertIn("up", result.output)
        self.assertIn("--no-build", result.output)
        self.assertIn("--force-recreate", result.output)
        self.assertIn("--no-deps", result.output)
        self.assertFalse((result.paths["new"]).exists())
        self.assertEqual(list(result.paths["backups"].glob("*")), [])
        self.assertFalse(any(argv and argv[0] in {"tag", "build", "run"} for argv in result.invocations))
        self.assertEqual(result.ups, [])
        self._assert_safe_docker(result)

    def _deploy(
        self,
        mode,
        wrapped=True,
        dry_run=False,
        health_base=None,
        docker_port="FAIL",
        publish_address="192.168.86.5",
        extra_tags=(),
    ):
        tmp = Path(tempfile.mkdtemp(prefix="neurio-deploy-"))
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(tmp)], check=False))
        app = tmp / "opt"
        old = app / "source-old"
        data = app / "data"
        backups = app / "backups"
        new = app / f"source-{SHA}"
        old.mkdir(parents=True)
        data.mkdir()
        backups.mkdir()
        self._sqlite(data / "neurio.sqlite", "4")
        (old / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
        (old / "compose.mqtt.yml").write_text("name: neurio-lan-collector\n", encoding="utf-8")
        (old / "readme.txt").write_text("keep\n", encoding="utf-8")
        (old / "mqtt_password").write_text("do-not-archive\n", encoding="utf-8")
        override = (
            "services:\n"
            "  neurio-recorder:\n"
            "    environment:\n"
            "      NEURIO_CLIENT_SECRET_FILE: /run/secrets/neurio_client_secret\n"
            "    volumes:\n"
            f"      - /opt/neurio-recorder/secrets/{CANARY_OVERRIDE}:/run/secrets/neurio_client_secret:ro\n"
        )
        (old / "compose.cloud.override.yml").write_text(override, encoding="utf-8")
        env_lines = [
            f"DATA_HOST_PATH={data}",
            f"BACKUP_HOST_PATH={backups}",
            "MQTT_USERNAME=neurio",
            f"NEURIO_CLIENT_SECRET={CANARY_SECRET}",
        ]
        if publish_address:
            env_lines.insert(0, f"PUBLISH_ADDRESS={publish_address}")
        (old / ".env").write_text("\n".join(env_lines) + "\n", encoding="utf-8")
        tarball = tmp / "src.tgz"
        self._tarball(tarball, wrapped)

        bin_dir = tmp / "bin"
        bin_dir.mkdir()
        docker = bin_dir / "docker"
        docker.write_text(FAKE_DOCKER, encoding="utf-8")
        docker.chmod(0o755)
        log_path = tmp / "docker.log"
        tags_path = tmp / "tags.json"
        state_path = tmp / "state"
        state_path.write_text("old", encoding="utf-8")
        tags_path.write_text(json.dumps(["neurio-lan-collector:local", *extra_tags]))

        server = None
        base = health_base
        if mode is not None and health_base is None:
            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            server.mode = mode
            server.state_file = str(state_path)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)
            port = server.server_address[1]
            self._wait_port(port)
            base = f"http://127.0.0.1:{port}"

        env = os.environ.copy()
        env["PATH"] = str(bin_dir) + os.pathsep + env.get("PATH", "")
        env["APP_DIR"] = str(app)
        env["DATA_DIR"] = str(data)
        env["DB_PATH"] = str(data / "neurio.sqlite")
        env["LOCK_PATH"] = str(data / "collector.lock")
        env["BACKUP_DIR"] = str(backups)
        env["CURRENT_SOURCE"] = str(old)
        env["SOURCE_TARBALL"] = str(tarball)
        env["HEALTH_WAIT_SECONDS"] = "0"
        env["HEALTH_INTERVAL_SECONDS"] = "0"
        env["HEALTH_CURL_MAX_TIME"] = "1"
        env["GITHUB_TOKEN"] = CANARY_TOKEN
        env["FAKE_DOCKER_LOG"] = str(log_path)
        env["FAKE_TAGS_FILE"] = str(tags_path)
        env["FAKE_STATE_FILE"] = str(state_path)
        env["FAKE_OLD_DIR"] = str(old)
        env["FAKE_NEW_DIR"] = str(new)
        env["FAKE_DOCKER_PORT"] = docker_port
        if base:
            env["HEALTH_BASE"] = base
        else:
            env.pop("HEALTH_BASE", None)
        env.pop("PUBLISH_ADDRESS", None)
        env.pop("GH_TOKEN", None)

        args = ["bash", str(SCRIPT)]
        if dry_run:
            args.append("--dry-run")
        args.append(SHA)
        completed = subprocess.run(
            args, check=False, capture_output=True, text=True, env=env,
        )
        output = completed.stdout + completed.stderr
        invocations = []
        if log_path.is_file():
            invocations = [json.loads(line) for line in log_path.read_text().splitlines() if line]
        ups = []
        for argv in invocations:
            if argv and argv[0] == "compose" and "up" in argv:
                project = argv[argv.index("--project-directory") + 1]
                if project == str(new):
                    ups.append(("new",))
                elif project == str(old):
                    ups.append(("old",))
                else:
                    ups.append(("other",))
        completed.output = output
        completed.invocations = invocations
        completed.ups = ups
        completed.paths = {"app": app, "old": old, "new": new, "data": data, "backups": backups}
        for canary in (CANARY_SECRET, CANARY_OVERRIDE, CANARY_TOKEN):
            self.assertNotIn(canary, output)
        return completed

    def _assert_safe_docker(self, result):
        for argv in result.invocations:
            self.assertNotIn("down", argv)
            self.assertNotIn("prune", argv)
            self.assertNotIn("restore", argv)
            self.assertNotIn("rm", argv)

    def _assert_backup_artifacts(self, result):
        backups = result.paths["backups"]
        sqlite_files = list(backups.glob(f"neurio-pre-{SHA}-*.sqlite"))
        self.assertEqual(len(sqlite_files), 1)
        backup = sqlite_files[0]
        self.assertRegex(backup.name, rf"^neurio-pre-{SHA}-\d{{8}}T\d{{6}}Z\.sqlite$")
        self.assertEqual(stat.S_IMODE(backup.stat().st_mode), 0o600)
        digest = hashlib.sha256(backup.read_bytes()).hexdigest()
        self.assertIn(digest, result.output)
        archives = list(backups.glob(f"src.pre-{SHA}.tgz"))
        self.assertEqual(len(archives), 1)
        self.assertEqual(stat.S_IMODE(archives[0].stat().st_mode), 0o600)
        with tarfile.open(archives[0], "r:gz") as tar:
            names = tar.getnames()
        self.assertTrue(any(name.endswith("readme.txt") for name in names))
        self.assertFalse(any(name == ".env" or name.endswith("/.env") for name in names))
        self.assertFalse(any("password" in name or "token" in name for name in names))
        env_copies = list(backups.glob(f".env.pre-{SHA}"))
        self.assertEqual(len(env_copies), 1)
        self.assertEqual(stat.S_IMODE(env_copies[0].stat().st_mode), 0o600)
        new_env = result.paths["new"] / ".env"
        self.assertTrue(new_env.is_file())
        self.assertEqual(stat.S_IMODE(new_env.stat().st_mode), 0o600)
        self.assertIn(CANARY_SECRET, new_env.read_text(encoding="utf-8"))
        override = (result.paths["new"] / "compose.cloud.override.yml").read_text(encoding="utf-8")
        self.assertIn("NEURIO_CLIENT_SECRET_FILE", override)
        self.assertIn(CANARY_OVERRIDE, override)
        runs = [argv for argv in result.invocations if argv and argv[0] == "run"]
        joined = [" ".join(argv) for argv in runs]
        self.assertTrue(any("10001:10001" in text and " backup " in f" {text} " for text in joined))
        self.assertTrue(any("verify-backup" in text for text in joined))
        self.assertTrue(any(f"neurio-lan-collector:pre-{SHA}" in text for text in joined))

    def _sqlite(self, path, version):
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
            (version,),
        )
        conn.commit()
        conn.close()

    def _tarball(self, path, wrapped):
        prefix = f"neurio-recorder-{SHA}/" if wrapped else ""
        files = {
            prefix + "Dockerfile": "FROM scratch\n",
            prefix + "compose.mqtt.yml": "name: neurio-lan-collector\n",
            prefix + "src/neurio_collector/__init__.py": "",
        }
        with tarfile.open(path, "w:gz") as tar:
            for name, text in files.items():
                data = text.encode()
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))

    def _wait_port(self, port):
        deadline = time.time() + 2
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    return
            except OSError:
                time.sleep(0.02)
        self.fail(f"health server did not listen on {port}")


if __name__ == "__main__":
    unittest.main()
