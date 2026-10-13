"""Offline checks for the Unix launcher; never start the real web server."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
FAKE_PYTHON = f"""#!{sys.executable}
import json
import os
from pathlib import Path
import shutil
import sys

project = Path.cwd()
in_venv = '.venv/bin/python' in sys.argv[0]
with (project / 'calls.jsonl').open('a') as log:
    log.write(json.dumps({{'venv': in_venv, 'args': sys.argv[1:]}}) + '\\n')
args = sys.argv[1:]
if args == ['-m', 'venv', '.venv']:
    target = project / '.venv' / 'bin' / 'python'
    target.parent.mkdir(parents=True)
    shutil.copy2(sys.argv[0], target)
elif args[:3] == ['-m', 'pip', 'install']:
    if os.environ.get('FAIL_INSTALL') == '1':
        sys.exit(1)
    (project / 'installed').touch()
elif args == ['-m', 'pip', '--version']:
    if not in_venv:
        sys.exit(1)
elif args[:1] == ['-c']:
    if 'import fastapi' in args[1] and not (project / 'installed').exists():
        sys.exit(1)
elif args == ['webui.py']:
    (project / 'launched').touch()
else:
    sys.exit(2)
"""


@unittest.skipUnless(os.name == "posix", "Unix launcher requires bash")
class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="launcher path with spaces ")
        self.addCleanup(self.temp.cleanup)
        self.project = Path(self.temp.name) / "project with spaces"
        self.project.mkdir()
        shutil.copy2(ROOT / "webui.sh", self.project / "webui.sh")
        (self.project / "requirements.txt").write_text("placeholder\n")
        (self.project / "webui.py").write_text('raise AssertionError("not executed")\n')
        self.fake_bin = Path(self.temp.name) / "fake bin"
        self.fake_bin.mkdir()
        self.fake_python = self.fake_bin / "python3.13"
        self.fake_python.write_text(FAKE_PYTHON)
        self.fake_python.chmod(0o755)

    def run_launcher(self, *, path=None, fail_install=False):
        env = os.environ.copy()
        env["PATH"] = str(path or self.fake_bin) + os.pathsep + "/usr/bin:/bin"
        if fail_install:
            env["FAIL_INSTALL"] = "1"
        return subprocess.run(
            ["/bin/bash", str(self.project / "webui.sh")],
            cwd=self.temp.name,
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def calls(self):
        return [
            json.loads(line)
            for line in (self.project / "calls.jsonl").read_text().splitlines()
        ]

    def make_venv(self):
        target = self.project / ".venv" / "bin" / "python"
        target.parent.mkdir(parents=True)
        shutil.copy2(self.fake_python, target)

    def test_creates_venv_installs_only_there_and_launches_from_space_path(self):
        result = self.run_launcher()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = self.calls()
        self.assertTrue((self.project / "launched").exists())
        self.assertEqual(
            [c["args"] for c in calls if c["args"][:2] == ["-m", "venv"]],
            [["-m", "venv", ".venv"]],
        )
        self.assertEqual(
            [c["venv"] for c in calls if c["args"][:3] == ["-m", "pip", "install"]],
            [True],
        )
        self.assertTrue(calls[-1]["venv"])
        self.assertEqual(calls[-1]["args"], ["webui.py"])

    def test_reuses_existing_venv_without_host_python_or_install(self):
        self.make_venv()
        (self.project / "installed").touch()
        empty_bin = Path(self.temp.name) / "empty bin"
        empty_bin.mkdir()
        result = self.run_launcher(path=empty_bin)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue((self.project / "launched").exists())
        self.assertTrue(all(c["venv"] for c in self.calls()))
        self.assertFalse(
            any(c["args"][:3] == ["-m", "pip", "install"] for c in self.calls())
        )

    def test_install_failure_prevents_server_start(self):
        result = self.run_launcher(fail_install=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Failed to install dependencies", result.stderr)
        self.assertFalse((self.project / "launched").exists())

    def test_invalid_venv_is_preserved(self):
        broken = self.project / ".venv"
        broken.mkdir()
        result = self.run_launcher()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(".venv is invalid", result.stderr)
        self.assertTrue(broken.exists())
        self.assertFalse((self.project / "launched").exists())
        self.assertFalse((self.project / "calls.jsonl").exists())

    def test_uv_runs_in_project_directory_and_preserves_failure(self):
        uv = self.fake_bin / "uv"
        uv.write_text(
            f"#!{sys.executable}\n"
            "from pathlib import Path\n"
            "import sys\n"
            "assert sys.argv[1:] == ['run', 'webui.py']\n"
            "Path('uv-launched').touch()\n"
            "sys.exit(7)\n"
        )
        uv.chmod(0o755)
        result = self.run_launcher()
        self.assertEqual(result.returncode, 7)
        self.assertTrue((self.project / "uv-launched").exists())
        self.assertFalse((self.project / "calls.jsonl").exists())


class WindowsLauncherTests(unittest.TestCase):
    def test_every_goto_target_exists_and_uv_shares_server_exit(self):
        import re

        text = (ROOT / "webui.bat").read_text()
        labels = {label.lower() for label in re.findall(r"^:([\w]+)\s*$", text, re.M)}
        targets = {
            label.lower() for label in re.findall(r"goto\s+:?([\w]+)", text, re.I)
        }
        self.assertTrue(targets <= labels, targets - labels)
        self.assertIn("uv run webui.py\n    goto :done", text)
        self.assertIn(':done\nset "SERVER_EXIT=%errorlevel%"', text)
        self.assertIn("exit /b %SERVER_EXIT%", text)


if __name__ == "__main__":
    unittest.main()
