"""Run current regressions; the original audit reproductions are now fixed."""
import sys
from pathlib import Path
import unittest

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
suite = unittest.defaultTestLoader.discover(str(root / "tests"), pattern="test_unlim.py")
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
