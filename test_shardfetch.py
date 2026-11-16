"""Root-level shim so `python -m unittest test_shardfetch -v` works.

The real test suite lives in tests/test_shardfetch.py.
"""

from tests.test_shardfetch import *  # noqa: F401,F403

if __name__ == "__main__":
    import unittest

    unittest.main()
