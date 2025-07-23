# tests/conftest.py
import sys, pathlib

# Add project root (one level up) to Python path so `import scripts...` works.
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
