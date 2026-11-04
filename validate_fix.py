import ast, subprocess, os

os.chdir("/Workspace/Users/shiva.eravelly@discounttire.com/cloud_cost_optimizer")

files = [
    "src/cco/etl/fingerprint.py",
    "src/cco/etl/normalizer.py",
    "tests/test_fingerprint_regression.py",
]

for f in files:
    ast.parse(open(f).read())
    print(f"OK  {f}")
print("\nAll files parse successfully.\n")

# Run full test suite
env = os.environ.copy()
env["PYTHONDONTWRITEBYTECODE"] = "1"
result = subprocess.run(
    [os.sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-v", "--tb=short"],
    env=env, capture_output=True, text=True,
)
print(result.stdout[-5000:])
if result.stderr:
    print("STDERR:", result.stderr[-2000:])
print(f"\nReturn code: {result.returncode}")
