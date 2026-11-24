import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ContractIdentityTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        for name in (".config", "contracts", "examples", "views"):
            shutil.copytree(ROOT / name, self.root / name)
        (self.root / "scripts").mkdir()
        shutil.copy(ROOT / "scripts/check-samples.py", self.root / "scripts")

    def assert_rejected(self, reason):
        result = subprocess.run(
            [sys.executable, "scripts/check-samples.py"], cwd=self.root,
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(reason, result.stderr)

    def test_patient_contract_copied_over_observation(self):
        contracts = self.root / "contracts/fabric-hds/1.4.0"
        shutil.copyfile(contracts / "Patient.json", contracts / "Observation.json")
        self.assert_rejected("Observation.json: contract resource 'Patient'")

    def test_view_resource_disagrees_with_contract(self):
        path = self.root / "views/r4/fabric-silver/Observation.json"
        view = json.loads(path.read_text(encoding="utf-8"))
        view["resource"] = "Patient"
        path.write_text(json.dumps(view), encoding="utf-8")
        self.assert_rejected("Observation.json: ViewDefinition resource 'Patient'")


if __name__ == "__main__":
    unittest.main()
