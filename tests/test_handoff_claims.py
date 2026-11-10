import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from neurio_collector.rpt_contract import contract_flags


class HandoffClaimTests(unittest.TestCase):
    def test_status_flags_stay_honest(self):
        flags = contract_flags()
        self.assertFalse(flags["rateplantool_contract"])
        self.assertTrue(flags["rateplantool_contract_verified"])
        self.assertEqual(flags["rateplantool_integration"], "not_wired")
        self.assertFalse(flags["safe_drop_in_adapter"])
        self.assertFalse(flags["general_csv_importable"])
        readme = (ROOT / "README.md").read_text()
        self.assertIn("Fixture tests are not live verification", readme)
        self.assertIn("does not modify Home Assistant", readme)
        api = (ROOT / "src" / "neurio_collector" / "api.py").read_text()
        self.assertIn('"docker_image_built": False', api)
        self.assertIn('"deployed": False', api)
        self.assertIn('"ha_poll_modified": False', api)
        self.assertIn('"lan_device_verified": False', api)

    def test_rateplantool_contract_replaced_the_auth_blocker(self):
        text = (ROOT / "docs" / "RATEPLANTOOL_CONTRACT.md").read_text()
        self.assertIn("78fc57c348e93e11cc6b3cf26f02fb292c54bbfe", text)
        self.assertIn("consumptionEnergy", text)
        self.assertIn("start + 24 hours", text)
        self.assertIn("Hour + 1", text)
        self.assertIn("not_wired", text)
        self.assertNotIn("could not read Username", text)
        self.assertNotIn("github.com", text)


if __name__ == "__main__":
    unittest.main()
