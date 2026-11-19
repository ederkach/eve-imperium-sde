import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import generate_sde as g


class PlanetResourcesTest(unittest.TestCase):
    def test_only_reagent_planets_are_stored_with_cycle_in_minutes(self):
        d = tempfile.mkdtemp()
        rows = [
            {"_key": 40013180, "power": 740},
            {"_key": 40013231, "power": 0, "workforce": 5, "reagent": {
                "amount_per_cycle": 41, "cycle_period": 3600, "secured_capacity": 59860,
                "type_id": 81144, "unsecured_capacity": 59860}},
            {"_key": 40013312, "power": 0, "reagent": {
                "amount_per_cycle": 665, "cycle_period": 3600, "secured_capacity": 970900,
                "type_id": 81143, "unsecured_capacity": 970900}},
        ]
        with open(os.path.join(d, "planetResources.jsonl"), "w") as f:
            f.write("\n".join(json.dumps(r) for r in rows) + "\n")
        conn = sqlite3.connect(":memory:")
        g.create_schema(conn)
        g.insert_planet_resources(conn, d)
        got = {r[0]: r[1:] for r in conn.execute("SELECT * FROM planetResources")}
        self.assertEqual(got, {40013231: (81144, 41, 60, 0, 5), 40013312: (81143, 665, 60, 0, 0)})

    def test_missing_file_is_skipped(self):
        conn = sqlite3.connect(":memory:")
        g.create_schema(conn)
        g.insert_planet_resources(conn, tempfile.mkdtemp())
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM planetResources").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
