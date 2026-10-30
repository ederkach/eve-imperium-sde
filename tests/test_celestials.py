import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import generate_sde as g


class CelestialTest(unittest.TestCase):
    def test_planet_type_id_stored_moons_null(self):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, "mapSolarSystems.jsonl"), "w") as f:
            f.write(json.dumps({"_key": 30000142, "name": {"en": "Jita"}}) + "\n")
        with open(os.path.join(d, "mapPlanets.jsonl"), "w") as f:
            f.write(json.dumps({"_key": 40009077, "solarSystemID": 30000142, "celestialIndex": 4, "typeID": 2016, "radius": 5060000}) + "\n")
        with open(os.path.join(d, "mapMoons.jsonl"), "w") as f:
            f.write(json.dumps({"_key": 40009080, "orbitID": 40009077, "orbitIndex": 1}) + "\n")
        conn = sqlite3.connect(":memory:")
        g.create_schema(conn)
        g.insert_celestial_names(conn, d)
        rows = dict((r[0], (r[1], r[2], r[3])) for r in conn.execute("SELECT * FROM celestialNames"))
        self.assertEqual(rows[40009077], ("Jita IV", 2016, 5060000))
        self.assertEqual(rows[40009080], ("Jita IV - Moon 1", None, None))


if __name__ == "__main__":
    unittest.main()
