import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import generate_sde as g


def write(d, name, rows):
    with open(os.path.join(d, name), "w") as f:
        f.write("\n".join(json.dumps(r) for r in rows) + "\n")


class WormholeUniverseTest(unittest.TestCase):
    def test_class_falls_back_to_constellation_then_region_and_effect_comes_from_secondary_sun(self):
        d = tempfile.mkdtemp()
        write(d, "mapRegions.jsonl", [
            {"_key": 11000001, "name": {"en": "A-R00001"}, "wormholeClassID": 1},
            {"_key": 10000002, "name": {"en": "The Forge"}, "wormholeClassID": 7},
        ])
        write(d, "mapConstellations.jsonl", [
            {"_key": 21000001, "name": {"en": "A-C00001"}, "regionID": 11000001, "wormholeClassID": 5},
            {"_key": 21000002, "name": {"en": "A-C00002"}, "regionID": 11000001},
            {"_key": 20000001, "name": {"en": "Kimotoro"}, "regionID": 10000002},
        ])
        write(d, "mapSolarSystems.jsonl", [
            {"_key": 31000001, "name": {"en": "J000001"}, "constellationID": 21000001, "regionID": 11000001, "securityStatus": -1},
            {"_key": 31000002, "name": {"en": "J000002"}, "constellationID": 21000002, "regionID": 11000001, "securityStatus": -1, "wormholeClassID": 3},
            {"_key": 31000003, "name": {"en": "J000003"}, "constellationID": 21000002, "regionID": 11000001, "securityStatus": -1},
            {"_key": 30000142, "name": {"en": "Jita"}, "constellationID": 20000001, "regionID": 10000002, "securityStatus": 0.9},
        ])
        write(d, "mapSecondarySuns.jsonl", [{"_key": 1, "solarSystemID": 31000001, "effectBeaconTypeID": 30878, "typeID": 30669}])
        conn = sqlite3.connect(":memory:")
        g.create_schema(conn)
        g._insert_universe_from_map_files(conn, d)
        got = {r[0]: r[1:] for r in conn.execute("SELECT solarsystem_id, wormhole_class, effect_type_id FROM universe")}
        self.assertEqual(got, {31000001: (5, 30878), 31000002: (3, None), 31000003: (1, None), 30000142: (7, None)})


if __name__ == "__main__":
    unittest.main()
