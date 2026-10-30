import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import generate_sde as g


def build(grades, types, reqs):
    d = tempfile.mkdtemp()
    with open(os.path.join(d, "cloneGrades.jsonl"), "w") as f:
        for i, skills in enumerate(grades):
            f.write(json.dumps({"_key": i, "skills": [{"typeID": t, "level": l} for t, l in skills]}) + "\n")
    conn = sqlite3.connect(":memory:")
    g.create_schema(conn)
    conn.executemany("INSERT INTO types (type_id, categoryID, published) VALUES (?,?,?)", types)
    conn.executemany(
        "INSERT INTO typeSkillRequirement (typeid, required_skill_id, required_skill_level) VALUES (?,?,?)", reqs)
    return conn, d


class AlphaTest(unittest.TestCase):
    def test_caps_and_ships(self):
        types = [(100, 16, 1), (200, 16, 1), (1, 6, 1), (2, 6, 1), (3, 6, 1), (4, 6, 0), (5, 6, 1)]
        reqs = [(1, 100, 3), (2, 100, 5), (3, 200, 1), (4, 100, 1)]
        conn, d = build([[(100, 4)], [(100, 4), (200, 2)]], types, reqs)
        g.insert_alpha(conn, d)
        self.assertEqual(dict(conn.execute("SELECT * FROM alpha_skill_caps")), {100: 4, 200: 2})
        self.assertEqual([r[0] for r in conn.execute("SELECT type_id FROM alpha_ships ORDER BY 1")], [1, 3, 5])

    def test_unknown_skill_type_fails(self):
        conn, d = build([[(999, 3)]], [(100, 16, 1)], [])
        with self.assertRaises(SystemExit):
            g.insert_alpha(conn, d)

    def test_bad_level_fails(self):
        conn, d = build([[(100, 6)]], [(100, 16, 1)], [])
        with self.assertRaises(SystemExit):
            g.insert_alpha(conn, d)

    def test_generated_db(self):
        path = os.environ.get("SDE_DB")
        if not path:
            self.skipTest("set SDE_DB to a generated sqlite")
        c = sqlite3.connect(path)
        self.assertGreater(c.execute("SELECT COUNT(*) FROM alpha_skill_caps").fetchone()[0], 0)
        self.assertGreater(c.execute("SELECT COUNT(*) FROM alpha_ships").fetchone()[0], 0)
        for t in ("alpha_skill_caps", "alpha_ships"):
            self.assertEqual(
                c.execute(f"SELECT COUNT(*) FROM {t} WHERE type_id NOT IN (SELECT type_id FROM types)").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
