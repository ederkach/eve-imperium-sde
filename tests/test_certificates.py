import os
import sqlite3
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import generate_sde as g

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "certificates")


def build():
    conn = sqlite3.connect(":memory:")
    g.create_schema(conn)
    g.insert_types(conn, FIXTURE)
    g.insert_certificates(conn, FIXTURE)
    g.insert_masteries(conn, FIXTURE)
    return conn


def count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


class CertificatesTest(unittest.TestCase):
    def test_tables_filled(self):
        conn = build()
        self.assertEqual(count(conn, "certificates"), 2)
        self.assertEqual(count(conn, "certificate_skills"), 3)
        self.assertEqual(count(conn, "certificate_recommended"), 1)
        self.assertEqual(count(conn, "masteries"), 8)
        self.assertEqual(
            conn.execute("SELECT * FROM certificate_skills WHERE certificate_id = 50 AND skill_type_id = 3300").fetchone(),
            (50, 3300, 1, 2, 3, 4, 5))
        self.assertEqual(
            conn.execute("SELECT group_id, name, ru_name, zh_name, description_ru FROM certificates WHERE certificate_id = 50").fetchone(),
            (255, "Small Energy Turret", "Лазерные орудия малой мощности", "小型能量炮台", "Малые лазеры."))
        self.assertEqual(
            conn.execute("SELECT level, certificate_id FROM masteries WHERE ship_type_id = 587 ORDER BY 1, 2").fetchall(),
            [(1, 96), (2, 96), (3, 50), (3, 96), (4, 50), (4, 96), (5, 50), (5, 96)])

    def test_types_have_race(self):
        conn = build()
        self.assertEqual(conn.execute("SELECT raceID, factionID FROM types WHERE type_id = 587").fetchone(), (1, 500001))
        self.assertEqual(conn.execute("SELECT raceID FROM types WHERE type_id = 3300").fetchone(), (None,))

    def test_generated_db(self):
        path = os.environ.get("SDE_DB")
        if not path:
            self.skipTest("set SDE_DB to a generated sqlite")
        c = sqlite3.connect(path)
        self.assertGreater(count(c, "certificates"), 100)
        self.assertGreater(count(c, "masteries"), 1000)
        self.assertEqual(c.execute(
            "SELECT COUNT(*) FROM masteries WHERE certificate_id NOT IN (SELECT certificate_id FROM certificates)").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
