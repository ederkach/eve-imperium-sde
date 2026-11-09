import os
import sqlite3
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import generate_sde as g


def order(type_id, price, loc, dur):
    return {"type_id": type_id, "price": price, "location_id": loc, "duration": dur}


class Headers(dict):
    pass


def fake_esi(pages_by_region):
    def fetch(url):
        region = int(url.split("/markets/")[1].split("/")[0])
        if region not in pages_by_region:
            return None
        page = int(url.rsplit("page=", 1)[1])
        pages = pages_by_region[region]
        return pages[page - 1], Headers({"X-Pages": str(len(pages))})
    return fetch


def make_db():
    conn = sqlite3.connect(":memory:")
    g.create_schema(conn)
    conn.executemany("INSERT INTO types (type_id, metaGroupID) VALUES (?,?)",
                     [(1001, 1), (1002, 2), (1003, 1), (2001, 1), (2002, 2), (2003, 1)])
    conn.executemany("INSERT INTO blueprint_manufacturing_output (blueprintTypeID, typeID) VALUES (?,?)",
                     [(101, 1001), (102, 1002), (103, 1003)])
    conn.execute("INSERT INTO loyalty_offer_outputs (offer_id, type_id) VALUES (1, 103)")
    return conn


class BlueprintSourcesTest(unittest.TestCase):
    def test_sources(self):
        fetch = fake_esi({
            10000002: [
                [order(101, 500.0, 60003760, 365), order(999, 1.0, 60003760, 365)],
                [order(101, 400.0, 60003760, 365), order(102, 1.0, 1_000_000_000_000, 90)],
            ],
            10000043: [[order(101, 450.0, 60008494, 365)]],
        })
        conn = make_db()
        g.insert_blueprint_sources(conn, True, fetch)
        rows = {r[0]: r for r in conn.execute("SELECT * FROM blueprint_sources")}
        self.assertEqual(set(rows), {101, 102, 103})
        self.assertEqual(rows[101][1:6], (1, 400.0, "10000002,10000043", 0, 0))
        self.assertEqual(rows[102][1:6], (0, None, None, 0, 1))
        self.assertEqual(rows[103][1:6], (0, None, None, 1, 0))

    def test_skip(self):
        conn = make_db()
        g.insert_blueprint_sources(conn, False, lambda u: self.fail("no ESI"))
        self.assertEqual(conn.execute("SELECT SUM(npc_seeded) FROM blueprint_sources").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
