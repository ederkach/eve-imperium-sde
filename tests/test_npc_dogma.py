import json
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import generate_sde as g


class NpcClassifyTest(unittest.TestCase):
    def test_group_name_rules(self):
        self.assertEqual(g.classify_npc("Asteroid Blood Raiders BattleCruiser", "Corpus Pope", None, "Cruiser"),
                         ("Asteroid", "Blood Raider", "Battlecruiser", "faction_500012.png"))
        self.assertEqual(g.classify_npc("Storyline Mission Frigate", "x", None, None)[0], "Storyline")
        self.assertEqual(g.classify_npc("FW Amarr Empire Hauler", "x", None, None)[:3],
                         ("Faction Warfare", "Amarr Empire", "Hauler"))
        self.assertEqual(g.classify_npc("Deadspace Sansha's Nation Frigate", "x", None, None)[1], "Sansha")
        self.assertEqual(g.classify_npc("Seeker Scouts", "x", None, None)[1:],
                         ("Sleeper", "Other", "faction_500005.png"))

    def test_fallbacks(self):
        # SDE factionID, then the hull of a playable ship with the same graphic
        self.assertEqual(g.classify_npc("Irregular Thing", "x", 500025, "Battleship")[1:],
                         ("Rogue Drone", "Battleship", "faction_500025.png"))
        # unknown factionID falls through to the type name
        self.assertEqual(g.classify_npc("Spawn Container", "Bloodraider Warehouse", 1000162, None)[1], "Blood Raider")
        self.assertEqual(g.classify_npc("Triglavian Entities", "x", 500026, None)[1:],
                         ("Other", "Other", "faction_500021.png"))
        self.assertEqual(g.classify_npc("Deadspace Overseer's Sentry", "x", None, None)[1:3], ("Overseer", "Sentry"))
        self.assertEqual(g.classify_npc("Faction Drone", "x", None, None)[1:3], ("Faction", "Drone"))

    def test_vocabulary(self):
        for group in ("FW Minmatar Republic Battlecruiser", "Deadspace Blood Raiders Destroyer", "Blood Raiders"):
            _, faction, ship, _ = g.classify_npc(group, "", None, None)
            self.assertNotIn(faction, ("Blood Raiders",))
            self.assertNotEqual(ship, "Combat Battlecruiser")

    def test_hull_by_graphic(self):
        types = {1: {"published": True, "graphicID": 7, "groupID": 10},
                 2: {"published": True, "graphicID": 8, "groupID": 11},
                 3: {"published": False, "graphicID": 9, "groupID": 10}}
        groups = {10: ("Marauder", 6), 11: ("Marauder", 7)}
        self.assertEqual(g.npc_hull_by_graphic(types, groups), {7: "Battleship"})

    def test_sde_facts(self):
        # group bucket beats the SDE factionID; real factions outside the vocabulary are Other
        self.assertEqual(g.classify_npc("Deadspace Overseer Battleship", "x", 500020, None)[1], "Overseer")
        self.assertEqual(g.classify_npc("EDENCOM Entities", "x", 500027, None, "amarrnavy")[1], "Other")
        # graphic livery, then race words on ships only
        self.assertEqual(g.classify_npc("Spawn Container", "Cloaked Cache", None, None, "angelbase")[1], "Angel Cartel")
        self.assertEqual(g.classify_npc("Convoy", "Hauler", None, None, "caldaribase")[1], "Other")
        self.assertEqual(g.classify_npc("Police Drone", "Gallente Police Captain", None, None, None, 120, True)[1],
                         "Gallente Federation")
        self.assertEqual(g.classify_npc("Large Collidable Structure", "Amarr Supply Depot", None, None)[1], "Other")
        # signature radius sizes moving entities that have no other clue
        self.assertEqual(g.classify_npc("Abyssal Spaceship Entities", "x", None, None, None, 400, True)[2], "Battleship")
        self.assertEqual(g.classify_npc("Spawn Container", "x", None, None, None, 400, False)[2], "Other")

    def test_populate_only_entities(self):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, "types.jsonl"), "w") as f:
            f.write(json.dumps({"_key": 1, "published": True, "graphicID": 5, "groupID": 27}) + "\n")
            f.write(json.dumps({"_key": 2, "graphicID": 5, "groupID": 99}) + "\n")
        with open(os.path.join(d, "graphics.jsonl"), "w") as f:
            f.write(json.dumps({"_key": 5, "sofFactionName": "guristas"}) + "\n")
        conn = sqlite3.connect(":memory:")
        g.create_schema(conn)
        conn.executemany("INSERT INTO types (type_id, en_name, group_name, groupID, categoryID, published) "
                         "VALUES (?,?,?,?,?,?)",
                         [(1, "Raven", "Battleship", 27, 6, 1), (2, "Raven NPC", "Irregular Ship", 99, 11, 0)])
        g.populate_npc_ship_data(conn, d)
        rows = dict((r[0], r[1:]) for r in conn.execute(
            "SELECT type_id, npc_ship_scene, npc_ship_faction, npc_ship_type FROM types"))
        self.assertEqual(rows, {1: (None, None, None), 2: ("Other", "Guristas", "Battleship")})


class DogmaPatchTest(unittest.TestCase):
    def test_missing_file(self):
        self.assertEqual(g.load_dogma_patches("/nonexistent/patches.json"), {})

    def test_shipped_file(self):
        patches = g.load_dogma_patches()
        self.assertEqual(len(patches), 14)
        self.assertEqual(json.loads(patches["selfRof"]), [{
            "domain": "shipID", "func": "LocationRequiredSkillModifier", "modifiedAttributeID": 51,
            "modifyingAttributeID": 293, "operation": 6, "skillTypeID": -1}])
        self.assertEqual(patches["moduleBonusAfterburner"],
                         '[{"domain":"shipID","func":"ItemModifier","modifiedAttributeID":4,'
                         '"modifyingAttributeID":796,"operation":2}]')

    def test_apply(self):
        d = tempfile.mkdtemp()
        ccp = [{"domain": "charID", "func": "ItemModifier", "modifiedAttributeID": 212,
                "modifyingAttributeID": 213, "operation": 0}]
        with open(os.path.join(d, "dogmaEffects.jsonl"), "w") as f:
            for key, name, mods in ((763, "missileDMGBonus", ccp), (1, "keep", ccp), (2, "emptyPatch", None)):
                f.write(json.dumps({"_key": key, "name": name, "modifierInfo": mods}) + "\n")
        patch_path = os.path.join(d, "p.json")
        with open(patch_path, "w") as f:
            json.dump({"missileDMGBonus": [["shipID", "LocationRequiredSkillModifier", 114, 213, 4, 3319]],
                       "emptyPatch": []}, f)
        conn = sqlite3.connect(":memory:")
        g.create_schema(conn)
        with mock.patch.object(g, "DOGMA_PATCHES_PATH", patch_path):
            g.insert_dogma_effects(conn, d)
        got = {r[0]: r[1] and json.loads(r[1]) for r in conn.execute("SELECT effect_id, modifier_info FROM dogmaEffects")}
        self.assertEqual(got[763], [{"domain": "shipID", "func": "LocationRequiredSkillModifier",
                                     "modifiedAttributeID": 114, "modifyingAttributeID": 213,
                                     "operation": 4, "skillTypeID": 3319}])
        self.assertEqual(got[1], ccp)
        self.assertIsNone(got[2])


if __name__ == "__main__":
    unittest.main()
