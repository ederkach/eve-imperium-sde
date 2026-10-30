import os
import sys
import tempfile
import unittest

from PIL import Image

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import build_icons as b


def sde(types, groups):
    return {
        "types": types,
        "groups": groups,
        "icons": {1: "res:/ui/texture/icons/1_64_1.png"},
        "folders": {10: "res:/dx9/model/x/icons"},
        "skin_materials": {},
    }


class PlanTest(unittest.TestCase):
    def test_item_and_blueprint_rules(self):
        have = {
            "res:/dx9/model/x/icons/10_64.png",
            "res:/ui/texture/icons/1_64_1.png",
            b.TECH_OVERLAYS[2],
        }
        types = {
            1: {"groupID": 100, "graphicID": 10},
            2: {"groupID": 100, "graphicID": 10, "metaGroupID": 2},
            3: {"groupID": 12, "graphicID": 10, "iconID": 1},
            4: {"groupID": 900, "graphicID": 10, "iconID": 1},
            5: {"groupID": 1888, "iconID": 1},
            6: {"groupID": 950, "iconID": 1},
            7: {"groupID": 1950},
        }
        groups = {100: 7, 12: 2, 900: 9, 1888: 9, 950: 34, 1950: 91}
        jobs, fallthrough = b.plan_types(sde(types, groups), have.__contains__, True, log=lambda m: None)
        self.assertEqual(jobs["type_1_64.png"], ("copy", "res:/dx9/model/x/icons/10_64.png"))
        self.assertEqual(jobs["type_2_64.png"][0], "tech")
        self.assertEqual(jobs["type_3_64.png"], ("copy", "res:/ui/texture/icons/1_64_1.png"))
        self.assertEqual(jobs["type_4_64.png"][1], b.BPO_FRAME[0])
        self.assertEqual(jobs["type_4_bpc_64.png"][1], b.BPC_FRAME[0])
        self.assertEqual(jobs["type_5_64.png"][1], b.REACTION_FRAME[0])
        self.assertNotIn("type_5_bpc_64.png", jobs)
        self.assertEqual(jobs["type_6_64.png"][1], b.RELIC_FRAME[0])
        self.assertNotIn("type_7_64.png", jobs)
        self.assertEqual(fallthrough, [4])


class ComposeTest(unittest.TestCase):
    def test_frame_layers(self):
        d = tempfile.mkdtemp()
        paths = {}
        for name, color in {"bg": (10, 20, 30, 255), "art": (200, 0, 0, 0), "add": (100, 100, 100, 128),
                            "tech": (0, 255, 0, 255)}.items():
            paths[name] = os.path.join(d, name + ".png")
            Image.new("RGBA", (64, 64), color).save(paths[name])
        im = b.compose_frame(paths["bg"], paths["art"], paths["add"], paths["tech"])
        self.assertEqual(im.getpixel((40, 40)), (60, 70, 80, 255))
        self.assertEqual(im.getpixel((5, 5)), (0, 255, 0, 255))


if __name__ == "__main__":
    unittest.main()
