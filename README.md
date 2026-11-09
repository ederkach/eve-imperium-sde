# eve-imperium-sde

SDE generator for Eve Empire (`generate_sde.py`).

## Changelog

- `types.radius REAL` (last column): the SDE `radius` of the type in metres, NULL when CCP does not ship one. Used by the app for ship/drone/fighter signature and angular-velocity maths.
- `blueprint_sources` (one row per blueprint in `blueprint_manufacturing_output`): `type_id INTEGER PRIMARY KEY, npc_seeded INTEGER NOT NULL, seed_price REAL, seed_region_ids TEXT, lp_offer INTEGER NOT NULL, is_copy_only INTEGER NOT NULL, snapshot_utc TEXT NOT NULL`. Lets the app know where a blueprint comes from without live market calls.
  - `npc_seeded`/`seed_price`/`seed_region_ids`: from ESI sell orders of The Forge, Domain, Sinq Laison, Heimatar and Metropolis. Rule: NPC order = NPC station (`location_id < 70000000`) + `duration >= 90` days (player orders last at most 90). `seed_price` is the lowest price, `seed_region_ids` comma-joined. A failing region is logged and skipped, never aborts the build.
  - `lp_offer`: 1 when any row of `loyalty_offer_outputs` has this blueprint as its output.
  - `is_copy_only`: 1 when the product's `metaGroupID` is 2 (Tech II) or 14 (Tech III).
  - Flags: `--blueprint-sources` (default on) / `--skip-blueprint-sources` (no ESI scan, `npc_seeded = 0` everywhere).
