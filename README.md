# eve-imperium-sde

SDE generator for Eve Empire (`generate_sde.py`).

## Third-party data

`data/system_static.csv` (`solarSystemID, iceFieldCount, hasJoveObservatory`) is extracted from the
`SolarSystems` table of `src/Hisa.App/Data/solarSystemsStaticData.db` in
[HISA](https://github.com/Crytopox/HISA), MIT License, (c) 2026 Crytopox. It fills `universe.iceFields` and
`universe.jove`. EVE Online data and names remain the property of Fenris Creations / CCP Games.

## Changelog

- `types.radius REAL` (last column): the SDE `radius` of the type in metres, NULL when CCP does not ship one. Used by the app for ship/drone/fighter signature and angular-velocity maths.
