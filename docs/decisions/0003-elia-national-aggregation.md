# 0003. National Elia totals: use the published total row, never sum the hierarchy

- Status: accepted
- Date: 2026-10-18

## Context

The Elia datasets break their values down differently (checked against the live API on 2026-10-18):

| Dataset | Breakdown | National total row? |
|---|---|---|
| `ods001` load | none, one national series | n/a |
| `ods032` solar | `Belgium` + 3 regions (Flanders, Wallonia, Brussels) + 11 provinces | **yes, `region = 'Belgium'`** |
| `ods031` wind | 5 disjoint parts: Offshore/Federal, Onshore × {Flanders, Wallonia} × {Elia, Dso} | **no** |

Summing every solar row would count national solar three times (Belgium + regions + provinces). Check for 2025-06-01: Belgium 150,266 = Flanders 106,342 + Wallonia 39,934 + Brussels 3,990, and the Flemish provinces sum to Flanders.

## Decision

- National solar = the `region = 'Belgium'` row.
- National wind = the sum of the 5 disjoint parts, with `parts_present` stored next to it so a missing part shows up as a gap instead of a silent dip.
- Staging keeps all regional rows. Phase 2 can use them (Antwerp province solar, for example).
- A quality check asserts that, where all are present, `Belgium ≈ Flanders + Wallonia + Brussels` (within 1%). If Elia changes the hierarchy, the check fails loudly.
