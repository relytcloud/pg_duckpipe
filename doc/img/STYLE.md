# Diagram Style Guide

Style rules for Excalidraw diagrams in this project. Read before creating or updating any diagram.

## Visual Style

- **Roughness**: `1` (default hand-drawn look)
- **Font**: `fontFamily: 1` (Virgil / handwritten)
- **Font sizes**: titles 18-20px, shape labels 16px (default), annotations 12-13px
- **Stroke width**: 2 for primary shapes and arrows, 1-1.5 for secondary/zone borders

## Color Palette

Soft fills with matching borders. Shapes use colored fills (not white) to distinguish component types.

| Role | Border (strokeColor) | Fill (backgroundColor) | Usage |
|------|---------------------|----------------------|-------|
| Data source | `#1971c2` (blue) | `#a5d8ff` | Heap tables, row-store components |
| Data target | `#1971c2` (blue) | `#a5d8ff` | DuckLake tables, columnar store |
| Pipeline / CDC | `#2f9e44` (green) | `#b2f2bb` | pg_duckpipe, replication components |
| Catalog / extension | `#2f9e44` (green) | `#b2f2bb` | pg_ducklake, extensions |
| Zone background | `#343a40` (dark gray) | transparent | Outer container (dashed border) |
| Text / arrows | `#1e1e1e` (near-black) | — | Default arrow and label color |
| Annotations | `#2f9e44` (green) | — | Zone labels (e.g., "PostgreSQL") |

## Layout Rules

- **Spacing**: minimum 40px between shapes, 60-80px preferred
- **Arrow labels**: place as separate `text` elements above or below arrows
- **Arrow binding**: use `startElementId`/`endElementId` for auto-routing
- **Dashed lines**: `strokeStyle: "dashed"` for containers/zones
- **Solid arrows**: for data flow
- **Flow direction**: left-to-right for data flow, top-to-bottom for hierarchy

## Text Readability Checklist

Before exporting, verify:
1. No arrow passes through any text label
2. No text overlaps another text or shape border
3. Labels are clearly associated with their shapes or arrows

## Shape Sizing

| Shape | Width | Height |
|-------|-------|--------|
| Primary component | 160-200 | 55-80 |
| Secondary/small | 140-160 | 30-45 |
| Zone container | full width | wraps children + 40px padding |
| Data store | 340-620 | 55-80 |

## Export

- Format: PNG
- Location: `doc/img/` for technical docs, `images/` for README
- Naming: `{short-name}.png` (e.g., `arch.png`, `backpressure.png`)
- Reference in markdown as: `![Alt text](img/{filename}.png)`
