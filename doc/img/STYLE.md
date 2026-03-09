# Diagram Style Guide

Style rules for creating Excalidraw diagrams in this project. Read this before creating or updating any diagram.

## Visual Style

- **Roughness**: `0` (clean, no hand-drawn effect)
- **Font**: `fontFamily: 2` (Helvetica/sans-serif)
- **Font sizes**: titles 18-20px, shape labels 16px (default), annotations 12-13px
- **Stroke width**: 2 for primary shapes and arrows, 1-1.5 for secondary/zone borders

## Color Palette

Muted, tech-style. White fills with colored borders to distinguish component types. Avoid bright/saturated fills.

| Role | Border (strokeColor) | Fill (backgroundColor) | Usage |
|------|---------------------|----------------------|-------|
| Control plane | `#339af0` (blue) | `#ffffff` or `#e7f5ff` | WAL consumer, main loop, collect_results |
| Flush / OS threads | `#e03131` (red) | `#ffffff` or `#fff5f5` | Flush threads, flush results |
| Snapshot | `#845ef7` (purple) | `#ffffff` or `#f3f0ff` | Snapshot manager, snapshot tasks |
| Queues / async | `#f59f00` (amber) | `#ffffff` or `#fff9db` | Queues, LISTEN task |
| Data store | `#0c8599` (cyan) | `#e3fafc` | DuckDB, DuckLake |
| Zone background | `#ced4da` (gray) | `#f1f3f5` | Outer container, grouping boxes |
| Inner zone | varies | `#e7f5ff`, `#fff5f5` | Tokio runtime zone, flush thread zone |
| Text / arrows | `#495057` (dark gray) | — | Default arrow and label color |
| Annotations | `#868e96` (gray) | — | Secondary labels, descriptions |
| Success | `#2f9e44` (green) | `#ebfbee` | Positive outcomes |
| Error/pause | `#e03131` (red) | `#fff5f5` | Negative outcomes, stalls |

## Layout Rules

- **Spacing**: minimum 40px between shapes, 60-80px preferred
- **Arrow labels**: place as separate `text` elements above or below arrows, never as `text` on the arrow itself (avoids overlap with crossing lines)
- **Arrow binding**: always use `startElementId`/`endElementId` for auto-routing
- **Dashed arrows**: `strokeStyle: "dashed"` for async/spawn relationships
- **Flow direction**: top-to-bottom for hierarchy, left-to-right for data flow
- **Zones**: large background rectangles with low-contrast borders; zone label as separate text at top-left, font size 14px

## Text Readability Checklist

Before exporting, verify:
1. No arrow passes through any text label
2. No text overlaps another text or shape border
3. Labels for bidirectional arrows (e.g., spawn → / ← mpsc) are on opposite sides (one above, one below)
4. Diamond decision labels ("yes"/"no") are placed outside the diamond, not overlapping edges

## Shape Sizing

| Shape | Width | Height |
|-------|-------|--------|
| Primary component | 160-200 | 55-70 |
| Secondary/small | 140-160 | 45-55 |
| Zone container | full width | wraps children + 40px padding |
| Decision diamond | 180 | 100 |
| Data store | 340-620 | 55-70 |

## Export

- Format: PNG
- Location: `doc/img/`
- Naming: `{number}-{short-name}.png` (e.g., `1-overview.png`, `3-backpressure.png`)
- Reference in markdown as: `![Alt text](img/{filename}.png)`
