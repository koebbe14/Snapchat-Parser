# Changelog

## Version 2.5

### Additional Records Support
- **Additional Records dialog:** New toolbar button opens an inspector for non-conversation production CSVs (IP data, subscriber info, push tokens, device advertising IDs, AI conversations, account change history, and more). Records are displayed in a tree view grouped by archive folder and file, with per-section tables showing native column headers.
- **Tagging in Additional Records:** Rows in additional records can be tagged using the same right-click context menu and tag priority system as conversation messages. Tag colors are applied in real-time via a custom cell delegate that ensures visibility regardless of theme or palette settings.
- **Tags dialog — Additional Records tab:** The Tags dialog now includes an "Additional Records" tab (renamed from "Production records") that groups tagged records by type, each with its own table and correct column headers. Tag background colors are rendered reliably using a dedicated paint delegate.

### HTML Export Improvements
- **Grouped additional records in export:** Additional records are no longer dumped into a single flat table. Each record type is rendered with its own table and column headers, grouped by archive folder, file, and section — matching the tree layout in the Additional Records dialog.
- **Record type dropdown filter:** The Additional Records section in exported HTML includes a dropdown selector (with `<optgroup>` per archive folder) to filter by record type.
- **Internal navigation:** When additional records are present, a nav bar links between the Conversations & Messages section and the Additional Records section, with a "Back to top" link.
- **Standalone additional-records-only export:** Exporting only additional records (no messages) now uses the same grouped renderer with proper per-type tables, CSS, and dark mode support.
- **Messages table isolation:** The messages table is assigned `id="messagesExportTable"` and all filter/sort/resize JavaScript uses `getElementById` instead of `querySelector('.dataframe')`, preventing conflicts with additional records tables.
- **Scoped CSS:** Column-specific `nth-child` width rules are scoped to `#messagesExportTable` so they do not affect additional records tables.
- **Tag row coloring in export:** Additional records rows in HTML exports show tag-priority background colors and alternating sender colors, matching the in-app appearance.

### Conversation Label Consistency
- **Dropdown labels in exported HTML:** The conversation filter dropdown in exported HTML now uses the same `_conversation_selector_display_name` pipeline as the in-app selector, producing labels like "Direct message with ..." and "Group · ..." instead of raw conversation titles or IDs.
- **Note row labels:** Investigative note rows in exported HTML use the same conversation display names for consistency.

### Dialog Size and Usability
- Increased default sizes for: Search Results (+200%), Message Filters (+100%), Tags (+50%), Manage Hotkeys & Tags (+100%), Manage Keyword Lists (+100%), Add Note to Conversation (+100%), Stats (+100%).
- Removed the Windows help question-mark button from the Stats and Additional Records dialogs.

### Bug Fixes
- **Tag colors in Additional Records dialog:** Fixed tag background colors not appearing due to `setAlternatingRowColors` palette overriding model `BackgroundRole`. The `ProductionRecordCellDelegate` now explicitly paints the background from the model before default rendering.
- **Tag colors in Tags dialog:** Fixed tag colors being overridden by dialog stylesheets. A dedicated `_TagColorDelegate` now paints `BackgroundRole` explicitly, and colors are stored via `setData(Qt.BackgroundRole)` instead of `setBackground()`.

### Internal / Data
- `gather_additional_production_export_rows` now includes `_archive_group` metadata per row (from `tree_group_label_for_internal`). All `_`-prefixed internal keys are excluded from CSV and HTML DataFrame exports.
- Help dialog updated with Additional Records documentation; version changed to 2.5.
