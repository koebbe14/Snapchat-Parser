# SnapchatParser v2.2 - Changelog

## Recent Changes

### Data Display Order
- **Changed**: All tables now display data in chronological order (oldest to newest)
  - Messages are sorted by timestamp in ascending order
  - Conversations in the selector are sorted chronologically (oldest first)

### Reported Files Integration
- **Added**: Support for incomplete data rows from Snapchat's conversations.csv (only contain: `sender_username`, `timestamp`, and `media_id`)
  - These rows typically represent flagged/removed media reported to NCMEC
  - Previously filtered out, now included in the program

- **Added**: Special "Reported Files" conversation grouping
  - Incomplete rows are automatically assigned to a special conversation
  - Appears in conversation selector as "Reported Files"
  - Appears in "All Conversations" view
  
### Fixed bug that would crash program when copying multiple rows

### Fixed issue causing HTML file to not sort by Tag Type

### Fixed bug that would crash the program when configuring column visibility and column order



