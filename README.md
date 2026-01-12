# Snapchat Parser

**Version 2.2**

**Developer:** Patrick Koebbe  
**Contact:** koebbe14@gmail.com  

---

## Overview

Snapchat Parser is a specialized tool designed for law enforcement and investigators to analyze Snapchat Responsive Records exports (in ZIP format). It automates the extraction, parsing, and visualization of conversation data from `conversations.csv` files and associated media (images, videos, and other files), even from nested ZIP structures. The tool provides an intuitive graphical user interface (GUI), enabling efficient review, tagging, filtering, and exporting of messages for criminal investigations, evidence triage, and reporting.

Key benefits include:
- **Forensic Integrity:** Preserves original data sources, line numbers, and file hashes for chain-of-custody compliance.
- **Efficiency:** Handles large datasets with virtual scrolling, optimized table rendering, and multi-threaded loading.
- **Customization:** Configurable columns, hotkeys, themes, and export options tailored to investigative workflows.
- **Privacy Features:** Optional blurring of media thumbnails for sensitive content reviews.

This tool is optimized for Internet Crimes Against Children (ICAC) investigations but is versatile for any Snapchat-related digital evidence analysis. It supports Windows environments and is packaged for easy deployment (e.g., via PyInstaller for standalone executables).

---

## Download and Installation

1. **Download:** Visit the [Releases] section on the right to download the latest version (executable or source code).
2. **Prerequisites:** 
   - Windows OS (tested on Windows 10/11).
   - No additional installation required for the standalone executable.
   - For source code installation (not necessary if using the .exe): Python 3.12+ with dependencies (PyQt5, Pillow, OpenCV, pandas, BeautifulSoup4, etc.). Install via `pip install -r requirements.txt` (create one based on imports if needed).
3. **Setup:** Run the executable or execute `python SnapchatParer_v2.0.py` from the source directory.
4. **Updates:** Check the Releases page for new versions. Version 2.0 introduces enhanced media handling, customizable hotkeys, and improved export interactivity.

---

## Key Features

### Data Import and Processing
- **Automated ZIP Parsing:** 
  - Loads Snapchat export ZIP files, including multi-level nested ZIPs.
  - Automatically detects and extracts all `conversations.csv` files and media assets (images, videos, HEIC/HEIF via optional `pillow-heif` support).
  - On-demand media extraction to minimize resource usage.
  - Multi-threaded loading with progress dialogs and interruption support for large datasets.
- **Data Normalization:**
  - Parses user IDs to usernames using built-in mappings.
  - Converts reaction codes to emojis (e.g., ❤️ for love, 😂 for laugh) with user attribution.
  - Handles group member lists with clickable "view" links for detailed inspection.
  - Supports legacy and modern reaction formats.

### User Interface and Navigation
- **Conversation Selector:**
  - Dropdown for selecting individual conversations or "All Conversations" view.
  - Reviewed conversations highlighted in bold red for quick status tracking.
  - Auto-advance to the next unreviewed conversation after marking as reviewed.
- **Message Table View:**
  - Virtual scrolling for efficient handling of thousands of messages.
  - Customizable columns (via right click on column headers): Reorder, hide/show, and resize with persistent settings saved via QSettings.
  - Columns include: Conversation ID/Title, Message ID, Content/Message Type, Date/Time, Sender/Receiver, Media ID/Media Preview, Reactions, Saved/Screenshotted/Replayed By, Group Members, IP/Port, Source File/Line Number, Tags, and more.
  - HTML rendering in cells for rich content (e.g., clickable Snapchat profile links for user IDs).
  - Row coloring based on tags, sender alternation, or custom themes.
- **Theme Support:**
  - Dark mode toggle for reduced eye strain during extended reviews.  
- **Search and Navigation Tools:**
  - Global search across all columns.
  - Keyboard shortcuts for navigation (e.g., arrow keys, page up/down).
  - Double-click to expand media or view details.

### Filtering and Analysis
- **Advanced Filtering:**
  - Filter by date range, sender/receiver, message/content type, saved status, reactions, tags, and more.
  - Live filter application with status indicators (e.g., "Filtered: X/Y messages").
  - One-click reset for all filters.
- **Tagging System:**
  - Predefined tags: CSAM (red), Evidence (orange), Of Interest (yellow), with customizable colors.
  - Custom tags via user-defined labels and hotkeys.
  - Multi-select tagging via Ctrl/Shift-click.
  - Right-click context menu for quick tagging.
  - Persistent tagging stored per case (using conversation hashes for uniqueness).
- **Media Preview and Handling:**
  - Inline thumbnails (100x100px) for images and videos.
  - Optional blurring: CSAM-only, all media, or none (configurable kernel size and sigma for Gaussian blur).
  - Clickable thumbnails open full media in external viewers.
  - Support for multiple media per message.
  - Media type overlays (e.g., "VIDEO") on previews.
- **Investigative Notes:**
  - Add free-form notes to individual conversations.
  - Notes exported as a dedicated section in HTML reports.
  - Toggle visibility in exports.

### Export and Reporting
- **Flexible Export Options:**
  - Formats: HTML (interactive report) or CSV (raw data).
  - Scope: Filtered messages, selected conversation, or all.
  - Custom field selection (e.g., exclude sensitive columns).
  - Sorting by date, sender, or custom order.
- **HTML Export Enhancements:**
  - Interactive table with client-side filtering, searching, sorting, and column resizing.
  - Dark mode toggle in the report.
  - Embedded thumbnails with optional blurring.
  - Export summary section: Scope, format, fields, blur mode, totals (messages, conversations, users, tags, media), date period.
  - "All Notes" collapsible section for investigative annotations.
  - Media hashes table (`file_hashes.csv` embedded) for forensic verification.
  - Legend for tag colors.  
- **CSV Export:**
  - Includes all selected fields with proper escaping for reactions and multi-line content.
- **Copy Tools:**
  - Right-click to copy selected rows (with headers) or individual cells to clipboard.

### Configuration and Utilities
- **Hotkey Customization:**
  - Configurable hotkeys for tagging, navigation, and actions (e.g., Ctrl+1 for CSAM tag).
  - Dialog for editing and saving hotkeys.
- **Column Management:**
  - Reorder, hide, and resize columns with saved preferences.
  - Optimal default widths based on column type (e.g., wide for messages, narrow for dates).
- **Help and Documentation:**
  - Built-in Help dialog with color legend, usage tips, and hotkey list.
- **Logging and Diagnostics:**
  - Toggleable logging (default: off; enable via menu or config.json).
  - Logs to `SnapchatParser.log` with rotation for size management.
  - Detailed error handling with user-friendly messages.
- **Cleanup and Performance:**
  - Automatic temp directory cleanup on exit (media extracts, thumbnails, hash temps).
  - Resource-efficient: Uses queues, threads, and interruption for responsive UI.
  - Supports large files with pandas for data processing.

### Forensic and Security Features
- **Data Integrity:**
  - Tracks original source file and line number for every message.
  - Generates MD5/SHA256 hashes for exported media.
  - Read-only operations on source files.
- **Privacy Controls:**
  - Blur sensitive media during review/export.
  - No internet access required; all processing is local.
- **Case Isolation:**
  - Review progress and tags stored per case hash to prevent cross-contamination.

---

## Usage Guide

1. **Launch the Application:** Run the executable or script.
2. **Import Data:** Go to File > Open ZIP and select your Snapchat export file. Wait for extraction (progress shown).
3. **Select Conversation:** Use the dropdown to choose a conversation or view all.
4. **Apply Filters:** Use the filter panel to narrow down messages (e.g., by date or tag).
5. **Review and Tag:** Browse the table, right-click to tag, or use hotkeys. Add notes via the notes button.
6. **Mark Reviewed:** Click "Mark As Reviewed" to track progress and auto-advance.
7. **Export Data:** Go to File > Export, select format/options, and save.
8. **Customize:** Access settings for, hotkeys, columns, and logging via menus.
9. **Exit:** Close the window; temps are auto-cleaned.

**Tips:**
- Use Ctrl+F for quick search.
- Double-click media cells to view full files.
- Resize columns by dragging headers; sizes are saved.

---

## Configuration

- **Settings File:** Uses QSettings for persistent configs (columns, hotkeys, themes).
- **config.json:** Optional override for logging (per-user).
- **Customization:** Edit hotkeys/tags via dialogs; themes toggled in settings.

---

## Forensic Considerations

- **Chain of Custody:** All messages link back to source CSV line numbers and files.
- **Validation:** Exports include hashes for media integrity checks.
- **Best Practices:** enable logging only when needed to avoid artifacts.
- **Limitations:** Assumes standard Snapchat .csv structure

---

## License

Permission is hereby granted to law-enforcement agencies, digital-forensic analysts, and authorized investigative personnel ("Authorized Users") to use and copy this software for the purpose of criminal investigations, evidence review, training, or internal operational use.

The following conditions apply:

1. **Redistribution:** This software may not be sold, published, or redistributed to the general public. Redistribution outside an authorized agency requires written permission from the developer.

2. **No Warranty:** This software is provided "AS IS," without warranty of any kind, express or implied, including but not limited to the warranties of accuracy, completeness, performance, non-infringement, or fitness for a particular purpose. The developer shall not be liable for any claim, damages, or other liability arising from the use of this software, including the handling of digital evidence.

3. **Evidence Integrity:** Users are responsible for maintaining forensic integrity and chain of custody when handling evidence. This software does not alter source evidence files and is intended only for analysis and review.

4. **Modifications:** Agencies and investigators may modify the software for internal purposes. Modified versions may not be publicly distributed without permission from the developer.

5. **Logging & Privacy:** Users are responsible for controlling log files and output generated during use of the software to prevent unauthorized disclosure of sensitive or personally identifiable information.

6. **Compliance:** Users agree to comply with all applicable laws, departmental policies, and legal requirements when using the software.

By using this software, the user acknowledges that they have read, understood, and agreed to the above terms.

---

## About the Developer

Patrick Koebbe is an Internet Crimes Against Children (ICAC) Investigator with expertise in digital forensics tools. This software was developed to streamline Snapchat data analysis in real-world investigations.

For support, feature requests, or collaborations, contact: koebbe14@gmail.com.
