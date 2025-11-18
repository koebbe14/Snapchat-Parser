<a href="https://www.buymeacoffee.com/koebbe14" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/default-orange.png" alt="Buy Me A Coffee" height="41" width="174"></a>
If you find my programs helpful or enjoy using them, feel free to buy me a coffee 😊


________________________________________
# 📘 Snapchat Parser
Version 1.0

Developer: Patrick Koebbe

Contact: (koebbe14@gmail.com)

## Download by clicking "Releases" to the right 
________________________________________
## 📖 Overview
Snapchat Parser is a forensic analysis tool designed to process Snapchat Responsive Records (.zip file), automatically extract and parse all available conversations.csv and medial files (even inside nested ZIPs), and present the messages in a structured, filterable, taggable, and exportable interface.
This tool is built specifically for law enforcement and digital forensic investigations, supporting workflows such as:
•	Reviewing large volumes of chat messages
•	Identifying CSAM, evidence, or relevant conversations
•	Tagging and triaging messages
•	Tracking review progress
•	Exporting data for reports, court submissions, or follow-up analysis
The application is optimized for speed, ease of use, and investigator workflow efficiency.
________________________________________
## 🚀 Key Features
### 🔍 Smart ZIP Import
•	Loads a full Snapchat export ZIP (even multi-level nested zips)
•	Automatically locates and indexes all conversations.csv files
•	Extracts media files and creates thumbnails for preview
•	Progress dialogs with clear status messaging
________________________________________
### 🗂️ Conversation Management
•	“All Conversations” view or one-at-a-time mode
•	Conversation dropdown auto-highlights reviewed conversations
→ Reviewed items appear bold and red
•	Auto-advance to next conversation after marking reviewed
________________________________________
### 🧭 Advanced Filters
•	Filter by:
o	Date range
o	Sender
o	Message type
o	Content type
o	Saved status
o	Many more fields
•	Live filter status indicator
•	One-click “Clear Filters”
________________________________________
### 🏷️ Message Tagging
•	Tags include:
o	CSAM
o	Evidence
o	Of Interest
o	Custom tags
•	Priority-color-based row highlighting
•	Tag via:
o	Right-click menu
o	Multi-select tagging
o	Hotkeys
________________________________________
### 🖼️ Media Support
•	Inline thumbnails for images and videos
•	Smart extraction from ZIP on-demand
•	Blur option for privacy-sensitive reviews
•	Multi-media message support
•	Clickable images that open full-size media
________________________________________
### 📑Source + Line Number
Each message now shows:
•	Source → folder + CSV name
•	Line Number → original line number in conversations.csv
Essential for forensic chain-of-custody, validation, and traceability.
________________________________________
### 📋 Copying Tools
Right-click any message row:
•	Copy Selected Rows → preserves headers
•	Copy Selected Cell → copies only the clicked cell
Perfect for reports and quick notes.
________________________________________
### 📤 HTML & CSV Export
•	Choose which fields to include
•	Exports respect filters and conversation selection
•	HTML export supports:
o	Full thumbnails
o	Blur mode
o	Horizontal scrolling
•	CSV export 
•	Exported reports include “file_hashes.csv” file for validation
________________________________________
🛠️ Logging (Toggle On/Off)
•	Located in Help → “Enable Logging”
•	Diagnostic logs stored in:
•	SnapchatParser.log
________________________________________
### ❌ Clean Exit with Temp-Dir Cleanup
On exit the parser:
•	Stops background threads safely
•	Deletes:
o	media extract dirs
o	thumbnail dirs
o	hash temp dirs
•	Saves configuration (global settings only)
________________________________________
________________________________________
## 📝 Usage Overview
1.	File → Open ZIP
2.	Wait for the importer to finish
3.	Select a conversation or view all
4.	Apply filters as needed
5.	Tag messages using right-click
6.	Use “Mark As Reviewed” to track progress
7.	Export to HTML or CSV when done
________________________________________
## 🎨 Color Legend
Tag	Meaning	Color
CSAM	Highest priority	🔴 Light Red
Evidence	Important investigative value	🟡 Light Yellow
Of Interest	Triage or potentially relevant	🔵 Light Blue
Non-tagged rows alternate between light gray and light blue based on sender.
Reviewed conversations appear red + bold in the selector.
________________________________________
## 🛡️ Forensic Notes
•	The parser preserves source file / line number for every message.
•	Media is extracted read-only and never modified.
•	Review history is stored per-case using a hash of conversations.csv.
•	Filters reset with each case to prevent cross-case contamination.
•	Logging is disabled by default to reduce unnecessary artifacts.
________________________________________
## 📄 License

Permission is hereby granted to law-enforcement agencies, digital-forensic analysts,
and authorized investigative personnel ("Authorized Users") to use, and copy,
this software for the purpose of criminal investigations, evidence review, training,
or internal operational use.

The following conditions apply:

1. Redistribution
   This software may not be sold, published, or redistributed to the general public.
   Redistribution outside an authorized agency requires written permission from the
   developer.

2. No Warranty
   This software is provided "AS IS," without warranty of any kind, express or implied,
   including but not limited to the warranties of accuracy, completeness, performance,
   non-infringement, or fitness for a particular purpose.

   The developer shall not be liable for any claim, damages, or other liability arising
   from the use of this software, including the handling of digital evidence.

3. Evidence Integrity
   Users are responsible for maintaining forensic integrity and chain of custody when
   handling evidence. This software does not alter source evidence files and is intended
   only for analysis and review.

4. Modifications
   Agencies and investigators may modify the software for internal purposes. Modified
   versions may not be publicly distributed without permission from the developer.

5. Logging & Privacy
   Users are responsible for controlling log files and output generated during use of
   the software to prevent unauthorized disclosure of sensitive or personally identifiable
   information.

6. Compliance
   Users agree to comply with all applicable laws, departmental policies, and legal
   requirements when using the software.

By using this software, the user acknowledges that they have read, understood, and
agreed to the above terms.________________________________________
🧑‍💻 About the Developer
Patrick Koebbe
Internet Crimes Against Children Investigator

