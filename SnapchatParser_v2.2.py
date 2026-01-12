import os, sys, io, re, json, zipfile, tempfile, shutil, logging, datetime, requests, csv, html
from collections import defaultdict
from logging.handlers import RotatingFileHandler
import pandas as pd
import cv2
import base64
import hashlib
import threading
import queue

def resource_path(relative_path):
    """
    Get absolute path to resource, works for dev and for PyInstaller .exe.
    """
    if hasattr(sys, '_MEIPASS'):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(base_path, relative_path)

from PIL import Image, ImageFilter
from bs4 import BeautifulSoup

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
    logging.getLogger(__name__).info("pillow-heif installed: HEIC/HEIF support enabled")
except ImportError:
    logging.getLogger(__name__).warning("pillow-heif not installed: HEIC/HEIF support disabled")

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QFileDialog,
    QTableWidget, QTableWidgetItem, QLabel, QDialog, QGroupBox, QCheckBox, QDialogButtonBox,
    QComboBox, QHeaderView, QMenu, QMessageBox, QLineEdit, QTextEdit, QListWidget, QToolBar,
    QStatusBar, QShortcut, QKeySequenceEdit, QInputDialog, QFrame, QStyledItemDelegate,
    QDateEdit, QListWidgetItem, QSplitter, QProgressDialog, QProgressBar, QStyle, QAbstractItemView,
    QGraphicsBlurEffect, QScrollArea, QAction, QToolButton, QTableView
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QDate, QTimer, QItemSelectionModel, QUrl, QRectF, QSize, QSettings, QAbstractTableModel, QModelIndex
from PyQt5.QtGui import (
    QPixmap, QImage, QBrush, QColor, QFont, QTextDocument, QIcon, 
    QKeySequence, QDesktopServices, QPalette, QGuiApplication
)

# ---------------------------
# LOGGING TOGGLE (default)
# Set to True = logging ON by default
# Set to False = logging OFF by default
# (config.json can override this per user)
# ---------------------------
ENABLE_LOGGING = False

# Logging
LOG = 'SnapchatParser.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)





APP_VERSION = "2.1"


BLUR_KERNEL_SIZE = (401, 401)
BLUR_SIGMA = 93
THUMBNAIL_SIZE = (100, 100)  # Standard thumbnail size for consistency

# =============================================================================
# --- DATA STRUCTURES & UTILITIES ---
# =============================================================================

def parse_reactions(reactions_str, user_id_map=None):
    """
    Parse reactions string and convert reaction integers to emojis.
    Also converts user IDs to usernames if user_id_map is provided.
    
    Format: {user_id}-{reaction_integer} separated by semicolons
    Example: "0bafdfd3-deda-46f8-afe0-e3fa3873bf05-1;7a1c0a6d-bdb3-4c79-9606-987de0fbf0fb-3"
    Also supports legacy format: {user_id} - {reaction_integer} (with spaces) separated by commas
    
    Reaction integer mapping:
    0: Unset, 1: Love ❤️, 2: Laugh Cry 😂, 3: Fire 🔥, 4: Thumbs Up 👍,
    5: Thumbs Down 👎, 6: Sad Cry 😢, 7: Wow 😮, 8: Question Mark ❓,
    9: Kiss 😘, 10: Sobbing 😭, 11: Skull 💀, 12: Exclamation Mark ❗,
    13: Angry 😠, 14: Salute 🫡
    """
    if not reactions_str or not str(reactions_str).strip():
        return ''
    
    reactions_str = str(reactions_str).strip()
    
    # Reaction integer to emoji mapping
    reaction_map = {
        '0': '❓',  # Unset
        '1': '❤️',  # Love
        '2': '😂',  # Laugh Cry
        '3': '🔥',  # Fire
        '4': '👍',  # Thumbs Up
        '5': '👎',  # Thumbs Down
        '6': '😢',  # Sad Cry
        '7': '😮',  # Wow
        '8': '❓',  # Question Mark
        '9': '😘',  # Kiss
        '10': '😭', # Sobbing
        '11': '💀', # Skull
        '12': '❗', # Exclamation Mark
        '13': '😠', # Angry
        '14': '🫡'  # Salute
    }
    
    # Reaction integer to name mapping (for tooltip/fallback)
    reaction_names = {
        '0': 'Unset',
        '1': 'Love',
        '2': 'Laugh Cry',
        '3': 'Fire',
        '4': 'Thumbs Up',
        '5': 'Thumbs Down',
        '6': 'Sad Cry',
        '7': 'Wow',
        '8': 'Question Mark',
        '9': 'Kiss',
        '10': 'Sobbing',
        '11': 'Skull',
        '12': 'Exclamation Mark',
        '13': 'Angry',
        '14': 'Salute'
    }
    
    parsed_reactions = []
    
    # Determine separator: semicolon (new format) or comma (legacy format)
    if ';' in reactions_str:
        # New format: user_id-reaction_integer separated by semicolons
        reaction_parts = [r.strip() for r in reactions_str.split(';')]
    else:
        # Legacy format: user_id - reaction_integer separated by commas
        reaction_parts = [r.strip() for r in reactions_str.split(',')]
    
    for reaction_part in reaction_parts:
        if not reaction_part:
            continue
        
        user_id = None
        reaction_value = None
        
        # Try new format first: user_id-reaction_integer (no spaces, dash at end)
        # Example: "0bafdfd3-deda-46f8-afe0-e3fa3873bf05-1"
        # We need to find the last dash that separates user_id from reaction
        if '-' in reaction_part:
            # Find the last dash (reaction integers are typically 1-2 digits)
            # Split by dash and check if last part is a digit
            parts = reaction_part.split('-')
            if len(parts) >= 2:
                # Check if the last part is a digit (reaction integer)
                last_part = parts[-1]
                if last_part.isdigit():
                    # Last part is the reaction integer
                    reaction_value = last_part
                    # Everything before the last dash is the user_id
                    user_id = '-'.join(parts[:-1])
                else:
                    # Try legacy format: "user_id - reaction" (with spaces)
                    if ' - ' in reaction_part:
                        legacy_parts = reaction_part.split(' - ', 1)
                        user_id = legacy_parts[0].strip()
                        reaction_value = legacy_parts[1].strip() if len(legacy_parts) > 1 else ''
                    else:
                        # No clear separator, might be just a user_id or malformed
                        # Check if entire string is a digit (reaction code without user_id)
                        if reaction_part.isdigit():
                            reaction_value = reaction_part
                            user_id = None
                        else:
                            # Keep as is if we can't parse it
                            parsed_reactions.append(reaction_part)
                            continue
            else:
                # Single dash, might be legacy format
                if ' - ' in reaction_part:
                    legacy_parts = reaction_part.split(' - ', 1)
                    user_id = legacy_parts[0].strip()
                    reaction_value = legacy_parts[1].strip() if len(legacy_parts) > 1 else ''
                else:
                    parsed_reactions.append(reaction_part)
                    continue
        else:
            # No dash at all - check if it's just an integer (reaction code without user_id)
            if reaction_part.isdigit():
                reaction_value = reaction_part
                user_id = None
            else:
                # Keep as is if we can't parse it
                parsed_reactions.append(reaction_part)
                continue
        
        # Convert user_id to username if mapping exists
        display_user = user_id if user_id else ''
        if user_id and user_id_map and user_id in user_id_map:
            display_user = user_id_map[user_id]
        
        # Process reaction value
        if reaction_value:
            if reaction_value.isdigit():
                # Special handling for reaction 0 (Unset) - show text instead of emoji
                if reaction_value == '0':
                    if display_user:
                        parsed_reactions.append(f"{display_user} - Unset/Unspecified")
                    else:
                        parsed_reactions.append("Unset/Unspecified")
                else:
                    emoji = reaction_map.get(reaction_value, '❓')
                    name = reaction_names.get(reaction_value, 'Unknown')
                    if display_user:
                        parsed_reactions.append(f"{display_user} - {emoji} ({name})")
                    else:
                        parsed_reactions.append(f"{emoji} ({name})")
            else:
                # Already an emoji or text, keep as is
                if display_user:
                    parsed_reactions.append(f"{display_user} - {reaction_value}")
                else:
                    parsed_reactions.append(reaction_value)
        else:
            # No reaction value, just user_id
            if display_user:
                parsed_reactions.append(display_user)
            else:
                parsed_reactions.append(reaction_part)
    
    result = ', '.join(parsed_reactions)
    return result


def parse_user_ids_to_usernames(user_ids_str, user_id_map=None, max_display=2):
    """
    Parse user IDs string and convert to usernames.
    If more than max_display user IDs, return HTML link "click to view" and full data.
    
    Args:
        user_ids_str: String containing user IDs (comma or semicolon separated)
        user_id_map: Dictionary mapping user_id to username
        max_display: Maximum number of usernames to display before showing link
    
    Returns:
        tuple: (display_text, full_data_dict)
        - display_text: HTML string to display (usernames or "click to view" link)
        - full_data_dict: Dictionary with 'usernames' and 'user_ids' lists for dialog
    """
    if not user_ids_str or not str(user_ids_str).strip():
        return ('', {'usernames': [], 'user_ids': []})
    
    user_ids_str = str(user_ids_str).strip()
    
    # Parse user IDs - try comma first, then semicolon, then space
    if ',' in user_ids_str:
        user_ids = [uid.strip() for uid in user_ids_str.split(',') if uid.strip()]
    elif ';' in user_ids_str:
        user_ids = [uid.strip() for uid in user_ids_str.split(';') if uid.strip()]
    elif ' ' in user_ids_str:
        user_ids = [uid.strip() for uid in user_ids_str.split() if uid.strip()]
    else:
        user_ids = [user_ids_str] if user_ids_str else []
    
    # Convert user IDs to usernames
    usernames = []
    user_ids_list = []
    
    for user_id in user_ids:
        if user_id:
            user_ids_list.append(user_id)
            # Convert to username if mapping exists
            if user_id_map and user_id in user_id_map:
                usernames.append(user_id_map[user_id])
            else:
                # Keep user_id if no mapping found
                usernames.append(user_id)
    
    # Create full data dict for dialog
    full_data = {
        'usernames': usernames,
        'user_ids': user_ids_list
    }
    
    # Determine display text
    if len(usernames) == 0:
        display_text = ''
    elif len(usernames) <= max_display:
        # Show usernames directly
        display_text = ', '.join(usernames)
    else:
        # More than max_display - show link
        display_text = '<a href="view_users">click to view</a>'
    
    return (display_text, full_data)


def format_group_member_display(data_str):
    """
    Format group member data for compact display in table cells.
    Returns (display_text, member_count, full_data)
    """
    if not data_str or not str(data_str).strip():
        return ('', 0, '')
    
    data_str = str(data_str).strip()
    full_data = data_str
    
    # Handle combined format: "Usernames: ...\nUser IDs: ..."
    if 'Usernames:' in data_str or 'User IDs:' in data_str:
        # Parse the combined format
        usernames = []
        user_ids = []
        
        if 'Usernames:' in data_str:
            usernames_part = data_str.split('User IDs:')[0].replace('Usernames:', '').strip()
            if usernames_part:
                if ',' in usernames_part:
                    usernames = [u.strip() for u in usernames_part.split(',') if u.strip()]
                elif ';' in usernames_part:
                    usernames = [u.strip() for u in usernames_part.split(';') if u.strip()]
                else:
                    usernames = [usernames_part] if usernames_part else []
        
        if 'User IDs:' in data_str:
            userids_part = data_str.split('User IDs:')[1].strip() if 'User IDs:' in data_str else ''
            if userids_part:
                if ',' in userids_part:
                    user_ids = [uid.strip() for uid in userids_part.split(',') if uid.strip()]
                elif ';' in userids_part:
                    user_ids = [uid.strip() for uid in userids_part.split(';') if uid.strip()]
                else:
                    user_ids = [userids_part] if userids_part else []
        
        # Count unique members: usernames and user IDs correspond to the same people
        # So we count the maximum of the two (they should match, but use max to be safe)
        # If one list is empty, use the other; if both exist, they should correspond 1:1
        if usernames and user_ids:
            # Both exist - they should correspond, so count the maximum
            member_count = max(len(usernames), len(user_ids))
        elif usernames:
            member_count = len(usernames)
        elif user_ids:
            member_count = len(user_ids)
        else:
            member_count = 0
        
        # For display, use the first username if available, otherwise first user ID
        if usernames:
            members = usernames
        elif user_ids:
            members = user_ids
        else:
            members = []
    else:
        # Parse to count members (original format)
        if ',' in data_str:
            members = [m.strip() for m in data_str.split(',') if m.strip()]
        elif ';' in data_str:
            members = [m.strip() for m in data_str.split(';') if m.strip()]
        elif '\n' in data_str:
            members = [m.strip() for m in data_str.split('\n') if m.strip()]
        else:
            members = [data_str] if data_str else []
        
        member_count = len(members)
    
    if member_count == 0:
        return ('', 0, '')
    elif member_count == 1:
        # Show the single member, truncated if too long
        display = members[0] if members else ''
        if len(display) > 30:
            display = display[:27] + '...'
        return (display, 1, full_data)
    else:
        # Show link text (will be wrapped in <a> tag by caller)
        return ("click to view", member_count, full_data)



def convert_user_ids_to_usernames(text, user_id_map, return_tooltip=False):
    """
    Convert user IDs in text to usernames using the provided mapping.
    If return_tooltip is True, returns (converted_text, tooltip_text) where tooltip contains user IDs.
    Otherwise returns just converted_text.
    
    Handles user IDs that appear:
    - As standalone values (comma/semicolon separated)
    - In reactions format: "user_id - emoji"
    - In other formats
    """
    if not text or not str(text).strip() or not user_id_map:
        return (text, '') if return_tooltip else text
    
    text_str = str(text).strip()
    
    # Track which user IDs were found for tooltip
    found_user_ids = []
    converted_text = text_str
    
    # Check each user ID in the map and replace it in the text
    for user_id, username in user_id_map.items():
        if user_id in text_str:
            # Replace user ID with username
            converted_text = converted_text.replace(user_id, username)
            found_user_ids.append(f"{user_id} ({username})")
    
    tooltip = ' | '.join(found_user_ids) if found_user_ids else ''
    
    if return_tooltip:
        return (converted_text, tooltip)
    return converted_text


def format_user_ids_as_links(user_ids_str):
    """
    Convert a string of user IDs (comma or semicolon separated) into HTML hyperlinks.
    Each user ID becomes a clickable link to the Snapchat profile.
    Note: Only splits on comma or semicolon to avoid splitting on spaces in column headers.
    """
    if not user_ids_str or not str(user_ids_str).strip():
        return ''
    
    user_ids_str = str(user_ids_str).strip()
    
    # Parse user IDs - ONLY use comma or semicolon as delimiters
    # Do NOT split on spaces as that would break on column headers like "replayed by"
    if ',' in user_ids_str:
        user_ids = [uid.strip() for uid in user_ids_str.split(',') if uid.strip() and len(uid.strip()) > 2]
    elif ';' in user_ids_str:
        user_ids = [uid.strip() for uid in user_ids_str.split(';') if uid.strip() and len(uid.strip()) > 2]
    else:
        # No comma or semicolon - treat as single user ID (if it's long enough)
        user_ids = [user_ids_str] if user_ids_str and len(user_ids_str) > 2 else []
    
    if not user_ids:
        return ''
    
    # Convert each user ID to a hyperlink
    links = []
    for user_id in user_ids:
        if user_id and len(user_id) > 2:  # Only create links for strings longer than 2 chars
            # Snapchat profile URL pattern
            profile_url = f"https://www.snapchat.com/add/{user_id}"
            links.append(f'<a href="{profile_url}" target="_blank" style="color: blue; text-decoration: underline;">{user_id}</a>')
    
    return ' '.join(links)





def configure_table_optimal_sizing(table, headers, table_name="table", settings=None):
    """
    Configure a QTableWidget or QTableView with optimal column widths, row heights, and readability settings.
    
    Args:
        table: QTableWidget or QTableView instance
        headers: List of column header names
        table_name: Unique name for this table (for QSettings persistence)
        settings: QSettings instance (optional, for saving column widths)
    """
    # Detect table type
    from PyQt5.QtWidgets import QTableWidget, QTableView
    is_table_widget = isinstance(table, QTableWidget)
    is_table_view = isinstance(table, QTableView)
    
    # Helper function to get column count
    def get_column_count():
        if is_table_widget:
            return table.columnCount()
        elif is_table_view and table.model():
            return table.model().columnCount()
        else:
            return len(headers)
    
    # Set improved font size (11pt for better readability)
    font = table.font()
    font.setPointSize(11)
    table.setFont(font)
    
    # Enable word wrap for better text display (works for both QTableWidget and QTableView)
    table.setWordWrap(True)
    
    # Set reasonable default row height for consistency
    # Use THUMBNAIL_SIZE + padding for all rows to ensure consistent heights
    default_row_height = THUMBNAIL_SIZE[1] + 25 if 'THUMBNAIL_SIZE' in globals() else 125
    table.verticalHeader().setDefaultSectionSize(default_row_height)
    
    # Configure header for interactive resizing
    header = table.horizontalHeader()
    header.setSectionResizeMode(QHeaderView.Interactive)
    header.setSectionsMovable(True)
    
    # Set min/max section sizes globally
    header.setMinimumSectionSize(50)
    # Remove maximum section size constraint to allow unlimited column expansion
    # header.setMaximumSectionSize(500)  # Removed per user request
    
    # Define optimal default widths (determined by user testing)
    default_widths = {
        # User-determined optimal widths
        "Conversation ID": 540,
        "Conversation Title": 200,
        "Message ID": 100,
        "Reply To": 100,
        "Content Type": 330,
        "Message Type": 240,
        "Date": 180,
        "Time": 140,
        "Sender": 280,
        "Receiver": 280,
        "Message": 740,
        "Media ID": 750,
        "Media": 180,
        "Tags": 160,
        "One-on-One?": 100,
        "Reactions": 330,
        "Saved By": 280,
        "Screenshotted By": 280,
        "Replayed By": 280,
        "Screen Recorded By": 280,
        "Read By": 280,
        "IP": 150,
        "Port": 100,
        "Source": 400,
        "Line Number": 100,
        "Group Members": 280,
        
        # Hotkeys & Tags dialog columns (keep existing)
        "Tag Label": 300,
        "Hotkey": 180,
    }
    
    # Load saved column widths from QSettings first (if available)
    # This allows saved user preferences to override defaults
    saved_widths = {}
    if settings:
        settings.beginGroup(f"TableColumnWidths_{table_name}")
        for i, header_name in enumerate(headers):
            if i >= get_column_count():
                continue
            saved_width = settings.value(header_name, None)
            if saved_width is not None:
                try:
                    width = int(saved_width)
                    # Only apply reasonable min/max constraints (50-2000px range)
                    width = max(50, min(2000, width))
                    saved_widths[i] = width
                except (ValueError, TypeError):
                    pass
        settings.endGroup()
    
    # Auto-size columns first (needed for columns without defaults)
    table.resizeColumnsToContents()
    
    # Apply column widths: saved widths > defaults > auto-size
    for i, header_name in enumerate(headers):
        if i >= get_column_count():
            continue
            
        # Priority: 1) Saved width, 2) Default width, 3) Auto-size
        if i in saved_widths:
            # Use saved user preference
            target_width = saved_widths[i]
        elif header_name in default_widths:
            # Use user-specified default width directly (no constraints)
            target_width = default_widths[header_name]
        else:
            # Use auto-sized width (already computed above)
            target_width = table.columnWidth(i)
            # Apply reasonable constraints for auto-sized columns only
            if target_width < 50:
                target_width = 50
            elif target_width > 1000:
                target_width = 1000
        
        table.setColumnWidth(i, target_width)
        
        # Set minimum width to prevent columns from becoming too narrow
        if header_name in ["Message", "Conversation Title", "Content Type"]:
            header.setMinimumSectionSize(200)
        elif header_name in ["Date", "Time", "IP", "Port"]:
            header.setMinimumSectionSize(80)
        elif header_name in ["One-on-One?", "Line Number"]:
            header.setMinimumSectionSize(120)  # Ensure header is fully visible
        elif header_name == "Message Type":
            header.setMinimumSectionSize(140)  # Ensure header is fully visible
        elif header_name == "Conversation ID":
            header.setMinimumSectionSize(350)  # Wide enough for full UUID display
        else:
            header.setMinimumSectionSize(100)
    
    # Saved widths are now loaded and applied above (before defaults)
    # This section is removed to avoid duplicate application
    
    # Connect to save column widths when user resizes
    def save_column_widths():
        if settings:
            settings.beginGroup(f"TableColumnWidths_{table_name}")
            for i, header_name in enumerate(headers):
                if i < get_column_count():
                    settings.setValue(header_name, table.columnWidth(i))
            settings.endGroup()
            settings.sync()
    
    # Save widths when user manually resizes columns
    header.sectionResized.connect(save_column_widths)
    
    # For QTableView, don't call resizeRowsToContents here - it will be called after data is populated
    # and handled by the _resize_rows_with_thumbnails method
    if is_table_widget:
        # Enable resize rows to contents for better text wrapping (only for QTableWidget)
        table.resizeRowsToContents()
        
        # Connect row height adjustment when data changes
        def adjust_row_heights():
            table.resizeRowsToContents()
        
        # Adjust row heights after data is populated
        QTimer.singleShot(100, adjust_row_heights)



class HtmlDelegate(QStyledItemDelegate):
    def paint(self, painter, option, index):
        self.initStyleOption(option, index)
        # Get the text from the item - it might be HTML
        text = index.data(Qt.DisplayRole)
        if not text:
            text = ""
        
        text_str = str(text)
        
        # Always render as HTML if it contains HTML tags
        doc = QTextDocument()
        if '<' in text_str and '>' in text_str:
            # It's HTML, render it
            doc.setHtml(text_str)
        else:
            # Plain text, set it as plain
            doc.setPlainText(text_str)
        
        doc.setTextWidth(option.rect.width())
        
        # Clear the text from option so the default renderer doesn't draw it
        option.text = ""
        
        # Draw the item background and selection highlight
        style = option.widget.style() or QApplication.style()
        style.drawControl(QStyle.CE_ItemViewItem, option, painter)
        
        # Now draw the HTML content on top
        painter.save()
        painter.translate(option.rect.left(), option.rect.top())
        clip = QRectF(0, 0, option.rect.width(), option.rect.height())
        doc.drawContents(painter, clip)
        painter.restore()

    def sizeHint(self, option, index):
        self.initStyleOption(option, index)
        text = index.data(Qt.DisplayRole)
        if not text:
            text = ""
        text_str = str(text)
        doc = QTextDocument()
        if '<' in text_str and '>' in text_str:
            doc.setHtml(text_str)
        else:
            doc.setPlainText(text_str)
        doc.setTextWidth(option.rect.width())
        return QSize(int(doc.idealWidth()), int(doc.size().height()))
    
    def createEditor(self, parent, option, index):
        # Prevent editing of HTML cells - return None to make them non-editable
        # This prevents the raw HTML from showing when cell is focused
        return None
    
    def editorEvent(self, event, model, option, index):
        """Handle mouse events for clickable links in HTML content.
        
        Note: Link clicks are handled by double-clicking the cell, which triggers
        the on_table_cell_double_clicked handler in the main window.
        """
        # Let the default behavior handle selection, and double-click will open the dialog
        return super().editorEvent(event, model, option, index)


class MessageTableModel(QAbstractTableModel):
    """QAbstractTableModel for virtual scrolling message table."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.messages_data = []  # List of (msg_index, msg, conv_id) tuples
        self.headers = []
        self.compute_row_color_func = None
        self.get_media_path_func = None
        self.user_id_to_username_map = {}
        self.dark_mode = False
        self.theme_manager = None
        self.all_messages = []  # Reference to all_messages list
        self.messages_df = None  # Reference to messages_df
        
    def rowCount(self, parent=QModelIndex()):
        """Return the number of rows."""
        return len(self.messages_data)
    
    def columnCount(self, parent=QModelIndex()):
        """Return the number of columns."""
        return len(self.headers)
    
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self.headers):
                return self.headers[section]
        return None
    
    def data(self, index, role=Qt.DisplayRole):
        """Return data for the given index and role."""
        if not index.isValid() or index.row() >= len(self.messages_data):
            return None
        
        msg_index, msg, conv_id = self.messages_data[index.row()]
        col = index.column()
        
        if role == Qt.DisplayRole:
            header = self.headers[col] if col < len(self.headers) else ""
            
            # Use precomputed date_str and time_str from messages_df if available
            if header == "Date":
                if self.messages_df is not None and msg_index < len(self.messages_df):
                    try:
                        row_data = self.messages_df.iloc[msg_index]
                        date_s = str(row_data.get('date_str', 'N/A')) if 'date_str' in self.messages_df.columns else 'N/A'
                        if date_s == 'N/A':
                            ts = msg.get('timestamp')
                            date_s = ts.strftime("%Y-%m-%d") if ts else 'N/A'
                        return date_s
                    except (IndexError, KeyError):
                        ts = msg.get('timestamp')
                        return ts.strftime("%Y-%m-%d") if ts else 'N/A'
                else:
                    ts = msg.get('timestamp')
                    return ts.strftime("%Y-%m-%d") if ts else 'N/A'
            
            elif header == "Time":
                if self.messages_df is not None and msg_index < len(self.messages_df):
                    try:
                        row_data = self.messages_df.iloc[msg_index]
                        time_s = str(row_data.get('time_str', 'N/A')) if 'time_str' in self.messages_df.columns else 'N/A'
                        if time_s == 'N/A':
                            ts = msg.get('timestamp')
                            time_s = ts.strftime("%H:%M:%S") if ts else 'N/A'
                        return time_s
                    except (IndexError, KeyError):
                        ts = msg.get('timestamp')
                        return ts.strftime("%H:%M:%S") if ts else 'N/A'
                else:
                    ts = msg.get('timestamp')
                    return ts.strftime("%H:%M:%S") if ts else 'N/A'
            
            elif header == "Sender":
                return str(msg.get('sender_username') or msg.get('sender') or '')
            
            elif header == "Receiver":
                return str(msg.get('recipient_username') or msg.get('receiver') or '')
            
            elif header == "Message":
                return str(msg.get('text') or msg.get('message') or '')
            
            elif header == "Tags":
                return ', '.join(sorted(msg.get('tags', set())))
            
            elif header == "Media ID":
                return str(msg.get('media_id') or msg.get('content_id') or '')
            
            elif header == "Conversation ID":
                # For reported files, show blank instead of __REPORTED_FILES__
                conv_id = str(msg.get('conversation_id', ''))
                if conv_id == '__REPORTED_FILES__' or msg.get('is_flagged_media', False):
                    return ''
                return conv_id
            
            elif header == "Conversation Title":
                # For reported files, show blank instead of "Reported Files"
                if msg.get('conversation_id') == '__REPORTED_FILES__' or msg.get('is_flagged_media', False):
                    return ''
                return str(msg.get('conversation_title', ''))
            
            elif header == "Message ID":
                return str(msg.get('message_id', ''))
            
            elif header == "Reply To":
                return str(msg.get('reply_to_message_id', ''))
            
            elif header == "Content Type":
                return str(msg.get('content_type', ''))
            
            elif header == "Message Type":
                return str(msg.get('message_type', ''))
            
            elif header == "One-on-One?":
                return str(msg.get('is_one_on_one', ''))
            
            elif header == "Reactions":
                return parse_reactions(msg.get('reactions', ''), self.user_id_to_username_map if self.user_id_to_username_map else None)
            
            elif header == "Saved By":
                user_ids_str = str(msg.get('saved_by', ''))
                display_text, full_data = parse_user_ids_to_usernames(
                    user_ids_str, 
                    self.user_id_to_username_map if self.user_id_to_username_map else None,
                    max_display=2
                )
                return display_text
            
            elif header == "Screenshotted By":
                user_ids_str = str(msg.get('screenshotted_by', ''))
                display_text, full_data = parse_user_ids_to_usernames(
                    user_ids_str, 
                    self.user_id_to_username_map if self.user_id_to_username_map else None,
                    max_display=2
                )
                return display_text
            
            elif header == "Replayed By":
                user_ids_str = str(msg.get('replayed_by', ''))
                display_text, full_data = parse_user_ids_to_usernames(
                    user_ids_str, 
                    self.user_id_to_username_map if self.user_id_to_username_map else None,
                    max_display=2
                )
                return display_text
            
            elif header == "Screen Recorded By":
                return str(msg.get('screen_recorded_by', ''))
            
            elif header == "Read By":
                user_ids_str = str(msg.get('read_by', ''))
                display_text, full_data = parse_user_ids_to_usernames(
                    user_ids_str, 
                    self.user_id_to_username_map if self.user_id_to_username_map else None,
                    max_display=2
                )
                return display_text
            
            elif header == "IP":
                return str(msg.get('upload_ip', ''))
            
            elif header == "Port":
                return str(msg.get('source_port_number', ''))
            
            elif header == "Source":
                return str(msg.get('source', ''))
            
            elif header == "Line Number":
                return str(msg.get('source_line', ''))
            
            elif header == "Group Members":
                group_usernames = str(msg.get('group_member_usernames', '')).strip()
                group_user_ids = str(msg.get('group_member_user_ids', '')).strip()
                # Combine into format expected by format_group_member_display
                if group_usernames and group_user_ids:
                    combined_data = f"Usernames: {group_usernames}\nUser IDs: {group_user_ids}"
                elif group_usernames:
                    combined_data = f"Usernames: {group_usernames}"
                elif group_user_ids:
                    combined_data = f"User IDs: {group_user_ids}"
                else:
                    combined_data = ''
                
                # Use format_group_member_display to get formatted text
                display_text, member_count, full_data = format_group_member_display(combined_data)
                
                # If more than 1 member, convert to HTML link with member count
                if member_count > 1:
                    return f'<a href="view_users">click to view ({member_count} members)</a>'
                else:
                    return display_text
            
            return ''
        
        elif role == Qt.BackgroundRole:
            # Compute background color
            if self.compute_row_color_func:
                bg_color = self.compute_row_color_func(msg, conv_id, row_index=index.row())
                return QBrush(QColor(bg_color))
        
        elif role == Qt.TextAlignmentRole:
            return Qt.AlignLeft | Qt.AlignTop
        
        elif role == Qt.ToolTipRole:
            if col < len(self.headers):
                header = self.headers[col]
                if header == "Media":
                    media_info = self.data(index, Qt.UserRole)
                    if media_info and isinstance(media_info, dict):
                        content_path = media_info.get('content_path', '')
                        if content_path and os.path.exists(content_path):
                            return os.path.basename(content_path)
                elif header == "Source":
                    return msg.get('source', 'Unknown')
                elif header == "Group Members":
                    group_usernames = str(msg.get('group_member_usernames', '')).strip()
                    group_user_ids = str(msg.get('group_member_user_ids', '')).strip()
                    if group_usernames and group_user_ids:
                        return f"Usernames: {group_usernames}\nUser IDs: {group_user_ids}"
                    elif group_usernames:
                        return f"Usernames: {group_usernames}"
                    elif group_user_ids:
                        return f"User IDs: {group_user_ids}"
        
        elif role == Qt.UserRole:
            # Store message index and media info
            if col < len(self.headers):
                header = self.headers[col]
                if header == "Message":
                    return msg_index  # Store message index for retrieval
                elif header == "Media":
                    media_id = str(msg.get('media_id') or msg.get('content_id') or '')
                    if media_id and self.get_media_path_func:
                        media_path_result = self.get_media_path_func(media_id, msg_index)
                        if media_path_result:
                            # Handle both single path and list of paths
                            if isinstance(media_path_result, list):
                                # Multiple media paths
                                media_paths = [p for p in media_path_result if p and os.path.exists(p)]
                                if media_paths:
                                    # Get individual media IDs for each path (needed for per-thumbnail blur)
                                    # Access the mapping from the bound method instance
                                    individual_media_ids = {}
                                    if hasattr(self.get_media_path_func, '__self__'):
                                        model_instance = self.get_media_path_func.__self__
                                        if hasattr(model_instance, '_media_path_to_id_map'):
                                            for path in media_paths:
                                                if path in model_instance._media_path_to_id_map:
                                                    individual_media_ids[path] = model_instance._media_path_to_id_map[path]
                                    
                                    return {
                                        'media_id': media_id,
                                        'media_paths': media_paths,
                                        'content_paths': media_paths,
                                        'individual_media_ids': individual_media_ids,  # Map path -> media_id for blur tracking
                                        'msg_index': msg_index
                                    }
                            else:
                                # Single media path
                                if os.path.exists(media_path_result):
                                    return {
                                        'media_id': media_id,
                                        'media_path': media_path_result,
                                        'content_path': media_path_result,
                                        'msg_index': msg_index
                                    }
                elif header == "Saved By":
                    user_ids_str = str(msg.get('saved_by', ''))
                    display_text, full_data = parse_user_ids_to_usernames(
                        user_ids_str, 
                        self.user_id_to_username_map if self.user_id_to_username_map else None,
                        max_display=2
                    )
                    return full_data
                
                elif header == "Screenshotted By":
                    user_ids_str = str(msg.get('screenshotted_by', ''))
                    display_text, full_data = parse_user_ids_to_usernames(
                        user_ids_str, 
                        self.user_id_to_username_map if self.user_id_to_username_map else None,
                        max_display=2
                    )
                    return full_data
                
                elif header == "Replayed By":
                    user_ids_str = str(msg.get('replayed_by', ''))
                    display_text, full_data = parse_user_ids_to_usernames(
                        user_ids_str, 
                        self.user_id_to_username_map if self.user_id_to_username_map else None,
                        max_display=2
                    )
                    return full_data
                
                elif header == "Read By":
                    user_ids_str = str(msg.get('read_by', ''))
                    display_text, full_data = parse_user_ids_to_usernames(
                        user_ids_str, 
                        self.user_id_to_username_map if self.user_id_to_username_map else None,
                        max_display=2
                    )
                    return full_data
                
                elif header == "Group Members":
                    group_usernames = str(msg.get('group_member_usernames', '')).strip()
                    group_user_ids = str(msg.get('group_member_user_ids', '')).strip()
                    group_members_combined = ''
                    if group_usernames and group_user_ids:
                        group_members_combined = f"Usernames: {group_usernames}\nUser IDs: {group_user_ids}"
                    elif group_usernames:
                        group_members_combined = f"Usernames: {group_usernames}"
                    elif group_user_ids:
                        group_members_combined = f"User IDs: {group_user_ids}"
                    return group_members_combined
        
        return None
    
    def flags(self, index):
        """Return item flags."""
        if not index.isValid():
            return Qt.NoItemFlags
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable
    
    def setMessages(self, messages_data, headers, compute_row_color_func, get_media_path_func, 
                    user_id_to_username_map, dark_mode, theme_manager, all_messages, messages_df):
        """Update the model with new messages."""
        self.beginResetModel()
        self.messages_data = messages_data  # List of (msg_index, msg, conv_id) tuples
        self.headers = headers
        self.compute_row_color_func = compute_row_color_func
        self.get_media_path_func = get_media_path_func
        self.user_id_to_username_map = user_id_to_username_map
        self.dark_mode = dark_mode
        self.theme_manager = theme_manager
        self.all_messages = all_messages
        self.messages_df = messages_df
        self.endResetModel()
    
    def getMessageAtRow(self, row):
        """Get the message data at the given row."""
        if 0 <= row < len(self.messages_data):
            return self.messages_data[row][1]  # Return the message dict
        return None
    
    def getMessageIndexAtRow(self, row):
        """Get the message index at the given row."""
        if 0 <= row < len(self.messages_data):
            return self.messages_data[row][0]  # Return the msg_index
        return None
    
    def getConvIdAtRow(self, row):
        """Get the conversation ID at the given row."""
        if 0 <= row < len(self.messages_data):
            return self.messages_data[row][2]  # Return the conv_id
        return None


class MediaThumbnailDelegate(QStyledItemDelegate):
    """Custom delegate for rendering media thumbnails in the table."""
    
    def __init__(self, parent=None, main_window=None):
        super().__init__(parent)
        self.main_window = main_window
        # If main_window not provided, try to find it
        if not self.main_window:
            for w in QApplication.topLevelWidgets():
                if isinstance(w, type(parent)) if parent else False:
                    self.main_window = w
                    break
    
    def paint(self, painter, option, index):
        """Paint the thumbnail(s) or content_id text."""
        # Get media info from UserRole
        media_info = index.data(Qt.UserRole)
        
        if not media_info or not isinstance(media_info, dict):
            # No media, just show text
            super().paint(painter, option, index)
            return
        
        # Check for multiple media paths
        content_paths = media_info.get('content_paths', [])
        if not content_paths:
            # Fall back to single content_path for backward compatibility
            content_path = media_info.get('content_path', '')
            if content_path and os.path.exists(content_path):
                content_paths = [content_path]
            else:
                super().paint(painter, option, index)
                return
        
        # Draw background
        painter.save()
        if option.state & QStyle.State_Selected:
            painter.fillRect(option.rect, option.palette.highlight())
        else:
            bg_color = index.data(Qt.BackgroundRole)
            if bg_color:
                painter.fillRect(option.rect, bg_color)
        
        # Get global blur setting
        global_blur = self.main_window and hasattr(self.main_window, 'blur_all') and self.main_window.blur_all
        
        # Get individual media IDs mapping (for per-thumbnail blur tracking)
        individual_media_ids = media_info.get('individual_media_ids', {})
        
        # Calculate spacing and positioning for multiple thumbnails
        num_thumbnails = len(content_paths)
        thumbnail_width = THUMBNAIL_SIZE[0]
        thumbnail_height = THUMBNAIL_SIZE[1]
        spacing = 5  # Space between thumbnails
        total_width = (thumbnail_width * num_thumbnails) + (spacing * (num_thumbnails - 1))
        start_x = option.rect.x() + max(0, (option.rect.width() - total_width) // 2)
        y = option.rect.y() + 2
        
        # Draw each thumbnail
        try:
            for i, content_path in enumerate(content_paths):
                if not content_path or not os.path.exists(content_path):
                    continue
                
                # Check if this specific thumbnail should be blurred
                # First check if there's an individual media_id for this path
                individual_media_id = individual_media_ids.get(content_path, '')
                individual_blur = False
                if individual_media_id and self.main_window and hasattr(self.main_window, 'blurred_thumbnails'):
                    individual_blur = individual_media_id in self.main_window.blurred_thumbnails
                
                # If no individual_media_id, fall back to checking the combined media_id
                if not individual_media_id:
                    media_id = media_info.get('media_id', '')
                    if media_id and self.main_window and hasattr(self.main_window, 'blurred_thumbnails'):
                        individual_blur = media_id in self.main_window.blurred_thumbnails
                
                # Blur if global blur is on OR if this specific thumbnail is individually blurred
                should_blur = global_blur or individual_blur
                
                ext = os.path.splitext(content_path)[1].lower()
                is_video = ext in ['.mp4', '.webm', '.ogg', '.mov', '.avi', '.mkv']
                is_image = ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.heic', '.heif']
                
                if not (is_video or is_image):
                    continue
                
                # Load thumbnail
                if is_image:
                    pixmap = QPixmap(content_path)
                else:
                    # For video, try to load thumbnail (generated during extraction)
                    thumb_path = content_path + '_thumb.jpg'
                    if os.path.exists(thumb_path):
                        pixmap = QPixmap(thumb_path)
                    else:
                        # Generate thumbnail on the fly
                        try:
                            cap = cv2.VideoCapture(content_path)
                            if cap.isOpened():
                                ret, frame = cap.read()
                                if ret:
                                    cv2.imwrite(thumb_path, frame)
                                    pixmap = QPixmap(thumb_path)
                                else:
                                    pixmap = None
                            else:
                                pixmap = None
                            cap.release()
                        except:
                            pixmap = None
                
                if pixmap and not pixmap.isNull():
                    # Scale to THUMBNAIL_SIZE while maintaining aspect ratio
                    scaled_pixmap = pixmap.scaled(
                        THUMBNAIL_SIZE[0], THUMBNAIL_SIZE[1],
                        Qt.KeepAspectRatio, Qt.SmoothTransformation
                    )
                    
                    # Calculate x position for this thumbnail
                    x = start_x + (i * (thumbnail_width + spacing))
                    
                    # Apply blur if needed
                    if should_blur:
                        try:
                            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp_file:
                                tmp_path = tmp_file.name
                            scaled_pixmap.save(tmp_path, 'JPG')
                            img = cv2.imread(tmp_path)
                            if img is not None:
                                blurred_img = cv2.GaussianBlur(img, BLUR_KERNEL_SIZE, BLUR_SIGMA)
                                cv2.imwrite(tmp_path, blurred_img)
                                scaled_pixmap = QPixmap(tmp_path)
                                try:
                                    os.unlink(tmp_path)
                                except:
                                    pass
                        except Exception:
                            pass
                    
                    painter.drawPixmap(x, y, scaled_pixmap)
                    
                    # Draw media type tag below thumbnail
                    tag_text = "(VID)" if is_video else "(IMG)"
                    font = painter.font()
                    font.setPointSize(8)
                    painter.setFont(font)
                    painter.setPen(QColor('gray'))
                    tag_rect = QRectF(x, option.rect.bottom() - 16, thumbnail_width, 14)
                    painter.drawText(tag_rect, Qt.AlignCenter, tag_text)
        except Exception:
            pass
        
        painter.restore()
    
    def sizeHint(self, option, index):
        """Return size hint for the thumbnail cell - this determines row height."""
        media_info = index.data(Qt.UserRole)
        if media_info and isinstance(media_info, dict):
            # Check for multiple media paths
            content_paths = media_info.get('content_paths', [])
            if not content_paths:
                # Fall back to single content_path
                content_path = media_info.get('content_path', '')
                if content_path:
                    content_paths = [content_path]
            
            if content_paths:
                # Check if any path is valid media
                valid_paths = [p for p in content_paths if p and os.path.exists(p)]
                if valid_paths:
                    # Check if first path is valid media type
                    first_path = valid_paths[0]
                    ext = os.path.splitext(first_path)[1].lower()
                    is_video = ext in ['.mp4', '.webm', '.ogg', '.mov', '.avi', '.mkv']
                    is_image = ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.heic', '.heif']
                    if is_video or is_image:
                        # Calculate width based on number of thumbnails
                        num_thumbnails = len(valid_paths)
                        thumbnail_width = THUMBNAIL_SIZE[0]
                        spacing = 5
                        total_width = (thumbnail_width * num_thumbnails) + (spacing * (max(0, num_thumbnails - 1)))
                        # Return height that accommodates thumbnail + label
                        return QSize(max(total_width + 20, option.rect.width() if option.rect.width() > 0 else 100), THUMBNAIL_SIZE[1] + 25)
        
        # Default size for non-media cells - use consistent height matching media rows
        # This ensures all rows have the same height to prevent overlapping
        return QSize(option.rect.width() if option.rect.width() > 0 else 100, THUMBNAIL_SIZE[1] + 25)
    
    def editorEvent(self, event, model, option, index):
        """Handle mouse events for opening media files and right-click blur."""
        media_info = index.data(Qt.UserRole)
        if not media_info or not isinstance(media_info, dict):
            return super().editorEvent(event, model, option, index)
        
        # Check for multiple media paths
        content_paths = media_info.get('content_paths', [])
        if not content_paths:
            # Fall back to single content_path
            content_path = media_info.get('content_path', '')
            if content_path:
                content_paths = [content_path]
        
        if not content_paths:
            return super().editorEvent(event, model, option, index)
        
        # Determine which thumbnail was clicked based on mouse position
        click_x = event.pos().x() - option.rect.x()
        thumbnail_width = THUMBNAIL_SIZE[0]
        spacing = 5
        
        # Calculate total width and start position (same as in paint method)
        num_thumbnails = len(content_paths)
        total_width = (thumbnail_width * num_thumbnails) + (spacing * (num_thumbnails - 1))
        start_x = max(0, (option.rect.width() - total_width) // 2)
        
        # Adjust click_x relative to start_x
        relative_x = click_x - start_x
        
        # Find which thumbnail was clicked
        clicked_index = int(relative_x / (thumbnail_width + spacing)) if (thumbnail_width + spacing) > 0 else 0
        clicked_index = min(clicked_index, len(content_paths) - 1)
        clicked_index = max(0, clicked_index)
        
        clicked_path = content_paths[clicked_index]
        
        # Handle double-click to open media
        if event.type() == event.MouseButtonDblClick and event.button() == Qt.LeftButton:
            if clicked_path and os.path.exists(clicked_path):
                QDesktopServices.openUrl(QUrl.fromLocalFile(clicked_path))
                return True
        
        # Handle right-click context menu for blur
        elif event.type() == event.MouseButtonPress and event.button() == Qt.RightButton:
            # Check if click is within thumbnail bounds
            thumbnail_x = start_x + (clicked_index * (thumbnail_width + spacing))
            if thumbnail_x <= click_x <= thumbnail_x + thumbnail_width:
                # Get individual media_id for this thumbnail
                individual_media_ids = media_info.get('individual_media_ids', {})
                individual_media_id = individual_media_ids.get(clicked_path, '')
                
                # If no individual_media_id, use the combined media_id (for single media)
                if not individual_media_id:
                    individual_media_id = media_info.get('media_id', '')
                
                if individual_media_id and self.main_window:
                    # Check if this thumbnail is currently blurred
                    is_blurred = individual_media_id in getattr(self.main_window, 'blurred_thumbnails', set())
                    
                    # Create context menu
                    menu = QMenu()
                    blur_action = menu.addAction("Unblur Media" if is_blurred else "Blur Media")
                    action = menu.exec_(event.globalPos())
                    
                    if action == blur_action:
                        # Toggle blur for this specific thumbnail
                        if not hasattr(self.main_window, 'blurred_thumbnails'):
                            self.main_window.blurred_thumbnails = set()
                        
                        if is_blurred:
                            self.main_window.blurred_thumbnails.discard(individual_media_id)
                        else:
                            self.main_window.blurred_thumbnails.add(individual_media_id)
                        
                        # Trigger repaint of this cell
                        model.dataChanged.emit(index, index, [])
                        self.main_window.message_table.viewport().update()
                        return True
        
        return super().editorEvent(event, model, option, index)


def scan_zip_recursive(zip_path):
    entries = []
    conv_files = []
    def _scan(zpath, zfile, prefix=""):
        nonlocal conv_files
        for info in zfile.infolist():
            name = info.filename
            internal = prefix + name if prefix else name
            entries.append((zpath, internal))
            if os.path.basename(name).lower() == 'conversations.csv':
                conv_files.append((zpath, internal))
            if name.lower().endswith('.zip') and not info.is_dir():
                try:
                    data = zfile.read(name)
                    with zipfile.ZipFile(io.BytesIO(data)) as nested:
                        _scan(zpath, nested, prefix=internal + "!")
                except zipfile.BadZipFile as e:
                    logger.debug(f"Can't open nested zip {name}: {e}")
                except Exception as e:
                    logger.debug(f"Error processing nested zip {name}: {e}")
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            _scan(zip_path, z, prefix="")
    except zipfile.BadZipFile as e:
        logger.error(f"Failed to open {zip_path}: {e}")
        raise
    return entries, conv_files
    
def get_file_bytes_from_zip(zip_path, internal):
    parts = internal.split('!')
    cur = zip_path
    try:
        for part in parts:
            with zipfile.ZipFile(cur, 'r') as z:
                if part == parts[-1]:
                    return z.read(part)
                else:
                    raw = z.read(part)
                    cur = io.BytesIO(raw)
    except:
        return None

def build_media_index(zip_path, build_token_index=False):
    """
    Build media index. If build_token_index=False (default), only builds basic mapping and basenames.
    Token index is built lazily on demand to improve initial import speed.
    """
    mapping = {}
    basenames = []
    token_index = {}  # Lazy: only built when build_token_index=True
    # Only raise error on BadZipFile, suppress others during deep scan
    try:
        entries, conv_files = scan_zip_recursive(zip_path)
    except zipfile.BadZipFile as e:
        raise e
    except Exception as e:
        logger.error(f"Unknown error during ZIP indexing: {e}")
        return mapping, basenames, conv_files, token_index

    for zpath, internal in entries:
        base = os.path.basename(internal)
        if not base: continue
        basenames.append((base, zpath, internal))
        mapping.setdefault(base, (zpath, internal))
        mapping.setdefault(base.lower(), (zpath, internal))
        
        # Only build token index if explicitly requested (lazy loading)
        if build_token_index:
            # Build token index: extract tokens from filename and internal path for fast lookup
            # Look for actual media ID tokens like "EiASF..." (not folder names or dates)
            tokens_in_base = []
            
            # Pattern 1: "b~" followed by base64-like string (most common)
            b_tokens = re.findall(r'b~([A-Za-z0-9_\-]{20,})', base)
            tokens_in_base.extend(b_tokens)
            
            # Pattern 2: Base64-like strings starting with letters (not dates/folder names)
            base64_like = re.findall(r'([A-Z][A-Za-z0-9_]{19,})', base)
            for token in base64_like:
                if not re.search(r'[-_]{2,}', token) and token.count('-') < 3 and token.count('_') < 3:
                    tokens_in_base.append(token)
            
            # Pattern 3: 32-character hex strings
            hex_tokens = re.findall(r'([0-9a-fA-F]{32})', base)
            tokens_in_base.extend(hex_tokens)
            
            # Index tokens (normalize to lowercase for case-insensitive matching)
            for token in set(tokens_in_base):
                token_clean = token.lower().strip()
                if len(token_clean) >= 20:
                    if (token_clean.count('-') < 4 and token_clean.count('_') < 4 and 
                        not re.match(r'^\d{4}-\d{2}-\d{2}', token_clean)):
                        if token_clean not in token_index:
                            token_index[token_clean] = []
                        if (zpath, internal) not in token_index[token_clean]:
                            token_index[token_clean].append((zpath, internal))
    
    return mapping, basenames, conv_files, token_index

def find_media_by_media_id(media_id, basenames, token_index=None, cache=None):
    """
    Optimized lookup using token index for O(1) lookups instead of O(n) linear search.
      - Uses token_index dictionary for fast lookups when available
      - Falls back to linear search if token_index not provided
      - Uses cache to avoid reprocessing the same media_id
    Returns a list of (zpath, internal) tuples for each found match.
    """
    if not media_id:
        return []

    raw = str(media_id).strip()
    
    # Check cache first to avoid reprocessing
    if cache is not None and raw in cache:
        return cache[raw]
    
    # Reduced logging in hot paths - only log if verbose logging enabled
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Processing media_id: '{raw}'")
    
    # Extract tokens matching the same patterns used in indexing
    # Pattern 1: "b~" followed by base64-like string
    b_tokens = re.findall(r'b~([A-Za-z0-9_\-]{20,})', raw)
    cleaned_tokens = [t.lower().strip() for t in b_tokens if t.strip()]
    
    # Pattern 2: Base64-like strings starting with letters (not dates/folder names)
    base64_like = re.findall(r'([A-Z][A-Za-z0-9_]{19,})', raw)
    for token in base64_like:
        token_clean = token.lower().strip()
        # Filter out folder-like tokens (same logic as indexing)
        if (len(token_clean) >= 20 and not re.search(r'[-_]{2,}', token_clean) and 
            token_clean.count('-') < 3 and token_clean.count('_') < 3 and
            not re.match(r'^\d{4}-\d{2}-\d{2}', token_clean)):
            if token_clean not in cleaned_tokens:
                cleaned_tokens.append(token_clean)
    
    # Pattern 3: 32-character hex strings
    hex_tokens = re.findall(r'([0-9a-fA-F]{32})', raw)
    for token in hex_tokens:
        token_clean = token.lower().strip()
        if token_clean not in cleaned_tokens:
            cleaned_tokens.append(token_clean)
            
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Extracted and cleaned tokens: {cleaned_tokens}")

    matches = []
    seen_paths = set()  # Avoid duplicates
    
    # Use token_index for fast lookup if available
    if token_index is not None:
        for token in cleaned_tokens:
            token_lower = token.lower()
            # Direct lookup in token_index
            if token_lower in token_index:
                for zpath, internal in token_index[token_lower]:
                    path_key = (zpath, internal)
                    if path_key not in seen_paths:
                        matches.append((zpath, internal))
                        seen_paths.add(path_key)
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Match found for token '{token}': zpath='{zpath}', internal='{internal}'")
                if matches:  # Found match, no need to continue searching
                    break
    else:
        # Fallback to linear search if no token_index provided
        for token in cleaned_tokens:
            token_lower = token.lower()
            for base, zpath, internal in basenames:
                if token_lower in (base or "").lower() or token_lower in (internal or "").lower():
                    path_key = (zpath, internal)
                    if path_key not in seen_paths:
                        matches.append((zpath, internal))
                        seen_paths.add(path_key)
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Match found for token '{token}': zpath='{zpath}', internal='{internal}'")
                        break  # stop after first match for this token
                
    # Reduced logging - only log if verbose
    if logger.isEnabledFor(logging.INFO) and len(matches) > 0:
        logger.info(f"Total matches for media_id '{raw}': {len(matches)}")
    
    # Cache the result
    if cache is not None:
        cache[raw] = matches
    
    return matches


def find_reported_file_media(media_id, basenames, cache=None):
    """
    Find media files for reported/flagged files by searching for filenames containing the media_id.
    
    This is more robust than pattern matching since filename formats may vary, but the media_id
    will always be embedded in the filename.
    
    Args:
        media_id: The media ID from the CSV (e.g., "53bc312a-415a-5771-aa79-08c1a983e370-31")
        basenames: List of (basename, zpath, internal) tuples
        cache: Optional cache dictionary
    
    Returns:
        List of (zpath, internal) tuples for matching files
    """
    if not media_id:
        return []
    
    raw = str(media_id).strip()
    
    # Check cache first to avoid reprocessing
    if cache is not None and raw in cache:
        return cache[raw]
    
    # Convert media_id to lowercase for case-insensitive matching
    media_id_lower = raw.lower()
    
    matches = []
    seen_paths = set()  # Avoid duplicates
    
    # Search through all basenames for filenames containing the media_id
    for base, zpath, internal in basenames:
        # Check both basename and internal path (case-insensitive)
        base_lower = (base or "").lower()
        internal_lower = (internal or "").lower()
        
        # If media_id is found in either basename or internal path, it's a match
        if media_id_lower in base_lower or media_id_lower in internal_lower:
            path_key = (zpath, internal)
            if path_key not in seen_paths:
                matches.append((zpath, internal))
                seen_paths.add(path_key)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Reported file match found for media_id '{raw}': zpath='{zpath}', internal='{internal}'")
    
    # Reduced logging - only log if verbose
    if logger.isEnabledFor(logging.INFO) and len(matches) > 0:
        logger.info(f"Total matches for reported file media_id '{raw}': {len(matches)}")
    
    # Cache the result
    if cache is not None:
        cache[raw] = matches
    
    return matches


def extract_file_from_zip(zip_path, internal_name, dest_dir=None):
    if dest_dir is None: dest_dir = tempfile.mkdtemp(prefix="snap_media_")
    # Ensure destination directory exists
    os.makedirs(dest_dir, exist_ok=True)
    # Using 'w' for simple file path, or BytesIO for nested zip (cur)
    cur = zip_path
    try:
        parts = internal_name.split("!")
        logger.debug(f"Extracting file: {internal_name}")
        for i, part in enumerate(parts):
            with zipfile.ZipFile(cur, 'r') as z:
                if i == len(parts)-1:
                    dest = os.path.join(dest_dir, os.path.basename(part))
                    # Check if file already exists - avoid re-extracting
                    if os.path.exists(dest):
                        try:
                            # Verify it's a valid file (not corrupted)
                            if os.path.getsize(dest) > 0:
                                logger.debug(f"File already extracted, reusing: {dest}")
                                return dest
                        except OSError:
                            pass  # If we can't check, re-extract
                    logger.debug(f"Extracting to: {dest}")
                    with open(dest, 'wb') as dst:
                        shutil.copyfileobj(z.open(part), dst)
                    logger.debug(f"Successfully extracted: {dest}")
                    return dest
                else:
                    raw = z.read(part)
                    cur = io.BytesIO(raw)
    except zipfile.BadZipFile as e:
        logger.error(f"ZIP file error extracting {internal_name}: {e}")
    except Exception as e:
        logger.error(f"Extract error for {internal_name}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    return None

def generate_thumbnail(media_path, thumb_dir, size=THUMBNAIL_SIZE):
    os.makedirs(thumb_dir, exist_ok=True)
    name, ext = os.path.splitext(os.path.basename(media_path))
    thumb = os.path.join(thumb_dir, name + "_thumb.png")
    
    # Check if thumbnail already exists - avoid regenerating
    if os.path.exists(thumb) and os.path.exists(media_path):
        # Verify the thumbnail is newer than the source (or source doesn't exist anymore)
        try:
            thumb_time = os.path.getmtime(thumb)
            media_time = os.path.getmtime(media_path)
            if thumb_time >= media_time:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Thumbnail already exists, reusing: {thumb}")
                return thumb  # Thumbnail exists and is up to date
        except OSError:
            pass  # If we can't check times, regenerate
    
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Generating thumbnail for: {media_path}")
    try:
        ext_l = ext.lower()
        if ext_l in ['.jpg','.jpeg','.png','.gif','.webp','.bmp','.heic','.heif']: 
            im = Image.open(media_path)
            im.thumbnail(size, Image.LANCZOS)
            # Create square with transparent background
            square_im = Image.new('RGBA', size, (0, 0, 0, 0))  # transparent background
            offset = ((size[0] - im.width) // 2, (size[1] - im.height) // 2)
            square_im.paste(im.convert('RGBA'), offset, im.convert('RGBA'))
            square_im.save(thumb, "PNG", quality=85)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Successfully generated thumbnail: {thumb}")
            return thumb
        if ext_l in ['.mp4','.mov','.webm','.avi','.mkv','.ogg']:
            logger.debug(f"Extracting frame from video: {media_path}")
            # Use timeout mechanism to prevent hanging on corrupted videos
            def read_frame_with_timeout(cap, timeout=5):
                """Read a frame from VideoCapture with timeout to prevent hanging"""
                result_queue = queue.Queue()
                def read_frame():
                    try:
                        ret, frame = cap.read()
                        result_queue.put((ret, frame))
                    except Exception as e:
                        result_queue.put((False, None))
                        logger.error(f"Exception reading video frame: {e}")
                
                thread = threading.Thread(target=read_frame, daemon=True)
                thread.start()
                thread.join(timeout=timeout)
                
                if thread.is_alive():
                    logger.warning(f"Video frame read timed out after {timeout}s for: {media_path}")
                    return False, None
                
                try:
                    return result_queue.get_nowait()
                except queue.Empty:
                    logger.warning(f"No result from video frame read for: {media_path}")
                    return False, None
            
            cap = cv2.VideoCapture(media_path)
            if cap.isOpened():
                ret, frame = read_frame_with_timeout(cap, timeout=5)
                cap.release()
                if ret and frame is not None:
                    try:
                        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        im = Image.fromarray(frame)
                        im.thumbnail(size, Image.LANCZOS)
                        # Create square with transparent background
                        square_im = Image.new('RGBA', size, (0, 0, 0, 0))  # transparent background
                        offset = ((size[0] - im.width) // 2, (size[1] - im.height) // 2)
                        square_im.paste(im.convert('RGBA'), offset, im.convert('RGBA'))
                        square_im.save(thumb, "PNG", quality=80)
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Successfully generated video thumbnail: {thumb}")
                        return thumb
                    except Exception as e:
                        logger.error(f"Error processing video frame: {e}")
                else:
                    logger.warning(f"Failed to read frame from video: {media_path}")
            else:
                logger.warning(f"Failed to open video: {media_path}")
    except Exception as e:
        logger.error(f"Thumbnail generation failed for {media_path}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    return None


class ThemeManager:
    """Manages light and dark theme colors for the application."""
    
    LIGHT_THEME = {
        # Background colors
        'bg_main': '#f4f4f9',
        'bg_widget': '#ffffff',
        'bg_alternate': '#f5f5f5',
        'bg_dialog': '#f4f4f9',
        'bg_table': '#ffffff',
        'bg_table_alternate': '#f5f5f5',
        'bg_table_hover': '#f1f1f1',
        'bg_groupbox': '#ffffff',
        'bg_legend': '#f9f9f9',
        
        # Text colors
        'text_primary': '#000000',
        'text_secondary': '#333333',
        'text_white': '#ffffff',
        'text_black': '#000000',
        
        # Border colors
        'border': '#d0d0d0',
        
        # Button colors
        'button_hover': '#e0e0e0',
        'button_bg': '#ffffff',
        
        # Tag colors (keep same for visibility)
        'tag_csam': '#ff0000',
        'tag_evidence': '#ff8000',
        'tag_interest': '#ffff00',
        'tag_custom': '#e0ffe0',
        
        # Keyword colors
        'keyword_hit': '#ffffe0',
        'keyword_hit_alt': '#9fe780',
        
        # Sender colors
        'sender1': '#f3f6f4',
        'sender2': '#9fc5e8',
        
        # Default row colors
        'row_default': '#ffffff',
        'row_alternate': '#f5f5f5',
    }
    
    DARK_THEME = {
        # Background colors
        'bg_main': '#2b2b2b',
        'bg_widget': '#3c3c3c',
        'bg_alternate': '#404040',
        'bg_dialog': '#2b2b2b',
        'bg_table': '#3c3c3c',
        'bg_table_alternate': '#4a4a4a',  # More distinguishable from bg_table
        'bg_table_hover': '#555555',
        'bg_groupbox': '#3c3c3c',
        'bg_legend': '#404040',
        
        # Text colors
        'text_primary': '#e0e0e0',
        'text_secondary': '#d0d0d0',
        'text_white': '#ffffff',
        'text_black': '#000000',
        
        # Border colors
        'border': '#555555',
        
        # Button colors
        'button_hover': '#505050',
        'button_bg': '#3c3c3c',
        
        # Tag colors (adjusted for dark mode but still visible)
        'tag_csam': '#cc0000',
        'tag_evidence': '#cc6600',
        'tag_interest': '#cccc00',
        'tag_custom': '#4a6a4a',
        
        # Keyword colors
        'keyword_hit': '#6a6a4a',
        'keyword_hit_alt': '#5a8a4a',
        
        # Sender colors
        'sender1': '#4a4c4a',
        'sender2': '#4a5a6a',
        
        # Default row colors
        'row_default': '#3c3c3c',
        'row_alternate': '#4a4a4a',  # More distinguishable from row_default
    }
    
    def __init__(self, dark_mode=False):
        self.dark_mode = dark_mode
        self.colors = self.DARK_THEME if dark_mode else self.LIGHT_THEME
    
    def get_color(self, key):
        """Get a color value by key."""
        return self.colors.get(key, '#000000')
    
    def get_stylesheet(self):
        """Get the main application stylesheet."""
        if self.dark_mode:
            return """
                QMainWindow { background-color: %s; color: %s; }
                QWidget { background-color: %s; color: %s; }
                QPushButton { 
                    background-color: %s; 
                    color: %s; 
                    padding: 5px; 
                    border: 1px solid %s;
                }
                QPushButton:hover { background-color: %s; }
                QComboBox { 
                    background-color: %s; 
                    color: %s; 
                    padding: 5px; 
                    border: 1px solid %s;
                }
                QLineEdit { 
                    background-color: %s; 
                    color: %s; 
                    padding: 5px; 
                    border: 1px solid %s;
                }
                QGroupBox { 
                    border: 1px solid %s; 
                    border-radius: 5px; 
                    padding: 10px; 
                    color: %s;
                }
                QTableWidget { 
                    background-color: %s; 
                    color: %s; 
                    gridline-color: %s;
                }
                QHeaderView::section { 
                    background-color: %s; 
                    color: %s; 
                    padding: 5px;
                }
                QTextEdit { 
                    background-color: %s; 
                    color: %s; 
                    border: 1px solid %s;
                }
                QLabel { color: %s; }
                QStatusBar { background-color: %s; color: %s; }
                QMenu { background-color: %s; color: %s; }
                QMenu::item:selected { background-color: %s; }
                QToolBar { background-color: %s; }
            """ % (
                self.get_color('bg_main'), self.get_color('text_primary'),
                self.get_color('bg_widget'), self.get_color('text_primary'),
                self.get_color('button_bg'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('button_hover'),
                self.get_color('bg_widget'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('bg_widget'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('border'), self.get_color('text_primary'),
                self.get_color('bg_table'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('bg_alternate'), self.get_color('text_primary'),
                self.get_color('bg_widget'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('text_primary'),
                self.get_color('bg_main'), self.get_color('text_primary'),
                self.get_color('bg_widget'), self.get_color('text_primary'),
                self.get_color('button_hover'),
                self.get_color('bg_main'),
            )
        else:
            return """
                QPushButton { padding: 5px; }
                QPushButton:hover { background-color: %s; }
                QComboBox { padding: 5px; }
                QLineEdit { padding: 5px; }
                QGroupBox { border: 1px solid %s; border-radius: 5px; padding: 10px; }
            """ % (
                self.get_color('button_hover'),
                self.get_color('border'),
            )
    
    def get_dialog_stylesheet(self):
        """Get stylesheet for dialogs."""
        if self.dark_mode:
            return """
                QDialog { background-color: %s; color: %s; }
                QTableWidget { font-size: 14px; background-color: %s; color: %s; }
                QLabel { font-size: 14px; color: %s; }
                QPushButton { 
                    padding: 8px; 
                    font-size: 14px; 
                    min-width: 100px; 
                    min-height: 30px; 
                    background-color: %s;
                    color: %s;
                    border: 1px solid %s;
                }
                QPushButton:hover { background-color: %s; }
                QTextEdit { 
                    padding: 8px; 
                    font-size: 14px; 
                    background-color: %s;
                    color: %s;
                    border: 1px solid %s;
                }
                QDialogButtonBox QPushButton { 
                    font-size: 14px; 
                    padding: 8px; 
                    min-width: 100px; 
                    min-height: 30px; 
                    background-color: %s;
                    color: %s;
                    border: 1px solid %s;
                }
                QDialogButtonBox QPushButton:hover { background-color: %s; }
                QComboBox { 
                    background-color: %s; 
                    color: %s; 
                    border: 1px solid %s;
                }
                QLineEdit { 
                    background-color: %s; 
                    color: %s; 
                    border: 1px solid %s;
                }
                QGroupBox { 
                    border: 1px solid %s; 
                    color: %s;
                }
                QCheckBox { color: %s; }
                QListWidget { 
                    background-color: %s; 
                    color: %s;
                }
            """ % (
                self.get_color('bg_dialog'), self.get_color('text_primary'),
                self.get_color('bg_table'), self.get_color('text_primary'),
                self.get_color('text_primary'),
                self.get_color('button_bg'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('button_hover'),
                self.get_color('bg_widget'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('button_bg'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('button_hover'),
                self.get_color('bg_widget'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('bg_widget'), self.get_color('text_primary'), self.get_color('border'),
                self.get_color('border'), self.get_color('text_primary'),
                self.get_color('text_primary'),
                self.get_color('bg_widget'), self.get_color('text_primary'),
            )
        else:
            return """
                QDialog { background-color: %s; }
                QTableWidget { font-size: 14px; }
                QLabel { font-size: 14px; }
                QPushButton { padding: 8px; font-size: 14px; min-width: 100px; min-height: 30px; }
                QPushButton:hover { background-color: %s; }
                QTextEdit { padding: 8px; font-size: 14px; }
                QDialogButtonBox QPushButton { 
                    font-size: 14px; 
                    padding: 8px; 
                    min-width: 100px; 
                    min-height: 30px; 
                }
                QDialogButtonBox QPushButton:hover { background-color: %s; }
            """ % (
                self.get_color('bg_dialog'),
                self.get_color('button_hover'),
                self.get_color('button_hover'),
            )



# =============================================================================
# --- CUSTOM WIDGETS & DIALOGS ---
# =============================================================================

class ClickableThumbnail(QLabel):
    def __init__(self, media_path, parent=None):
        super().__init__(parent)
        self.media_path = media_path
        self.local_blur = False

    
    def mousePressEvent(self, ev):
        if ev.button() == Qt.LeftButton and self.media_path:
            QDesktopServices.openUrl(QUrl.fromLocalFile(self.media_path))
            
    def contextMenuEvent(self, ev):
        menu = QMenu(self)
        toggle_action = menu.addAction("Unblur" if self.local_blur else "Blur")
        copy_action = menu.addAction("Copy File Name")
        action = menu.exec_(ev.globalPos())
        if action == toggle_action:
            self.local_blur = not self.local_blur
            effect = QGraphicsBlurEffect()
            effect.setBlurRadius(10)
            self.setGraphicsEffect(effect if self.local_blur else None)
        elif action == copy_action:
            if self.media_path:
                QApplication.clipboard().setText(os.path.basename(self.media_path))

class HelpDialog(QDialog):
    def __init__(self, tag_colors, parent=None):
        super().__init__(parent)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setWindowTitle("Help & Color Legend")
        self.resize(1200, 900)
        layout = QVBoxLayout(self)

        text_area = QTextEdit()
        text_area.setReadOnly(True)
        layout.addWidget(text_area)

        html_content = self.generate_help_content(tag_colors)
        text_area.setHtml(html_content)

        # --- Logging toggle checkbox (uses parent's logging_enabled) ---
        self.logging_checkbox = QCheckBox("Enable Logging")
        main = self.parent()
        if main is not None and hasattr(main, "logging_enabled"):
            self.logging_checkbox.setChecked(bool(main.logging_enabled))
        self.logging_checkbox.toggled.connect(self.on_logging_toggled)
        layout.addWidget(self.logging_checkbox)
        # ---------------------------------------------------------------

        btns = QDialogButtonBox(QDialogButtonBox.Ok)
        btns.accepted.connect(self.accept)
        layout.addWidget(btns)

        # Apply dark mode stylesheet if parent has dark mode enabled
        if hasattr(self.parent(), "theme_manager") and hasattr(self.parent(), "dark_mode") and self.parent().dark_mode:
            self.setStyleSheet(self.parent().theme_manager.get_dialog_stylesheet())

    def on_logging_toggled(self, checked):
        main = self.parent()
        if main is not None and hasattr(main, "toggle_logging_enabled"):
            main.toggle_logging_enabled(checked)


    def generate_help_content(self, tag_colors):
        html = "<h2>Snapchat Parser - How-To Guide</h2>"
        html += (
            "<p>"
            "Snapchat Parser is a tool for forensic review of conversation data from Snapchat responsive records. "
            "It loads one or more <b>conversations.csv</b> files from a ZIP (or nested ZIPs), builds a "
            "searchable and filterable message table, and provides tagging, review tracking, and export "
            "features. The application is designed to handle large datasets efficiently while keeping the "
            "interface responsive."
            "</p>"
        )

        html += "<h3>Core Features</h3>"
        html += "<ul>"

        # Import / loading
        html += (
            "<li><b>Import ZIP:</b> Use the <b>Open</b> button in the toolbar to load a Snapchat ZIP "
            "evidence file. The conversations and media indexes are built in a background thread to "
            "prevent the GUI from freezing. A progress window will indicate when data is being imported.</li>"
        )

        # Conversation selector
        html += (
            "<li><b>Conversation Selector:</b> The dropdown at the top allows you to view "
            "<b>All Conversations</b> or a single conversation at a time. "
            "Conversations that have been marked as reviewed are shown in <b style='color:red;'>red</b> "
            "and <b>bold</b> with a <code>(Reviewed)</code> marker.</li>"
        )

        # Filters
        html += (
            "<li><b>Advanced Filters:</b> Click the <b>Filters</b> button to filter by date range, sender, "
            "message type, content type, saved state, and more. The filter status is shown above the table, "
            "and you can clear filters to return to the full set of messages for the currently selected scope."
            "</li>"
        )

        # Tagging / hotkeys / context menu
        html += (
            "<li><b>Tagging (Right-Click / Hotkeys):</b> Right-click any message row to add or remove "
            "tags (such as <b>CSAM</b>, <b>Evidence</b>, or <b>Of Interest</b>) from one or more selected "
            "messages. Tag colors are applied to the entire row and are prioritized according to the "
            "Tag Color Legend below. Custom hotkeys can be configured in the <b>Manage Hotkeys</b> dialog."
            "</li>"
        )

        # Mark Reviewed
        html += (
            "<li><b>Mark Reviewed:</b> Use the <b>Mark As Reviewed</b> button on the toolbar to toggle the "
            "reviewed status of the <b>currently selected conversation</b>. When you mark a conversation as "
            "reviewed, the tool automatically advances to the next conversation in the selector. Reviewed "
            "conversations are highlighted in the selector using a red, bold <code>(Reviewed)</code> label."
            "</li>"
        )

        # Blur media
        html += (
            "<li><b>Blur Media:</b> Use the <b>Blur Media</b> toggle button in the toolbar to blur all media " 
            "thumbnails in the table. This can help with privacy or when you want to focus on context first " 
            "and only reveal media when needed. You can also right-click on individual thumbnails to blur or " 
            "unblur specific images without affecting others. The individual blur state persists even when " 
            "the global blur toggle is changed.</li>"
        )

        # Copy features
        html += (
            "<li><b>Copy Data:</b> Right-click the table to access copying options. "
            "<b>Copy Selected Rows</b> copies the selected rows (with headers) to the clipboard. "
            "<b>Copy Selected Cell</b> copies only the cell under the cursor (including additional tooltip "
            "information such as media filenames when applicable).</li>"
        )

        # Columns / Source / Line Number
        html += (
            "<li><b>Columns (including Source / Line Number):</b> The main table supports multiple columns "
            "for each message, including <b>Source</b> (folder/CSV that the message came from) and "
            "<b>Line Number</b> (the original 1-based line number in the source <code>conversations.csv</code>). "
            "These columns are also available in exports.</li>"
        )

        # Export
        html += (
            "<li><b>Export to HTML / CSV:</b> Use the <b>Export</b> button in the toolbar to export the " 
            "currently displayed messages to <b>HTML</b> or <b>CSV</b> format. You can choose which fields to " 
            "include (including <b>Source</b>, <b>Line #</b>, and <b>Notes</b>). HTML exports preserve " 
            "thumbnails (subject to blur settings), include interactive filtering and sorting capabilities, " 
            "and feature a built-in dark mode toggle. The exported HTML file includes all tags, notes, and " 
            "conversation metadata. CSV exports are suitable for further processing in spreadsheet applications " 
            "or data analysis tools.</li>"
        )

        # Logging
        html += (
            "<li><b>Logging:</b> Use the <b>Enable Logging</b> checkbox at the bottom of this Help window "
            "to toggle logging on or off. When enabled, the application writes diagnostic information to "
            "<code>SnapchatParser.log</code>, which can assist with troubleshooting.</li>"
        )

        
        # Dark Mode
        html += (
            "<li><b>Dark Mode:</b> Click the <b>Dark Mode</b> button in the toolbar (computer icon) to toggle " 
            "between light and dark themes. Dark mode applies to the main window, all tables, dialogs, and " 
            "pop-up windows, providing a more comfortable viewing experience in low-light environments. The " 
            "dark mode preference is saved and will be restored when you restart the application.</li>"
        )

        # Save/Load Progress
        html += (
            "<li><b>Save/Load Progress:</b> Use the <b>Save/Load Progress</b> button in the toolbar to save " 
            "your current review state to a JSON file. This includes all tagged messages, reviewed " 
            "conversations, and investigative notes. You can load this file later (after re-importing your ZIP file) to restore your progress, " 
            "allowing you to pause your review session and continue exactly where you left off. The progress " 
            "file is saved with a timestamp and unique identifier in the filename for easy identification.</li>"
        )

        # Notes Feature
        html += (
            "<li><b>Investigative Notes:</b> The <b>Notes</b> button in the toolbar provides a dropdown menu " 
            "with two options: <b>Add note to selected conversation</b> and <b>View notes</b>. Use " 
            "<b>Add note to selected conversation</b> to attach investigative notes to a specific conversation. " 
            "These notes are saved with your progress and will appear in HTML exports. Use <b>View notes</b> " 
            "to see all notes you've created, organized by conversation (displayed as user1,user2 format). " 
            "Notes are particularly useful for documenting findings, observations, or reminders about specific " 
            "conversations during your review process.</li>"
        )

        # Individual Thumbnail Blur
        html += (
            "<li><b>Individual Thumbnail Blur:</b> Right-click on any media thumbnail in the table to access " 
            "a context menu with options to <b>Blur</b> or <b>Unblur</b> that specific image, or <b>Copy</b> " 
            "the media filename to the clipboard. This allows you to selectively blur sensitive content while " 
            "keeping other media visible. Individual blur settings persist even when you toggle the global " 
            "blur button on or off.</li>"
        )

        # HTML Export Features
        html += (
            "<li><b>Enhanced HTML Export:</b> The HTML export includes several advanced features: " 
            "<b>Interactive filtering</b> by conversation, tag, date, and search terms; <b>sortable columns</b> " 
            "by clicking column headers; <b>dark mode toggle</b> for comfortable viewing; <b>investigative " 
            "notes</b> displayed both inline with messages and in a dedicated section; and <b>color-coded " 
            "tags</b> matching the application's tag legend. The exported HTML is fully self-contained and " 
            "can be shared with team members or used in reports without requiring the original application.</li>"
        )

        html += "</ul>"

        # Tag color legend
        html += "<h3>Tag Color Legend (Row Highlight)</h3>"
        html += (
            "<p>The row background color is determined first by the highest-priority tag applied to the "
            "message. If no priority tag is present, rows are alternated by sender to make participant "
            "changes visually clear.</p>"
        )

        html += (
            "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse:collapse; width:60%;'>"
            "<tr><th>Tag</th><th>Color</th><th>Priority</th></tr>"
        )

        priority_tags = ["CSAM", "Evidence", "Of Interest"]
        for i, tag in enumerate(priority_tags):
            color = tag_colors.get(tag, QColor(255, 255, 255))
            hex_color = color.lighter(130).name()
            html += (
                f"<tr>"
                f"<td><b>{tag}</b></td>"
                f"<td style='background-color:{hex_color};'>&nbsp;&nbsp;&nbsp;&nbsp;</td>"
                f"<td>{len(priority_tags) - i} (Highest for CSAM)</td>"
                f"</tr>"
            )

        html += "</table>"

        html += (
            "<p><i>Note:</i> When no priority tag is present, rows alternate between light gray and light "
            "blue based on sender. This alternation resets at the top of the current view and flips when "
            "the sender changes.</p>"
        )

        # Keyboard Shortcuts / Tips
        html += "<h3>Tips & Best Practices</h3>"
        html += "<ul>"
        html += (
            "<li><b>Keyboard Shortcuts:</b> Configure custom hotkeys for tags in the <b>Manage Hotkeys</b> " 
            "dialog. This allows you to quickly tag messages without using the mouse, significantly speeding " 
            "up your review workflow.</li>"
        )
        html += (
            "<li><b>Progress Management:</b> Regularly save your progress using the <b>Save/Load Progress</b> " 
            "feature, especially during long review sessions. The progress file includes all your tags, " 
            "reviewed conversations, and notes, ensuring you never lose work.</li>"
        )
        html += (
            "<li><b>Filtering Strategy:</b> Use the conversation selector and advanced filters together to " 
            "narrow down your view. For example, filter by date range and then select a specific conversation " 
            "to focus on a particular time period within that conversation.</li>"
        )
        html += (
            "<li><b>Export Organization:</b> When exporting, consider including the <b>Notes</b> field in " 
            "your HTML exports. This ensures all your investigative notes are preserved in the exported " 
            "document and can be shared with team members or included in reports.</li>"
        )
        html += (
            "<li><b>Media Review:</b> Use the global blur toggle to initially review conversations without " 
            "distraction from media content. Then, selectively unblur individual thumbnails as needed using " 
            "the right-click context menu when you need to examine specific media.</li>"
        )
        html += "</ul>"

        # About section
        html += "<h3>About</h3>"
        html += "<table cellpadding='3' cellspacing='0'>"
        html += "<tr><td><b>Program:</b></td><td>Snapchat Parser</td></tr>"
        html += "<tr><td><b>Version:</b></td><td>1.14 (Dark Mode Edition)</td></tr>"
        html += "<tr><td><b>Developer:</b></td><td>Patrick Koebbe</td></tr>"
        html += "</table>"

        return html


class FirstRunColumnWidthDialog(QDialog):
    """Dialog shown after data import to instruct users about column width customization."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Welcome - Column Width Customization")
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.resize(500, 320)
        
        layout = QVBoxLayout(self)
        
        # Title
        title_label = QLabel("<h2>Customize Your Column Widths</h2>")
        title_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(title_label)
        
        # Instructions
        instructions = QLabel(
            "<p>Welcome to Snapchat Parser!</p>"
            "<p><b>You can customize the column widths to your preference:</b></p>"
            "<ul>"
            "<li>Click and drag the column borders to resize columns</li>"
            "<li>Your column width preferences are <b>automatically saved</b></li>"
            "<li>Your settings will be remembered for future sessions</li>"
            "</ul>"
            "<p>Take a moment to adjust the columns to your preferred widths. "
            "These settings will persist across all future sessions.</p>"
        )
        instructions.setWordWrap(True)
        instructions.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        layout.addWidget(instructions)
        
        # Checkbox to prevent future displays
        self.dont_show_again_checkbox = QCheckBox("Don't show this message again")
        layout.addWidget(self.dont_show_again_checkbox)
        
        # Apply dark mode if parent has it enabled
        if parent and hasattr(parent, "theme_manager") and hasattr(parent, "dark_mode") and parent.dark_mode:
            self.setStyleSheet(parent.theme_manager.get_dialog_stylesheet())
        
        # OK button
        button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        button_box.accepted.connect(self.accept)
        layout.addWidget(button_box)
    
    def should_show_again(self):
        """Return True if the dialog should be shown again in the future."""
        return not self.dont_show_again_checkbox.isChecked()


class GroupMembersDialog(QDialog):
    """Dialog to display group member usernames and user IDs in a formatted list."""
    def __init__(self, usernames_str, user_ids_str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Group Members")
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.resize(600, 400)
        
        layout = QVBoxLayout(self)
        
        # Parse the data - could be comma-separated or other format
        usernames = self._parse_member_data(usernames_str) if usernames_str else []
        user_ids = self._parse_member_data(user_ids_str) if user_ids_str else []
        
        # Usernames section
        if usernames:
            username_label = QLabel(f"<b>Usernames ({len(usernames)}):</b>")
            layout.addWidget(username_label)
            
            username_text = QTextEdit()
            username_text.setReadOnly(True)
            username_text.setPlainText('\n'.join(usernames))
            username_text.setMaximumHeight(150)
            layout.addWidget(username_text)
        
        # User IDs section
        if user_ids:
            if usernames:
                layout.addSpacing(10)
            userid_label = QLabel(f"<b>User IDs ({len(user_ids)}):</b>")
            layout.addWidget(userid_label)
            
            userid_text = QTextEdit()
            userid_text.setReadOnly(True)
            userid_text.setPlainText('\n'.join(user_ids))
            userid_text.setMaximumHeight(150)
            layout.addWidget(userid_text)
        
        # If no data
        if not usernames and not user_ids:
            no_data_label = QLabel("No group member data available.")
            layout.addWidget(no_data_label)
        
        # Close button
        button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        button_box.accepted.connect(self.accept)

        # Apply dark mode stylesheet if parent has dark mode enabled
        if hasattr(self.parent(), "theme_manager") and hasattr(self.parent(), "dark_mode") and self.parent().dark_mode:
            self.setStyleSheet(self.parent().theme_manager.get_dialog_stylesheet())
        layout.addWidget(button_box)
    
    def _parse_member_data(self, data_str):
        """Parse member data string into a list of individual members."""
        if not data_str or not str(data_str).strip():
            return []
        
        data_str = str(data_str).strip()
        # Try splitting by common delimiters
        # First try comma, then semicolon, then newline
        if ',' in data_str:
            members = [m.strip() for m in data_str.split(',') if m.strip()]
        elif ';' in data_str:
            members = [m.strip() for m in data_str.split(';') if m.strip()]
        elif '\n' in data_str:
            members = [m.strip() for m in data_str.split('\n') if m.strip()]
        else:
            # Single value
            members = [data_str] if data_str else []
        
        return members


class MessageViewerDialog(QDialog):
    def __init__(self, messages, all_messages, basenames, media_extract_dir, thumb_dir, blur_all, parent=None, highlight_query=None, exact_match=False):
        super().__init__(parent)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.parent = parent
        self.setWindowTitle(" Tagged Messages")
        self.resize(1100, 700)
        
        self.all_messages = all_messages
        self.message_indices = messages
        self.basenames = basenames
        self.media_extract_dir = media_extract_dir
        self.thumb_dir = thumb_dir
        self.blur_all = blur_all
        
        self.highlight_query = (highlight_query or '').strip().lower()
        self.exact_match = exact_match
        
        layout = QVBoxLayout(self)
        
        # Define headers dynamically as a list (add your new ones here)
        self.headers = ["Conversation ID", "Conversation Title", "Message ID", "Reply To", "Content Type", "Message Type", "Date", "Time", "Sender", "Receiver", "Message", "Media ID", "Media", "Tags", "Saved By", "One-on-One?", "IP", "Port", "Reactions", "Screenshotted By", "Replayed By", "Screen Recorded By", "Read By", "Source", "Line Number", "Group Members"]  # Adjust as needed
        
        self.table = QTableWidget(0, len(self.headers))
        self.table.setHorizontalHeaderLabels(self.headers)
        
        # Set the delegate for HTML rendering
        self.table.setItemDelegate(HtmlDelegate())
        
        # Connect double-click to open group members dialog
        self.table.cellDoubleClicked.connect(self.on_table_cell_double_clicked)
        
        # Enable horizontal scrolling as needed
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        
        # Configure optimal table sizing
        configure_table_optimal_sizing(self.table, self.headers, "message_viewer_table", None)
        
        # Media column special handling
        media_col = self.headers.index("Media") if "Media" in self.headers else -1
        if media_col >= 0:
            self.table.horizontalHeader().setSectionResizeMode(media_col, QHeaderView.Fixed)
            self.table.setColumnWidth(media_col, 500)
        
        # Message column can stretch
        msg_col = self.headers.index("Message") if "Message" in self.headers else -1
        if msg_col >= 0:
            self.table.horizontalHeader().setSectionResizeMode(msg_col, QHeaderView.Stretch)
        
        header = self.table.horizontalHeader()
        header.setStretchLastSection(True)
        
        # Stretch specific columns (adjust indices based on your headers)
        msg_col = self.headers.index("Message") if "Message" in self.headers else -1
        if msg_col >= 0:
            header.setSectionResizeMode(msg_col, QHeaderView.Stretch)
        
        media_col = self.headers.index("Media") if "Media" in self.headers else -1
        if media_col >= 0:
            header.setSectionResizeMode(media_col, QHeaderView.Fixed)
            self.table.setColumnWidth(media_col, 500)  # Fixed width for media
        
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.ctx_menu)
        layout.addWidget(self.table)
        
        btns_layout = QHBoxLayout()
        btns_layout.addStretch()
        copy_btn = QPushButton("Copy Selected")
        copy_btn.clicked.connect(self.copy_selected)
        btns_layout.addWidget(copy_btn)
        layout.addLayout(btns_layout)
        

        # Apply dark mode stylesheet if parent has dark mode enabled
        if self.parent and hasattr(self.parent, "theme_manager") and hasattr(self.parent, "dark_mode") and self.parent.dark_mode:
            self.setStyleSheet(self.parent.theme_manager.get_dialog_stylesheet())
        self.populate(self.message_indices)

    def get_msg_at_row(self, row):
        if row < len(self.message_indices):
            return self.all_messages[self.message_indices[row]]
        return None

    def populate(self, message_indices):
            self.table.setRowCount(0)
            for msg_index in message_indices:
                msg = self.all_messages[msg_index]
                r = self.table.rowCount()
                self.table.insertRow(r)
                
                row_color = None
                for tag in ["CSAM", "Evidence", "Of Interest"]:
                    if tag in msg.get('tags', set()):
                        row_color = self.parent.TAG_COLORS.get(tag)
                        break 
                
                # If a tag color was set, lighten it (as per your current code)
                if row_color:
                    row_color = row_color.lighter(130)
                
                # If no priority tag color, alternate by sender with more contrasting colors
                if row_color is None:
                    sender = str(msg.get('sender_username') or msg.get('sender') or '').strip()
                    # Keep stateful last_sender in a temporary variable on the function
                    if r == 0:
                        # first row: initialize
                        self._last_sender_for_alternation = sender
                        # choose base color A (light blue for first sender)
                        alt_toggle = False
                    else:
                        prev_sender = getattr(self, '_last_sender_for_alternation', None)
                        if sender == prev_sender:
                            # same sender -> keep previous alt_toggle
                            alt_toggle = getattr(self, '_last_alt_toggle', False)
                        else:
                            # sender changed -> flip
                            alt_toggle = not getattr(self, '_last_alt_toggle', False)
                            self._last_sender_for_alternation = sender

                    self._last_alt_toggle = alt_toggle
                    if alt_toggle:
                        row_color = QColor(211, 211, 211)  # light gray for one participant
                    else:
                        row_color = QColor(173, 216, 230)  # light blue for the other participant

                brush = QBrush(row_color) if row_color else None

                def create_item(text, bg_brush, enable_word_wrap=True):
                    item = QTableWidgetItem()
                    text = str(text)
                    # Convert user IDs to usernames if mapping exists
                    user_id_tooltip = ''
                    if self.parent and hasattr(self.parent, 'user_id_to_username_map') and self.parent.user_id_to_username_map:
                        converted_text, user_id_tooltip = convert_user_ids_to_usernames(
                            text, self.parent.user_id_to_username_map, return_tooltip=True
                        )
                        text = converted_text
                    
                    if self.highlight_query:
                        pattern = re.escape(self.highlight_query)
                        if self.exact_match:
                            pattern = r'\b' + pattern + r'\b'
                        highlighted = re.sub(pattern, lambda m: f'<span style="background-color: yellow;">{m.group(0)}</span>', text, flags=re.IGNORECASE)
                        item.setText(highlighted)
                    else:
                        item.setText(text)
                    if user_id_tooltip:
                        item.setToolTip(f"User IDs: {user_id_tooltip}")
                    item.setData(Qt.TextWordWrap, enable_word_wrap)  # Control word wrapping per column
                    if bg_brush:
                        item.setBackground(bg_brush)
                    return item

                ts = msg.get('timestamp')
                date_s = ts.strftime("%Y-%m-%d") if ts else 'N/A'
                time_s = ts.strftime("%H:%M:%S") if ts else 'N/A'
                
                # Get user_id_to_username_map from parent
                user_id_map = None
                if self.parent and hasattr(self.parent, 'user_id_to_username_map') and self.parent.user_id_to_username_map:
                    user_id_map = self.parent.user_id_to_username_map
                
                # Map headers to data extraction (add logic for your new headers here)
                # For reported files, show blank for Conversation ID and Title
                conv_id = str(msg.get('conversation_id', ''))
                is_reported = conv_id == '__REPORTED_FILES__' or msg.get('is_flagged_media', False)
                
                col_data = {
					"Conversation ID": '' if is_reported else conv_id,
                    "Conversation Title": '' if is_reported else str(msg.get('conversation_title', '')),
                    "Message ID": str(msg.get('message_id', '')),
                    "Reply To": str(msg.get('reply_to_message_id', '')),
                    "Content Type": str(msg.get('content_type', '')),
                    "Message Type": str(msg.get('message_type', '')),
                    "Date": date_s,
                    "Time": time_s,
                    "Sender": str(msg.get('sender_username') or msg.get('sender') or ''),
                    "Receiver": str(msg.get('recipient_username') or msg.get('receiver') or ''),
                    "Message": str(msg.get('text') or msg.get('message') or ''),
                    "Media ID": str(msg.get('media_id') or msg.get('content_id') or ''),
                    "Tags": ', '.join(sorted(msg.get('tags', set()))),
                    "Saved By": str(msg.get('saved_by', '')),
                    "One-on-One?": str(msg.get('is_one_on_one', '')),
                    "IP": str(msg.get('upload_ip', '')),
                    "Port": str(msg.get('source_port_number', '')),
                    "Reactions": parse_reactions(msg.get('reactions', ''), user_id_map),
                    "Screenshotted By": str(msg.get('screenshotted_by', '')),
                    "Replayed By": str(msg.get('replayed_by', '')),
                    "Screen Recorded By": str(msg.get('screen_recorded_by', '')),
                    "Read By": str(msg.get('read_by', '')),
                    "Source": str(msg.get('source', '')),
                    "Line Number": str(msg.get('source_line', '')),
                }
                
                # Store full data for user ID columns (for dialog access)
                user_id_columns_data = {}
                for col_name in ["Saved By", "Screenshotted By", "Replayed By", "Read By"]:
                    user_ids_str = col_data.get(col_name, '')
                    display_text, full_data = parse_user_ids_to_usernames(
                        user_ids_str, 
                        user_id_map,
                        max_display=2
                    )
                    col_data[col_name] = display_text
                    user_id_columns_data[col_name] = full_data
                
                # Combine group member usernames and user IDs into single column
                group_usernames = str(msg.get('group_member_usernames', '')).strip()
                group_user_ids = str(msg.get('group_member_user_ids', '')).strip()
                group_members_combined = ''
                if group_usernames and group_user_ids:
                    group_members_combined = f"Usernames: {group_usernames}\nUser IDs: {group_user_ids}"
                elif group_usernames:
                    group_members_combined = f"Usernames: {group_usernames}"
                elif group_user_ids:
                    group_members_combined = f"User IDs: {group_user_ids}"
                col_data["Group Members"] = group_members_combined
                
                # Set items dynamically based on header positions
                for col, header in enumerate(self.headers):
                    value = col_data.get(header, '')  # Default to empty if no mapping
                    # Special handling for Group Members column
                    if header == "Group Members":
                        display_text, member_count, full_data = format_group_member_display(value)
                        if member_count > 1:
                            # Make "Click to View (N members)" blue and underlined using HTML
                            html_text = f'<span style="color: blue; text-decoration: underline; cursor: pointer;">{display_text}</span>'
                            item = QTableWidgetItem()
                            item.setTextAlignment(Qt.AlignLeft | Qt.AlignTop)
                            item.setFlags(item.flags() | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                            item.setData(Qt.TextWordWrap, True)
                            item.setText(html_text)  # Set HTML text for blue underlined link
                            item.setToolTip(full_data if full_data else '')
                            item.setData(Qt.UserRole, full_data)  # Store full data for dialog access
                            if brush:
                                item.setBackground(brush)
                            self.table.setItem(r, col, item)
                        else:
                            # Single member or empty, display as plain text
                            item = create_item(display_text, brush)
                            item.setData(Qt.UserRole, full_data)
                            self.table.setItem(r, col, item)
                    elif header in ["Saved By", "Screenshotted By", "Replayed By", "Read By"]:
                        # Handle user ID columns with username conversion
                        full_data = user_id_columns_data.get(header, {'usernames': [], 'user_ids': []})
                        usernames = full_data.get('usernames', [])
                        user_ids = full_data.get('user_ids', [])
                        
                        if len(usernames) > 2 or len(user_ids) > 2:
                            # More than 2 users - show link
                            html_text = '<a href="view_users">click to view</a>'
                            item = QTableWidgetItem()
                            item.setTextAlignment(Qt.AlignLeft | Qt.AlignTop)
                            item.setFlags(item.flags() | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                            item.setData(Qt.TextWordWrap, True)
                            item.setText(html_text)
                            item.setData(Qt.UserRole, full_data)  # Store full data for dialog
                            if brush:
                                item.setBackground(brush)
                            self.table.setItem(r, col, item)
                        else:
                            # 2 or fewer users - show usernames directly
                            item = create_item(value, brush, enable_word_wrap=True)
                            item.setData(Qt.UserRole, full_data)  # Store full data for dialog
                            self.table.setItem(r, col, item)
                    else:
                        # Allow word wrap for all columns (user can resize and it will wrap)
                        # Only disable for Message ID, Media ID, Reply To (shorter IDs that shouldn't wrap)
                        id_columns_no_wrap = ["Message ID", "Media ID", "Reply To"]
                        enable_wrap = header not in id_columns_no_wrap
                        self.table.setItem(r, col, create_item(value, brush, enable_word_wrap=enable_wrap))
                
                # Handle Media column separately (assuming it's still present)
                media_col = self.headers.index("Media") if "Media" in self.headers else -1
                if media_col >= 0:
                    media_id = str(msg.get('media_id') or msg.get('content_id') or '')
                    if media_id:
                        # OPTIMIZED: Build token_index on demand if needed (use parent's token_index)
                        token_index = None
                        media_lookup_cache = None
                        if self.parent:
                            # Use parent's token_index and cache
                            if not hasattr(self.parent, '_token_index_built') or not self.parent._token_index_built:
                                if hasattr(self.parent, '_ensure_token_index'):
                                    self.parent._ensure_token_index()
                            if hasattr(self.parent, 'token_index'):
                                token_index = self.parent.token_index
                            if hasattr(self.parent, 'media_lookup_cache'):
                                media_lookup_cache = self.parent.media_lookup_cache
                        entries = find_media_by_media_id(media_id, self.basenames,
                                                         token_index,
                                                         media_lookup_cache)
                        # Only process the first entry to avoid duplicate thumbnails for the same media_id
                        if entries:
                            zpath, internal = entries[0]
                            extracted = extract_file_from_zip(zpath, internal, self.media_extract_dir)
                            if extracted and os.path.exists(extracted):
                                thumb = generate_thumbnail(extracted, self.thumb_dir)
                                if thumb and os.path.exists(thumb):
                                    widget = ClickableThumbnail(extracted)
                                    if brush:
                                        palette = widget.palette()
                                        palette.setBrush(QPalette.Window, brush)
                                        widget.setPalette(palette)
                                    widget.setPixmap(QPixmap(thumb).scaled(THUMBNAIL_SIZE[0], THUMBNAIL_SIZE[1], Qt.KeepAspectRatio, Qt.SmoothTransformation))
                                    if self.parent.blur_all:
                                        eff = QGraphicsBlurEffect()
                                        eff.setBlurRadius(10)
                                        widget.setGraphicsEffect(eff)
                                    self.table.setCellWidget(r, media_col, widget)
                                else:
                                    self.table.setItem(r, media_col, create_item(os.path.basename(extracted), brush))
                            else:
                                self.table.setItem(r, media_col, create_item(media_id, brush))
                    else:
                        self.table.setItem(r, media_col, create_item("", brush))
                
            self.table.resizeRowsToContents()  # Column widths handled by configure_table_optimal_sizing
            
    def update_row_color(self, row):
        msg = self.get_msg_at_row(row)
        if not msg:
            return

        # priority tag?
        row_color = None
        for tag in ["CSAM", "Evidence", "Of Interest"]:
            if tag in msg.get('tags', set()):
                row_color = self.parent.TAG_COLORS.get(tag)
                break
        if row_color:
            row_color = row_color.lighter(130)
        else:
            # sender-change alternation (match populate())
            sender = str(msg.get('sender_username') or msg.get('sender') or '').strip()
            # Walk up one row to determine alternation flip on sender change
            if row == 0:
                alt_toggle = False
            else:
                prev = self.get_msg_at_row(row - 1)
                prev_sender = str(prev.get('sender_username') or prev.get('sender') or '').strip() if prev else ''
                # If sender changed from previous row, flip; else keep
                # We need the prior row's computed toggle; recompute briefly:
                # Determine prior toggle by looking two rows back (fallback to False at top)
                if row - 1 == 0:
                    prev_alt = False
                else:
                    prev2 = self.get_msg_at_row(row - 2)
                    prev2_sender = str(prev2.get('sender_username') or prev2.get('sender') or '').strip() if prev2 else ''
                    prev_alt = (prev_sender != prev2_sender)
                alt_toggle = (not prev_alt) if (sender != prev_sender) else prev_alt

            row_color = QColor(211, 211, 211) if alt_toggle else QColor(173, 216, 230)

        brush = QBrush(row_color)
        for col in range(self.table.columnCount()):
            item = self.table.item(row, col)
            if item:
                item.setBackground(brush)
            widget = self.table.cellWidget(row, col)
            if widget:
                palette = widget.palette()
                palette.setBrush(QPalette.Window, brush)
                widget.setPalette(palette)
        self.table.resizeRowToContents(row)


    def ctx_menu(self, pos):
        item = self.table.itemAt(pos)
        if not item:
            return
        row = item.row()
        menu = QMenu(self)
        add_menu = menu.addMenu("Add Tag")
        for t in sorted(self.parent.available_tags):
            action = add_menu.addAction(t)
            action.triggered.connect(lambda checked, r=row, tag=t: self.add_tag(r, tag))
        
        remove_action = menu.addAction("Remove Tags")
        mark_action = menu.addAction("Mark Conv Reviewed")
        
        action = menu.exec_(self.table.viewport().mapToGlobal(pos))
        
        if action == remove_action:
            self.remove_tags(row)
        elif action == mark_action:
            msg = self.get_msg_at_row(row)
            if msg:
                self.parent.toggle_reviewed(msg.get('conversation_id'))
                self.parent.populate_selector()
                return

    def add_tag(self, row, tag):
        msg = self.get_msg_at_row(row)
        if msg:
            tags = set(msg.get('tags', set()))
            tags.add(tag)
            msg['tags'] = tags
            tags_col = self.headers.index("Tags") if "Tags" in self.headers else -1
            if tags_col >= 0:
                self.table.item(row, tags_col).setText(', '.join(sorted(tags)))
            self.parent.available_tags.add(tag)
            self.parent.save_config()
            # Update row color in-place without full repopulate
            self.update_row_color(row)

    def on_table_cell_double_clicked(self, row, col):
        """Handle double-click on table cells, especially group member columns."""
        header = self.headers[col] if col < len(self.headers) else None
        
        if header == "Group Members":
            item = self.table.item(row, col)
            if item:
                full_data = item.data(Qt.UserRole)
                if full_data:
                    # Parse the combined data to extract usernames and user IDs
                    usernames = ''
                    user_ids = ''
                    if 'Usernames:' in full_data and 'User IDs:' in full_data:
                        parts = full_data.split('User IDs:')
                        usernames = parts[0].replace('Usernames:', '').strip()
                        user_ids = parts[1].strip() if len(parts) > 1 else ''
                    elif 'Usernames:' in full_data:
                        usernames = full_data.replace('Usernames:', '').strip()
                    elif 'User IDs:' in full_data:
                        user_ids = full_data.replace('User IDs:', '').strip()
                    else:
                        # Fallback: try to parse as original format
                        usernames = full_data
                    
                    dlg = GroupMembersDialog(usernames, user_ids, self)
                    dlg.exec_()
                    
                    # After dialog closes, clear selection to force proper HTML rendering
                    current_selection = self.table.selectionModel().selection()
                    self.table.clearSelection()
                    QApplication.processEvents()
                    if current_selection:
                        self.table.selectionModel().select(current_selection, QItemSelectionModel.Select)
                    self.table.viewport().update()
        
        elif header in ["Saved By", "Screenshotted By", "Replayed By", "Read By"]:
            item = self.table.item(row, col)
            if item:
                full_data = item.data(Qt.UserRole)
                if full_data and isinstance(full_data, dict):
                    usernames = full_data.get('usernames', [])
                    user_ids = full_data.get('user_ids', [])
                    
                    # Only open dialog if there are users to display
                    if usernames or user_ids:
                        # Format usernames and user_ids as comma-separated strings
                        usernames_str = ', '.join(usernames) if usernames else ''
                        user_ids_str = ', '.join(user_ids) if user_ids else ''
                        
                        dlg = GroupMembersDialog(usernames_str, user_ids_str, self)
                        dlg.setWindowTitle(header)  # Set dialog title to column name
                    dlg.exec_()
                    
                    # After dialog closes, clear selection to force proper HTML rendering
                    current_selection = self.table.selectionModel().selection()
                    self.table.clearSelection()
                    QApplication.processEvents()
                    if current_selection:
                        self.table.selectionModel().select(current_selection, QItemSelectionModel.Select)
                    self.table.viewport().update()

    def remove_tags(self, row):
        msg = self.get_msg_at_row(row)
        if msg:
            msg['tags'] = set()
            tags_col = self.headers.index("Tags") if "Tags" in self.headers else -1
            if tags_col >= 0:
                self.table.item(row, tags_col).setText('')
            self.parent.save_config()
            # Update row color in-place without full repopulate
            self.update_row_color(row)

    def copy_selected(self):
        selected_ranges = self.table.selectedRanges()
        if not selected_ranges:
            QMessageBox.information(self, "Info", "No selection")
            return

        # We'll collect the unique rows in order
        rows = set()
        for r in range(self.table.rowCount()):
            for c in range(self.table.columnCount()):
                if self.table.item(r, c) and self.table.item(r, c).isSelected():
                    rows.add(r)
                    break
        if not rows:
            QMessageBox.information(self, "Info", "No rows selected")
            return

        rows = sorted(rows)
        headers = [self.table.horizontalHeaderItem(c).text() if self.table.horizontalHeaderItem(c) else '' for c in range(self.table.columnCount())]
        lines = ["\t".join(headers)]
        for r in rows:
            row_values = []
            for c in range(self.table.columnCount()):
                it = self.table.item(r, c)
                row_values.append(it.text() if it else '')
            lines.append("\t".join(row_values))

        QApplication.clipboard().setText("\n".join(lines))
        QMessageBox.information(self, "Copied", f"Copied {len(rows)} row(s) to clipboard")

class ExportOptionsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setWindowTitle("Export Options")
        # Get primary screen geometry and set dialog to ~50% width and ~60% height (adjust percentages as needed)
        screen = QGuiApplication.primaryScreen()
        screen_geometry = screen.availableGeometry()
        width = int(screen_geometry.width() * 0.5)  # 50% of screen width
        height = int(screen_geometry.height() * 0.6)  # 60% of screen height   
        width = max(500, width)  # Min 500px wide
        height = max(400, height)  # Min 400px tall
        self.resize(width, height)
        
        # Main layout
        main_layout = QVBoxLayout(self)
        
        # Scroll area
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll_widget = QWidget()
        scroll.setWidget(scroll_widget)
        layout = QVBoxLayout(scroll_widget)
        
        scope_group = QGroupBox("Scope")
        scope_layout = QVBoxLayout()
        self.cb_tagged = QCheckBox("Tagged Messages")
        self.cb_sel = QCheckBox("Selected Conversation")
        self.cb_all = QCheckBox("All Conversations")
        scope_layout.addWidget(self.cb_tagged)
        scope_layout.addWidget(self.cb_sel)
        scope_layout.addWidget(self.cb_all)
        scope_group.setLayout(scope_layout)
        layout.addWidget(scope_group)
        
        sanitize_group = QGroupBox("Sanitize")
        sanitize_layout = QVBoxLayout()
        self.cb_blur_csam = QCheckBox("Blur CSAM-tagged")
        self.cb_blur_all = QCheckBox("Blur All")
        sanitize_layout.addWidget(self.cb_blur_csam)
        sanitize_layout.addWidget(self.cb_blur_all)
        sanitize_group.setLayout(sanitize_layout)
        layout.addWidget(sanitize_group)
        
        format_layout = QHBoxLayout()
        format_layout.addWidget(QLabel("Format:"))
        self.fmt_combo = QComboBox()
        self.fmt_combo.addItems(["HTML", "CSV"])
        format_layout.addWidget(self.fmt_combo)
        layout.addLayout(format_layout)
        
        sort_layout = QHBoxLayout()
        sort_layout.addWidget(QLabel("Sort By:"))
        self.sort_combo = QComboBox()
        self.sort_combo.addItems(["User/Conversation (Default)", "Timestamp"])
        sort_layout.addWidget(self.sort_combo)
        layout.addLayout(sort_layout)
        
        fields_group = QGroupBox("Fields to Include")
        fields_layout = QVBoxLayout()
        self.cb_select_all = QCheckBox("Select All")
        self.cb_select_all.setChecked(True)
        self.cb_select_all.stateChanged.connect(self.toggle_all_fields)
        fields_layout.addWidget(self.cb_select_all)
        self.field_checkboxes = {  # Map to your headers
            'Conversation': QCheckBox("Conversation ID"),
            'Conversation Title': QCheckBox("Conversation Title"),
            'Message ID': QCheckBox("Message ID"),
            'Reply To': QCheckBox("Reply To"),
            'Content Type': QCheckBox("Content Type"),
            'Message Type': QCheckBox("Message Type"),
            'Date': QCheckBox("Date"),
            'Time': QCheckBox("Time"),
            'Sender': QCheckBox("Sender"),
            'Receiver': QCheckBox("Receiver"),
            'Message': QCheckBox("Message"),
            'Media ID': QCheckBox("Media ID"),
            'Media': QCheckBox("Media"),
            'Tags': QCheckBox("Tags"),
            'Saved By': QCheckBox("Saved By"),
            'One-on-One?': QCheckBox("One-on-One?"),
            'IP': QCheckBox("IP"),
            'Port': QCheckBox("Port"),
            'Reactions': QCheckBox("Reactions"),
            'Screenshotted By': QCheckBox("Screenshotted By"),
            'Replayed By': QCheckBox("Replayed By"),
            'Screen Recorded By': QCheckBox("Screen Recorded By"),
            'Read By': QCheckBox("Read By"),
            'Source': QCheckBox("Source"),
            'Line #': QCheckBox("Line #"),
            'Group Members': QCheckBox("Group Members"),
            'Notes': QCheckBox("Notes"),
        }
        for cb in self.field_checkboxes.values():
            cb.setChecked(True)
            fields_layout.addWidget(cb)
        fields_group.setLayout(fields_layout)
        layout.addWidget(fields_group)
        
        main_layout.addWidget(scroll)
        
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        # Apply dark mode stylesheet if parent has dark mode enabled
        if hasattr(self.parent(), "theme_manager") and hasattr(self.parent(), "dark_mode") and self.parent().dark_mode:
            self.setStyleSheet(self.parent().theme_manager.get_dialog_stylesheet())
        main_layout.addWidget(btns)  # Buttons outside scroll

    def toggle_all_fields(self, state):
        for cb in self.field_checkboxes.values():
            cb.setChecked(state == Qt.Checked)

    def get_options(self):
        return {
            'scope_tagged': self.cb_tagged.isChecked(),
            'scope_selected': self.cb_sel.isChecked(),
            'scope_all': self.cb_all.isChecked(),
            'blur_csam': self.cb_blur_csam.isChecked(),
            'blur_all': self.cb_blur_all.isChecked(),
            'format': self.fmt_combo.currentText(),
            'sort_by': self.sort_combo.currentText(),
            'fields': [k for k, v in self.field_checkboxes.items() if v.isChecked()]
        }

class ManageHotkeysDialog(QDialog):
    def __init__(self, tags, hotkeys, tag_colors, parent=None):
        super().__init__(parent)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setWindowTitle("Manage Hotkeys & Tags")
        self.resize(1200, 900)
        layout = QVBoxLayout(self)
        self.tag_colors = tag_colors # Store the map of protected tags
        self.protected_tags = {"CSAM", "Evidence", "Of Interest"}


        # Use a table to manage tag labels and hotkeys
        self.table = QTableWidget(0, 2)
        self.table.setHorizontalHeaderLabels(["Tag Label", "Hotkey"])
        # Configure optimal table sizing (this will set Interactive mode and apply default widths)
        configure_table_optimal_sizing(self.table, ["Tag Label", "Hotkey"], "hotkeys_table", None)
        # Override with increased widths
        # Tag Label: 300 * 1.2 = 360, Hotkey: 180 * 1.2 * 1.15 = 248 (20% + 15% more)
        self.table.setColumnWidth(0, 360)  # Tag Label column
        self.table.setColumnWidth(1, 248)  # Hotkey column (increased by additional 15%)
        layout.addWidget(self.table)
        
        # Action buttons
        btn_layout = QHBoxLayout()
        self.add_btn = QPushButton("Add New Tag")
        self.add_btn.clicked.connect(self.add_tag_row)
        self.remove_btn = QPushButton("Remove Selected Tag")
        self.remove_btn.clicked.connect(self.remove_tag_row)
        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.remove_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        # Dialog buttons
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.populate_table(tags, hotkeys)

        # Apply dark mode stylesheet if parent has dark mode enabled
        if hasattr(self.parent(), "theme_manager") and hasattr(self.parent(), "dark_mode") and self.parent().dark_mode:
            self.setStyleSheet(self.parent().theme_manager.get_dialog_stylesheet())
        
    def populate_table(self, tags, hotkeys):
        self.table.setRowCount(0)
        
        # Prepare list of all tags, prioritizing the ones with colors
        all_tags = sorted(list(set(tags) | set(hotkeys.keys())))
        
        # Ensure default tags are listed first
        priority_tags = sorted(list(self.protected_tags))
        # Ignore any stray 'Reviewed' tag if it exists in config
        other_tags = sorted([t for t in all_tags if t not in self.protected_tags and t != "Reviewed"])

        for tag in priority_tags + other_tags:
            self._add_row(tag, hotkeys.get(tag, ""))


    def _add_row(self, tag_label, hotkey_sequence="", is_special=False):
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        # Column 0: Tag Label (QLineEdit item)
        tag_item = QTableWidgetItem(tag_label)
        if tag_label in self.protected_tags or is_special:
            # Make protected tags non-editable
            tag_item.setFlags(tag_item.flags() & ~Qt.ItemIsEditable & ~Qt.ItemIsSelectable)
            tag_item.setToolTip("Default tags (CSAM, Evidence, Of Interest) cannot be edited.")

        self.table.setItem(row, 0, tag_item)
        
        # Column 1: Hotkey (QKeySequenceEdit widget)
        key_editor = QKeySequenceEdit(QKeySequence(hotkey_sequence))
        self.table.setCellWidget(row, 1, key_editor)

    def add_tag_row(self):
        # Use QInputDialog to get the new tag name first
        new_tag_name, ok = QInputDialog.getText(self, "New Tag Label", "Enter the label for the new tag:")
        if ok and new_tag_name.strip():
            # Check for duplicates
            new_tag_name = new_tag_name.strip()
            for r in range(self.table.rowCount()):
                item = self.table.item(r, 0)
                if item and item.text().lower() == new_tag_name.lower():
                    QMessageBox.warning(self, "Error", f"A tag with the label '{new_tag_name}' already exists.")
                    return
            
            self._add_row(new_tag_name)
            self.table.setCurrentCell(self.table.rowCount() - 1, 0) # Select the new row
        
    def remove_tag_row(self):
        current_row = self.table.currentRow()
        if current_row < 0:
            QMessageBox.information(self, "Remove Tag", "Please select a row to remove.")
            return

        tag_item = self.table.item(current_row, 0)
        tag_label = tag_item.text()
        
        # Prevent removing protected tags
        if tag_label in self.protected_tags:
            QMessageBox.warning(self, "Protected Tag", f"The default tag '{tag_label}' cannot be removed or renamed. It can only be hidden by removing its hotkey.")
            return

        if QMessageBox.question(self, "Confirm Removal", 
                                f"Are you sure you want to remove the custom tag '{tag_label}' and its associated hotkey?", 
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            self.table.removeRow(current_row)

    def get_hotkeys_and_tags(self):
        hotkeys = {}
        tags = set()
        
        for r in range(self.table.rowCount()):
            tag_item = self.table.item(r, 0)
            tag_label = tag_item.text().strip()
            
            if not tag_label: continue
            
            key_editor = self.table.cellWidget(r, 1)
            
            if key_editor:
                key_seq_str = key_editor.keySequence().toString()
                
                # Hotkeys map includes only those with a sequence
                if key_seq_str:
                    hotkeys[tag_label] = key_seq_str

                # All labels become available tags
                tags.add(tag_label)
                    
        return tags, hotkeys


class ManageKeywordListsDialog(QDialog):
    def __init__(self, keyword_lists, parent=None):
        super().__init__(parent)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setWindowTitle("Manage Keyword Lists")
        self.resize(1200, 900)
        self.current_lists = {k: list(v) for k, v in keyword_lists.items()} # Deep copy
        self.current_list_name = None

        main_v_layout = QVBoxLayout(self)
        main_h_layout = QHBoxLayout()

        # Left Panel: List Selector and Actions (Create/Delete/Rename)
        left_panel = QVBoxLayout()
        left_panel.addWidget(QLabel("<b>Available Keyword Lists</b>"))
        
        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QAbstractItemView.SingleSelection)
        self.list_widget.currentRowChanged.connect(self.load_list_details)
        left_panel.addWidget(self.list_widget)

        list_actions_layout = QHBoxLayout()
        self.new_btn = QPushButton("New List")
        self.new_btn.clicked.connect(self.new_list)
        self.rename_btn = QPushButton("Rename")
        self.rename_btn.clicked.connect(self.rename_list)
        self.delete_btn = QPushButton("Delete")
        self.delete_btn.clicked.connect(self.delete_list)
        
        list_actions_layout.addWidget(self.new_btn)
        list_actions_layout.addWidget(self.rename_btn)
        list_actions_layout.addWidget(self.delete_btn)
        left_panel.addLayout(list_actions_layout)
        main_h_layout.addLayout(left_panel, 1)

        # Right Panel: List Content Editor
        self.editor_group = QGroupBox("List Details")
        editor_layout = QVBoxLayout(self.editor_group)
        
        self.name_label = QLabel("None Selected")
        self.name_label.setFont(QFont("Arial", 12, QFont.Bold))
        editor_layout.addWidget(QLabel("<b>List Name:</b>"))
        editor_layout.addWidget(self.name_label)
        
        editor_layout.addWidget(QLabel("One keyword per line (paste lists here):"))
        self.text_edit = QTextEdit()
        self.save_timer = QTimer(self)
        self.save_timer.setSingleShot(True)
        self.save_timer.timeout.connect(lambda: self.save_list_content(force_save=True))
        self.text_edit.textChanged.connect(lambda: self.save_timer.start(500)) 
        editor_layout.addWidget(self.text_edit)
        
        self.cb_whole = QCheckBox("Exact word (Applies to all keywords in this list)")
        self.cb_whole.stateChanged.connect(lambda: self.save_list_content(force_save=True))
        editor_layout.addWidget(self.cb_whole)
        
        main_h_layout.addWidget(self.editor_group, 2)
        main_v_layout.addLayout(main_h_layout)

        # Dialog Buttons
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        main_v_layout.addWidget(btns)
        
        self.populate_list()
        self.set_editor_enabled(False)

        # Apply dark mode stylesheet if parent has dark mode enabled
        if hasattr(self.parent(), "theme_manager") and hasattr(self.parent(), "dark_mode") and self.parent().dark_mode:
            self.setStyleSheet(self.parent().theme_manager.get_dialog_stylesheet())
        QTimer.singleShot(0, self.select_first_list)  # NEW: Defer initial selection

    def select_first_list(self):
        if self.list_widget.count() > 0 and self.list_widget.currentRow() == -1:
            self.list_widget.setCurrentRow(0)
            self.load_list_details(0)

    def populate_list(self):
        self.list_widget.clear()
        for name in sorted(self.current_lists.keys()):
            self.list_widget.addItem(name)
        
        if self.current_lists:
            target_name = self.current_list_name if self.current_list_name in self.current_lists else sorted(self.current_lists.keys())[0]
            items = self.list_widget.findItems(target_name, Qt.MatchExactly)
            if items:
                self.list_widget.setCurrentItem(items[0])
                QTimer.singleShot(0, lambda: self.load_list_details(self.list_widget.currentRow()))  # NEW: Defer load
        else:
            self.set_editor_enabled(False)
            
    def set_editor_enabled(self, enabled):
        self.editor_group.setEnabled(enabled)
        self.rename_btn.setEnabled(enabled)
        self.delete_btn.setEnabled(enabled)
        if not enabled:
            self.name_label.setText("None Selected")
            self.text_edit.blockSignals(True)
            self.cb_whole.blockSignals(True)
            self.text_edit.clear()
            self.cb_whole.setChecked(False)
            self.text_edit.blockSignals(False)
            self.cb_whole.blockSignals(False)

    def load_list_details(self, row):
        # Save previous list's content if we are switching from a valid list
        if self.current_list_name:
            current_item_list = self.list_widget.findItems(self.current_list_name, Qt.MatchExactly)
            if current_item_list:
                old_row = self.list_widget.row(current_item_list[0]) 
                if row != old_row:
                    self.save_list_content(force_save=True) 

        self.save_timer.stop() 
        if row < 0:
            self.current_list_name = None
            self.set_editor_enabled(False)
            return
            
        list_item = self.list_widget.item(row)
        if not list_item: return

        list_name = list_item.text()
        self.current_list_name = list_name
        
        self.set_editor_enabled(True)
        self.name_label.setText(list_name)
        
        keywords_data = self.current_lists.get(list_name, [])
        
        self.text_edit.blockSignals(True)
        self.cb_whole.blockSignals(True)
        
        keywords = "\n".join(k for k, w in keywords_data)
        whole_word = keywords_data[0][1] if keywords_data else False
        
        self.text_edit.setPlainText(keywords)
        self.cb_whole.setChecked(whole_word)
        
        self.text_edit.blockSignals(False)
        self.cb_whole.blockSignals(False)
        
    def save_list_content(self, force_save=False):
        self.save_timer.stop() 
        
        if not self.current_list_name: return

        list_name = self.current_list_name
        text = self.text_edit.toPlainText().splitlines()
        whole_word = self.cb_whole.isChecked()
        
        new_keywords = [(ln.strip(), whole_word) for ln in text if ln.strip()]
        self.current_lists[list_name] = new_keywords
        
    def new_list(self):
        new_name, ok = QInputDialog.getText(self, "New Keyword List", "Enter a name for the new keyword list:")
        if ok and new_name.strip():
            new_name = new_name.strip()
            if new_name in self.current_lists:
                QMessageBox.warning(self, "Error", f"A list named '{new_name}' already exists.")
                return
            
            self.save_list_content(force_save=True)
            
            self.current_lists[new_name] = []
            self.current_list_name = new_name
            self.populate_list()
            
    def rename_list(self):
        if not self.current_list_name: return
        
        old_name = self.current_list_name
        new_name, ok = QInputDialog.getText(self, "Rename Keyword List", f"Rename '{old_name}' to:", text=old_name)
        if ok and new_name.strip() and new_name.strip() != old_name:
            new_name = new_name.strip()
            if new_name in self.current_lists:
                QMessageBox.warning(self, "Error", f"A list named '{new_name}' already exists.")
                return
                
            self.current_lists[new_name] = self.current_lists.pop(old_name)
            self.current_list_name = new_name
            self.populate_list()

    def delete_list(self):
        if not self.current_list_name: return
        
        list_name = self.current_list_name
        if QMessageBox.question(self, "Confirm Delete", 
                                f"Are you sure you want to delete the keyword list '{list_name}'?", 
                                QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            
            del self.current_lists[list_name]
            self.current_list_name = None
            self.populate_list()
            QTimer.singleShot(0, self.select_first_list)  # NEW: Defer selection after delete

    def accept(self):
        self.save_list_content(force_save=True)
        super().accept()

    def get_keyword_lists(self):
        return {k: v for k, v in self.current_lists.items()}

class ColumnConfigDialog(QDialog):
    def __init__(self, headers, order, hidden, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configure Columns")
        self.resize(1200, 900)
        layout = QVBoxLayout(self)
        self.list_widget = QListWidget()
        for h in order:
            item = QListWidgetItem(h)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked if h in hidden else Qt.Checked)
            self.list_widget.addItem(item)
        self.list_widget.setDragDropMode(QListWidget.InternalMove)
        layout.addWidget(self.list_widget)
        
        btns_layout = QHBoxLayout()
        up_btn = QPushButton("Up")
        up_btn.clicked.connect(self.move_up)
        btns_layout.addWidget(up_btn)
        down_btn = QPushButton("Down")
        down_btn.clicked.connect(self.move_down)
        btns_layout.addWidget(down_btn)
        layout.addLayout(btns_layout)
        
        dlg_btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        dlg_btns.accepted.connect(self.accept)
        dlg_btns.rejected.connect(self.reject)

        # Apply dark mode stylesheet if parent has dark mode enabled
        if hasattr(self.parent(), "theme_manager") and hasattr(self.parent(), "dark_mode") and self.parent().dark_mode:
            self.setStyleSheet(self.parent().theme_manager.get_dialog_stylesheet())
        layout.addWidget(dlg_btns)

    def move_up(self):
        row = self.list_widget.currentRow()
        if row > 0:
            item = self.list_widget.takeItem(row)
            self.list_widget.insertItem(row - 1, item)
            self.list_widget.setCurrentRow(row - 1)

    def move_down(self):
        row = self.list_widget.currentRow()
        if row < self.list_widget.count() - 1:
            item = self.list_widget.takeItem(row)
            self.list_widget.insertItem(row + 1, item)
            self.list_widget.setCurrentRow(row + 1)

    def get_config(self):
        order = [self.list_widget.item(i).text() for i in range(self.list_widget.count())]
        hidden = [self.list_widget.item(i).text() for i in range(self.list_widget.count()) if self.list_widget.item(i).checkState() == Qt.Unchecked]
        return order, hidden

class SearchDialog(QDialog):
    def __init__(self, keyword_lists, parent=None):
        super().__init__(parent)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setWindowTitle("Message Search")
        self.resize(1200, 900)
        layout = QVBoxLayout(self)
        
        query_group = QGroupBox("Search Query (Across All Columns)")
        query_layout = QVBoxLayout()
        self.query_text = QLineEdit()
        self.query_text.setPlaceholderText("Enter keyword, message ID, username, IP, etc.")
        query_layout.addWidget(self.query_text)
        self.cb_whole_word = QCheckBox("Exact word/phrase match (slower, more precise)")
        query_layout.addWidget(self.cb_whole_word)
        query_group.setLayout(query_layout)
        layout.addWidget(query_group)
        
        kw_group = QGroupBox("Optional Keyword List Search")
        kw_layout = QHBoxLayout()
        kw_layout.addWidget(QLabel("Keyword List:"))
        self.cb_kw_list = QComboBox()
        self.cb_kw_list.addItem("None (No filter)")
        self.cb_kw_list.addItems(sorted(keyword_lists.keys()))
        kw_layout.addWidget(self.cb_kw_list)
        kw_group.setLayout(kw_layout)
        layout.addWidget(kw_group)
        
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)

        # Apply dark mode stylesheet if parent has dark mode enabled
        if hasattr(self.parent(), "theme_manager") and hasattr(self.parent(), "dark_mode") and self.parent().dark_mode:
            self.setStyleSheet(self.parent().theme_manager.get_dialog_stylesheet())
        layout.addWidget(btns)

    def get_search_params(self):
        return {
            'query': self.query_text.text().strip(),
            'exact_match': self.cb_whole_word.isChecked(),
            'keyword_list': self.cb_kw_list.currentText() if self.cb_kw_list.currentText() != "None (No filter)" else None
        }

class FilterDialog(QDialog):
    def __init__(self, unique_values, current_filters, parent=None):
        super().__init__(parent)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setWindowTitle("Message Filters")
        self.resize(1200, 900)
        self.unique_values = unique_values
        self.current_filters = current_filters
        
        main_layout = QVBoxLayout(self)
        
        date_group = QGroupBox("Date Range Filter")
        date_layout = QHBoxLayout()
        date_layout.addWidget(QLabel("From:"))
        self.from_date_edit = QDateEdit(calendarPopup=True)
        self.from_date_edit.setDate(QDate.currentDate().addYears(-10))
        date_layout.addWidget(self.from_date_edit)
        date_layout.addWidget(QLabel("To:"))
        self.to_date_edit = QDateEdit(calendarPopup=True)
        self.to_date_edit.setDate(QDate.currentDate())
        date_layout.addWidget(self.to_date_edit)
        date_group.setLayout(date_layout)
        main_layout.addWidget(date_group)

        if 'from_date' in current_filters and current_filters['from_date']:
            d = current_filters['from_date']
            self.from_date_edit.setDate(QDate(d.year, d.month, d.day))
        if 'to_date' in current_filters and current_filters['to_date']:
            display_date = current_filters['to_date'] - pd.Timedelta(seconds=1) 
            self.to_date_edit.setDate(QDate(display_date.year, display_date.month, display_date.day))

        filters_group = QGroupBox("Column Filters (AND Logic)")
        filters_layout = QVBoxLayout()
        
        filters_layout.addWidget(QLabel("Sender Username:"))
        self.sender_combo = QComboBox()
        self.sender_combo.addItem("All Senders")
        self.sender_combo.addItems(sorted(self.unique_values.get('sender_username', [])))
        filters_layout.addWidget(self.sender_combo)
        self._set_combo_current(self.sender_combo, self.current_filters.get('sender_username', 'All Senders'))

        filters_layout.addWidget(QLabel("Message Type:"))
        self.type_combo = QComboBox()
        self.type_combo.addItem("All Types")
        self.type_combo.addItems(sorted(self.unique_values.get('message_type', [])))
        filters_layout.addWidget(self.type_combo)
        self._set_combo_current(self.type_combo, self.current_filters.get('message_type', 'All Types'))

        filters_layout.addWidget(QLabel("Content Type:"))
        self.content_combo = QComboBox()
        self.content_combo.addItem("All Content")
        self.content_combo.addItems(sorted(self.unique_values.get('content_type', [])))
        filters_layout.addWidget(self.content_combo)
        self._set_combo_current(self.content_combo, self.current_filters.get('content_type', 'All Content'))
        
        filters_layout.addWidget(QLabel("Message Status:"))
        self.saved_combo = QComboBox()
        self.saved_combo.addItem("All Messages")
        self.saved_combo.addItem("Only Saved")
        self.saved_combo.addItem("Only Unsaved")
        self.saved_combo.setCurrentText(self.current_filters.get('is_saved_display', 'All Messages'))
        filters_layout.addWidget(self.saved_combo)

        filters_group.setLayout(filters_layout)
        main_layout.addWidget(filters_group)

        btns_layout = QHBoxLayout()
        self.clear_btn = QPushButton("Clear All Filters")
        self.clear_btn.clicked.connect(self.clear_filters)
        btns_layout.addWidget(self.clear_btn)
        
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        btns_layout.addWidget(btns)
        main_layout.addLayout(btns_layout)


        # Apply dark mode stylesheet if parent has dark mode enabled
        if hasattr(self.parent(), "theme_manager") and hasattr(self.parent(), "dark_mode") and self.parent().dark_mode:
            self.setStyleSheet(self.parent().theme_manager.get_dialog_stylesheet())
        self.dates_cleared = False

    def _set_combo_current(self, combo, value):
        if value and value in [combo.itemText(i) for i in range(combo.count())]:
            combo.setCurrentText(value)

    def clear_filters(self):
        self.from_date_edit.setDate(QDate(1752, 9, 14))  # Min date to indicate cleared
        self.to_date_edit.setDate(QDate.currentDate())
        self.sender_combo.setCurrentIndex(0)
        self.type_combo.setCurrentIndex(0)
        self.content_combo.setCurrentIndex(0)
        self.saved_combo.setCurrentIndex(0)
        self.dates_cleared = True

    def get_filters(self):
        filters = {}
        dt_from = self.from_date_edit.date().toPyDate()
        dt_to = self.to_date_edit.date().toPyDate()
        if self.from_date_edit.date().year() == 1752:  # Min year indicates cleared
            filters['from_date'] = None
            filters['to_date'] = None
        else:
            filters['from_date'] = pd.to_datetime(dt_from).tz_localize('UTC')
            filters['to_date'] = pd.to_datetime(dt_to).tz_localize('UTC') + pd.Timedelta(days=1)
        
        if self.sender_combo.currentText() != "All Senders":
            filters['sender_username'] = self.sender_combo.currentText()
        if self.type_combo.currentText() != "All Types":
            filters['message_type'] = self.type_combo.currentText()
        if self.content_combo.currentText() != "All Content":
            filters['content_type'] = self.content_combo.currentText()
            
        saved_status = self.saved_combo.currentText()
        filters['is_saved_display'] = saved_status
        if saved_status == "Only Saved":
            filters['is_saved'] = True
        elif saved_status == "Only Unsaved":
            filters['is_saved'] = False
        else:
            filters['is_saved'] = None
            
        return filters

# =============================================================================
# --- THREADING (FOR PERFORMANCE) ---
# =============================================================================

class ThumbnailWorkerThread(QThread):
    """OPTIMIZED: Background thread for async thumbnail generation"""
    thumbnail_ready = pyqtSignal(int, str, str)  # row_index, media_path, thumb_path
    thumbnail_failed = pyqtSignal(int, str)  # row_index, media_path
    
    def __init__(self, jobs, media_extract_dir, thumb_dir, parent=None):
        super().__init__(parent)
        self.jobs = jobs  # List of (row_index, media_id, zpath, internal) tuples
        self.media_extract_dir = media_extract_dir
        self.thumb_dir = thumb_dir
        self._stop = False
    
    def run(self):
        for row_idx, media_id, zpath, internal in self.jobs:
            if self._stop:
                break
            try:
                extracted = extract_file_from_zip(zpath, internal, self.media_extract_dir)
                if extracted and os.path.exists(extracted):
                    thumb = generate_thumbnail(extracted, self.thumb_dir)
                    if thumb and os.path.exists(thumb):
                        self.thumbnail_ready.emit(row_idx, extracted, thumb)
                    else:
                        self.thumbnail_failed.emit(row_idx, extracted)
                else:
                    self.thumbnail_failed.emit(row_idx, internal)
            except Exception as e:
                if logger.isEnabledFor(logging.ERROR):
                    logger.error(f"Thumbnail worker error for {internal}: {e}")
                self.thumbnail_failed.emit(row_idx, internal)
    
    def stop(self):
        self._stop = True

class MultiProgressDialog(QDialog):
    """Custom progress dialog with multiple progress bars for different import phases"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Importing Data")
        self.setWindowModality(Qt.WindowModal)
        self.setMinimumSize(500, 300)
        
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        
        # Title
        title_label = QLabel("<b>Importing Data</b>")
        title_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(title_label)
        
        # Phase 1: ZIP Indexing
        self.phase1_group = QGroupBox("Phase 1: Indexing ZIP Archive")
        phase1_layout = QVBoxLayout()
        self.phase1_progress = QProgressBar(self)
        self.phase1_progress.setRange(0, 100)
        self.phase1_progress.setValue(0)
        self.phase1_label = QLabel("Initializing...")
        phase1_layout.addWidget(self.phase1_label)
        phase1_layout.addWidget(self.phase1_progress)
        self.phase1_group.setLayout(phase1_layout)
        layout.addWidget(self.phase1_group)
        
        # Phase 2: CSV Parsing
        self.phase2_group = QGroupBox("Phase 2: Parsing Conversation Files")
        phase2_layout = QVBoxLayout()
        self.phase2_progress = QProgressBar(self)
        self.phase2_progress.setRange(0, 100)
        self.phase2_progress.setValue(0)
        self.phase2_label = QLabel("Waiting...")
        phase2_layout.addWidget(self.phase2_label)
        phase2_layout.addWidget(self.phase2_progress)
        self.phase2_group.setLayout(phase2_layout)
        layout.addWidget(self.phase2_group)
        
        # Phase 3: Post-Processing
        self.phase3_group = QGroupBox("Phase 3: Post-Processing Data")
        phase3_layout = QVBoxLayout()
        self.phase3_progress = QProgressBar(self)
        self.phase3_progress.setRange(0, 100)
        self.phase3_progress.setValue(0)
        self.phase3_label = QLabel("Waiting...")
        phase3_layout.addWidget(self.phase3_label)
        phase3_layout.addWidget(self.phase3_progress)
        self.phase3_group.setLayout(phase3_layout)
        layout.addWidget(self.phase3_group)
        
        # Phase 4: UI Population - Selector
        self.phase4_group = QGroupBox("Phase 4: Populating Conversation Selector")
        phase4_layout = QVBoxLayout()
        self.phase4_progress = QProgressBar(self)
        self.phase4_progress.setRange(0, 100)
        self.phase4_progress.setValue(0)
        self.phase4_label = QLabel("Waiting...")
        phase4_layout.addWidget(self.phase4_label)
        phase4_layout.addWidget(self.phase4_progress)
        self.phase4_group.setLayout(phase4_layout)
        layout.addWidget(self.phase4_group)
        
        # Phase 5: UI Population - Table
        self.phase5_group = QGroupBox("Phase 5: Populating Message Table")
        phase5_layout = QVBoxLayout()
        self.phase5_progress = QProgressBar(self)
        self.phase5_progress.setRange(0, 100)
        self.phase5_progress.setValue(0)
        self.phase5_label = QLabel("Waiting...")
        phase5_layout.addWidget(self.phase5_label)
        phase5_layout.addWidget(self.phase5_progress)
        self.phase5_group.setLayout(phase5_layout)
        layout.addWidget(self.phase5_group)
        
        # Store phase states
        self.phase_states = {
            1: False,  # Not started
            2: False,
            3: False,
            4: False,
            5: False
        }

        # Apply dark mode stylesheet if parent has dark mode enabled
        if hasattr(self.parent(), "theme_manager") and hasattr(self.parent(), "dark_mode") and self.parent().dark_mode:
            self.setStyleSheet(self.parent().theme_manager.get_dialog_stylesheet())
    
    def update_phase(self, phase_num, percentage, message):
        """Update a specific phase's progress"""
        # Log progress updates for Phase 4 in detail, and completion for all phases
        if phase_num == 4:
            logger.info(f"PHASE 4 PROGRESS: {percentage}% - {message}")
        elif percentage >= 100:
            logger.info(f"PHASE {phase_num} COMPLETE: {message} (100%)")
        elif percentage == 0:
            logger.info(f"PHASE {phase_num} STARTED: {message} (0%)")
        
        if phase_num == 1:
            self.phase1_progress.setValue(int(percentage))
            self.phase1_label.setText(message)
            if percentage >= 100:
                self.phase1_label.setText("<b>✓ Process Complete</b>")
                self.phase1_label.setStyleSheet("color: green;")
                self.phase_states[1] = True
        elif phase_num == 2:
            self.phase2_progress.setValue(int(percentage))
            self.phase2_label.setText(message)
            if percentage >= 100:
                self.phase2_label.setText("<b>✓ Process Complete</b>")
                self.phase2_label.setStyleSheet("color: green;")
                self.phase_states[2] = True
        elif phase_num == 3:
            self.phase3_progress.setValue(int(percentage))
            self.phase3_label.setText(message)
            if percentage >= 100:
                self.phase3_label.setText("<b>✓ Process Complete</b>")
                self.phase3_label.setStyleSheet("color: green;")
                self.phase_states[3] = True
        elif phase_num == 4:
            self.phase4_progress.setValue(int(percentage))
            self.phase4_progress.update()  # Force immediate update of progress bar
            self.phase4_progress.repaint()  # Force immediate repaint of progress bar
            self.phase4_label.setText(message)
            self.phase4_label.update()  # Force immediate update of label
            self.phase4_label.repaint()  # Force immediate repaint of label
            self.phase4_group.update()  # Update the group box
            self.update()  # Update the dialog itself
            if percentage >= 100:
                self.phase4_label.setText("<b>✓ Process Complete</b>")
                self.phase4_label.setStyleSheet("color: green;")
                self.phase_states[4] = True
        elif phase_num == 5:
            self.phase5_progress.setValue(int(percentage))
            self.phase5_label.setText(message)
            if percentage >= 100:
                self.phase5_label.setText("<b>✓ Process Complete</b>")
                self.phase5_label.setStyleSheet("color: green;")
                self.phase_states[5] = True
        
        QApplication.processEvents()
    
    def start_phase(self, phase_num, message):
        """Mark a phase as started"""
        logger.info(f"PHASE {phase_num} START: {message}")
        if phase_num == 1:
            self.phase1_label.setText(message)
            self.phase1_label.setStyleSheet("")
        elif phase_num == 2:
            self.phase2_label.setText(message)
            self.phase2_label.setStyleSheet("")
        elif phase_num == 3:
            self.phase3_label.setText(message)
            self.phase3_label.setStyleSheet("")
        elif phase_num == 4:
            self.phase4_label.setText(message)
            self.phase4_label.setStyleSheet("")
        elif phase_num == 5:
            self.phase5_label.setText(message)
            self.phase5_label.setStyleSheet("")
        QApplication.processEvents()


class ZipLoaderThread(QThread):
    finished_indexing = pyqtSignal(list, dict, list, list, dict, str)  # Added token_index dict before error_message
    progress_update = pyqtSignal(int, str)  # Signal for progress updates: (percentage, message)
    def __init__(self, zip_path, parent=None):
        super().__init__(parent)
        self.zip_path = zip_path
    def run(self):
        all_messages = []
        conversations = defaultdict(list)
        basenames = []
        conv_files = []  # NEW: Initialize as empty list for error cases
        error_message = ""
       
        try:
            # Phase 1: Indexing ZIP file (0-15%) - lazy token_index (not built here)
            self.progress_update.emit(0, "Scanning ZIP archive structure...")
            mapping, basenames, conv_files, token_index = build_media_index(self.zip_path, build_token_index=False)
            self.progress_update.emit(5, f"Indexed {len(basenames)} media files, found {len(conv_files)} conversation file(s)")
           
            if not conv_files:
                error_message = f"Could not find 'conversations.csv' in the ZIP archive or its nested zips."
                self.finished_indexing.emit(all_messages, conversations, basenames, conv_files, {}, error_message)
                return
            
            # Phase 2: Parsing CSV files (15-50%)
            total_conv_files = len(conv_files)
            file_progress_range = 35 / total_conv_files if total_conv_files > 0 else 35  # Each file gets equal share of 35% (15-50%)

            # Track seen messages across all conversations.csv files to avoid
            # duplicating identical rows that appear in multiple folders/zips.
            # A "duplicate" is defined as having the same data in *all* fields
            # of the parsed message dict. If any field differs, it will be
            # treated as a distinct message.
            seen_message_signatures = set()
            
            for file_idx, (zip_with_conv, internal_conv) in enumerate(conv_files):
                progress_pct_start = 15 + int((file_idx / total_conv_files) * 35)  # Start progress for this file
                csv_name = os.path.basename(internal_conv)
                self.progress_update.emit(progress_pct_start, f"Parsing conversations.csv ({file_idx + 1}/{total_conv_files}): {csv_name}")
                
                # OPTIMIZED: Get raw bytes directly (no temp file extraction)
                raw_data = get_file_bytes_from_zip(zip_with_conv, internal_conv)
                if not raw_data:
                    error_message += f"Failed to read {internal_conv}. "
                    continue
                
                try:
                    try:
                        data = raw_data.decode('utf-8')
                    except UnicodeDecodeError:
                        data = raw_data.decode('latin1', errors='ignore')
                    lines = data.splitlines()
                    skip = 0
                    for i, line in enumerate(lines):
                        if 'content_type,message_type' in line:
                            skip = i
                            break
                            
                    # Build a short "source" label: folder + csv filename (no full path)
                    csv_name = os.path.basename(internal_conv)
                    folder_name = os.path.basename(os.path.dirname(internal_conv))
                    if folder_name:
                        source_label = f"{folder_name}/{csv_name}"
                    else:
                        source_label = csv_name
                   
                    # OPTIMIZED: Use StringIO instead of temp file - eliminates disk I/O
                    csv_content = lines[skip] + '\n' + '\n'.join(lines[skip+1:])
                    csv_io = io.StringIO(csv_content)
                    
                    # OPTIMIZED: Read CSV first to get actual columns, then filter and set dtypes
                    # Read without usecols first to see what columns exist
                    df_temp = pd.read_csv(csv_io, encoding='utf-8', nrows=0)  # Read just header
                    actual_cols = df_temp.columns.tolist()
                    
                    # Reset StringIO for full read
                    csv_io = io.StringIO(csv_content)
                    
                    # OPTIMIZED: Only read required columns that actually exist, and set explicit dtypes
                    required_cols = ['conversation_id', 'message_id', 'reply_to_message_id', 'content_type', 
                                    'message_type', 'timestamp', 'sender_username', 'recipient_username', 
                                    'sender', 'receiver', 'text', 'message', 'media_id', 'content_id', 
                                    'saved_by', 'is_one_on_one', 'upload_ip', 'source_port_number', 
                                    'reactions', 'screenshotted_by', 'replayed_by', 'screen_recorded_by', 
                                    'read_by', 'conversation_title', 'group_member_usernames', 'group_member_user_ids']
                    dtype_map = {
                        'conversation_id': 'string',
                        'message_id': 'string',
                        'reply_to_message_id': 'string',
                        'sender_username': 'string',
                        'recipient_username': 'string',
                        'sender': 'string',
                        'receiver': 'string',
                        'text': 'string',
                        'message': 'string',
                        'media_id': 'string',
                        'content_id': 'string',
                        'saved_by': 'string',
                        'upload_ip': 'string',
                        'source_port_number': 'string',
                        'reactions': 'string',
                        'screenshotted_by': 'string',
                        'replayed_by': 'string',
                        'screen_recorded_by': 'string',
                        'read_by': 'string',
                        'conversation_title': 'string',
                        # Handle bool column with NA values - use object type and convert manually
                        # 'is_one_on_one': 'bool',  # Commented out - will handle NA values manually
                        'group_member_usernames': 'string',
                        'group_member_user_ids': 'string'
                    }
                    
                    # Only use columns that actually exist in the CSV
                    available_cols = [c for c in required_cols if c in actual_cols]
                    # Build dtype dict only for columns that exist
                    dtype_dict = {k: v for k, v in dtype_map.items() if k in available_cols}
                    
                    # Read CSV with only existing required columns
                    if available_cols:
                        parse_dates_list = ['timestamp'] if 'timestamp' in available_cols else None
                        df = pd.read_csv(csv_io, encoding='utf-8', parse_dates=parse_dates_list, 
                                        usecols=available_cols, dtype=dtype_dict)
                        
                        # Fix: Handle is_one_on_one bool column with NA values
                        if 'is_one_on_one' in df.columns:
                            # Convert to nullable boolean, handling NA values
                            df['is_one_on_one'] = df['is_one_on_one'].apply(
                                lambda x: True if pd.notna(x) and str(x).lower() in ['true', '1', 'yes'] 
                                else False if pd.notna(x) and str(x).lower() in ['false', '0', 'no'] 
                                else None
                            )
                    else:
                        # Fallback: read all columns if none of our required columns exist
                        csv_io = io.StringIO(csv_content)
                        parse_dates_list = ['timestamp'] if 'timestamp' in actual_cols else None
                        df = pd.read_csv(csv_io, encoding='utf-8', parse_dates=parse_dates_list)
                    
                    # Clean up numeric ID columns
                    id_columns = ['message_id', 'reply_to_message_id']
                    for col in id_columns:
                        if col in df.columns:
                            df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) else '')
                    
                    # Filter rows: Allow normal rows OR flagged media rows (incomplete rows with sender_username, timestamp, media_id)
                    # Normal rows: have content_type and conversation_id
                    # Flagged media rows: have sender_username, timestamp, and media_id (but may be missing content_type or conversation_id)
                    normal_rows_mask = df['content_type'].notna() & df['conversation_id'].notna()
                    flagged_media_mask = (
                        df['sender_username'].notna() & 
                        df['timestamp'].notna() & 
                        df['media_id'].notna()
                    )
                    df = df[normal_rows_mask | flagged_media_mask]
                    
                    # Update progress during CSV row processing
                    total_rows = len(df)
                    rows_processed = 0
                    last_progress_update = progress_pct_start
                    
                    # OPTIMIZED: Use itertuples instead of iterrows for speed
                    for row_tuple in df.itertuples(index=True):
                        idx = row_tuple.Index
                        rows_processed += 1
                        # Update progress every 1000 rows or at milestones
                        if rows_processed % 1000 == 0 or rows_processed == total_rows:
                            # Calculate progress within this file's range
                            file_progress = (rows_processed / total_rows) * file_progress_range
                            current_pct = int(progress_pct_start + file_progress)
                            # Cap at the next file's start progress
                            max_pct = 15 + int(((file_idx + 1) / total_conv_files) * 35) if total_conv_files > 0 else 50
                            current_pct = min(current_pct, max_pct)
                            if current_pct != last_progress_update:
                                self.progress_update.emit(current_pct, 
                                                         f"Processing messages from {csv_name} ({rows_processed:,}/{total_rows:,})")
                                last_progress_update = current_pct
                        # OPTIMIZED: Convert row to dict.
                        # IMPORTANT: Use label-based lookup (loc) instead of iloc; after filtering,
                        # df's index may be non-contiguous (e.g., [1, 3, 5]), so using iloc(idx)
                        # can produce "single positional indexer is out-of-bounds" for valid rows.
                        msg = df.loc[idx].to_dict()
                        msg = {k: '' if pd.isna(v) and not isinstance(v, pd.Timestamp) else v for k, v in msg.items()}

                        # Compute original line number in the *original* conversations.csv
                        # - 'skip' is the 0-based line index of the header row in the original file
                        # - Data rows start at original line (skip + 2), because:
                        #     line index skip      -> header (1-based: skip + 1)
                        #     line index skip + 1  -> first data line (1-based: skip + 2)
                        source_line = skip + 2 + int(idx)

                        # Attach source metadata
                        msg['source'] = source_label
                        msg['source_line'] = int(source_line)

                        ts = msg.get('timestamp')
                        msg['timestamp'] = ts.to_pydatetime() if pd.notna(ts) and hasattr(ts, 'to_pydatetime') else (ts if pd.notna(ts) else None)

                        # Check if this is a flagged media row (incomplete row with only sender_username, timestamp, media_id)
                        # A flagged media row must have sender_username, timestamp, and media_id, but be missing BOTH content_type AND conversation_id
                        has_required_fields = (
                            msg.get('sender_username') and 
                            msg.get('timestamp') and 
                            msg.get('media_id')
                        )
                        missing_both = (
                            not msg.get('conversation_id') and 
                            not msg.get('content_type')
                        )
                        is_flagged_media = has_required_fields and missing_both
                        
                        if is_flagged_media:
                            # Assign to special "Reported Files" conversation
                            msg['conversation_id'] = '__REPORTED_FILES__'
                            msg['conversation_title'] = 'Reported Files'
                            msg['is_flagged_media'] = True
                            # Ensure tags is a set (but don't add automatic tag)
                            if not isinstance(msg.get('tags'), (list, set)):
                                msg['tags'] = set()
                            elif isinstance(msg['tags'], list):
                                msg['tags'] = set(msg['tags'])
                        
                        conv_id = str(msg.get('conversation_id', ''))
                        if not conv_id:
                            continue

                        # Build a canonical signature from *all* fields in msg.
                        # This ensures we only treat a row as a duplicate if every
                        # field (including source, timestamps, etc.) is identical.
                        canonical_items = []
                        for k, v in msg.items():
                            # Normalize various value types into deterministic, hashable forms
                            if isinstance(v, set):
                                norm_v = ('__set__', tuple(sorted(v)))
                            elif isinstance(v, pd.Timestamp):
                                norm_v = ('__ts__', v.isoformat() if not pd.isna(v) else '')
                            elif isinstance(v, datetime.datetime):
                                norm_v = ('__dt__', v.isoformat())
                            else:
                                # Use string representation for other scalar types
                                norm_v = '' if v is None else str(v)
                            canonical_items.append((k, norm_v))
                        sig_tuple = tuple(sorted(canonical_items))

                        if sig_tuple in seen_message_signatures:
                            # Exact same message (all fields) already loaded; skip duplicate row.
                            continue
                        seen_message_signatures.add(sig_tuple)

                        if not isinstance(msg.get('tags'), (list, set)):
                            msg['tags'] = set()

                        message_index = len(all_messages)
                        all_messages.append(msg)
                        conversations[conv_id].append(message_index)

                except pd.errors.ParserError as e:
                    error_message += f"Failed to parse CSV {internal_conv}: {e}. "
                except Exception as e:
                    error_message += f"Unknown error parsing {internal_conv}: {e}. "
       
        except zipfile.BadZipFile:
            error_message = f"Failed to open ZIP file: {self.zip_path} is not a valid ZIP archive."
        except Exception as e:
            error_message = f"An unexpected error occurred during ZIP processing: {e}"
        
        # Final progress update before emitting finished signal
        if not error_message:
            self.progress_update.emit(50, f"Loaded {len(all_messages):,} messages from {len(conversations)} conversations")
        
        self.finished_indexing.emit(all_messages, conversations, basenames, conv_files, token_index, error_message)

class MediaCellWidget(QWidget):
    def __init__(self, pixmap, media_type, media_path, parent=None, main_window=None):
        super().__init__(parent)
        self.setLayout(QVBoxLayout())
        self.layout().setContentsMargins(0, 0, 0, 0)
        self.layout().setSpacing(0)
        
        # Store reference to main window for blur_all access
        self.main_window = main_window
        if not self.main_window:
            # Try to find main window from parent chain
            p = parent
            while p and not isinstance(p, QMainWindow):
                p = p.parent()
            if p:
                self.main_window = p
        
        # Thumbnail label
        self.thumb_label = QLabel()
        self.thumb_label.setPixmap(pixmap)
        self.thumb_label.setScaledContents(False)  # Keep aspect ratio
        self.thumb_label.setAlignment(Qt.AlignCenter)
        self.layout().addWidget(self.thumb_label)
        
        # Overlay label for media type
        self.type_label = QLabel(self.thumb_label)  # Make it a child of thumb_label for overlay
        label_text = 'IMG' if media_type == 'image' else 'VID' if media_type == 'video' else 'OTHER'
        self.type_label.setText(label_text)
        self.type_label.setStyleSheet("""
            background-color: rgba(0, 0, 0, 0.5);
            color: white;
            padding: 2px 5px;
            font-size: 10px;
            font-weight: bold;
            border-radius: 3px;
        """)
        self.type_label.setAlignment(Qt.AlignCenter)
        self.type_label.move(5, 5)  # Position at top-left (adjust offsets as needed)
        self.type_label.adjustSize()  # Auto-size to content
        
        # Make clickable to open media_path
        self.media_path = media_path
        self.local_blur = False  # Track individual thumbnail blur state
        self.thumb_label.mousePressEvent = self.open_media
        self.thumb_label.setContextMenuPolicy(Qt.CustomContextMenu)
        self.thumb_label.customContextMenuRequested.connect(self.context_menu)
        self.setToolTip(os.path.basename(media_path))
    
    def open_media(self, event):
        if event.button() == Qt.LeftButton and self.media_path:
            QDesktopServices.openUrl(QUrl.fromLocalFile(self.media_path))
    
    def context_menu(self, pos):
        """Right-click context menu for blur/unblur and copy options"""
        menu = QMenu(self)
        # Check if thumbnail is currently blurred (either locally or globally)
        global_blur = self.main_window.blur_all if self.main_window and hasattr(self.main_window, 'blur_all') else False
        is_currently_blurred = self.local_blur or global_blur
        toggle_action = menu.addAction("Unblur" if is_currently_blurred else "Blur")
        copy_action = menu.addAction("Copy File Name")
        action = menu.exec_(self.thumb_label.mapToGlobal(pos))
        if action == toggle_action:
            self.local_blur = not self.local_blur
            self.update_blur_effect()
        elif action == copy_action:
            if self.media_path:
                QApplication.clipboard().setText(os.path.basename(self.media_path))
    
    def update_blur_effect(self):
        """Update blur effect based on local blur state and global blur_all setting"""
        # Blur if local_blur is True, OR if global blur_all is True (and local_blur hasn't been explicitly set to False)
        # If local_blur is True, always blur (overrides global)
        # If local_blur is False, use global blur_all setting
        should_blur = False
        if self.local_blur:
            # Local blur explicitly enabled - always blur
            should_blur = True
        else:
            # Local blur is False - check global blur_all setting
            if self.main_window and hasattr(self.main_window, 'blur_all'):
                should_blur = self.main_window.blur_all
        
        if should_blur:
            eff = QGraphicsBlurEffect()
            eff.setBlurRadius(10)
            self.thumb_label.setGraphicsEffect(eff)
        else:
            self.thumb_label.setGraphicsEffect(None)

class SnapParserMain(QMainWindow):
    TAG_COLORS = {
        "CSAM": QColor(255, 102, 102),
        "Evidence": QColor(255, 255, 102),
        "Of Interest": QColor(102, 255, 102),
    }
    
    # A pool of distinct colors to assign to new/custom tags.
    # (Pick anything you like; these are just good, readable choices.)
    AVAILABLE_TAG_COLOR_HEX = [
        "#FFB300",  # vivid orange
        "#803E75",  # strong purple
        "#FF6800",  # vivid orange-ish
        "#A6BDD7",  # very light blue
        "#C10020",  # strong red
        "#CEA262",  # light brown
        "#817066",  # grayish purple
        "#007D34",  # strong green
        "#F6768E",  # light pink
        "#00538A",  # strong blue
        "#FF7A5C",  # coral-ish
        "#53377A",  # deep purple
        "#FF8E00",  # vivid amber
        "#B32851",  # reddish purple
        "#93AA00",  # yellowish green
        "#593315",  # dark brown
        "#F13A13",  # vivid red
        "#232C16",  # dark olive
    ]

    # Default alternating row colors (must match your existing scheme)
    _DEFAULT_ROW_A = QColor(173, 216, 230)  # light blue
    _DEFAULT_ROW_B = QColor(211, 211, 211)  # light gray

    def _get_unused_tag_color(self) -> QColor:
        """Pick a color not used by TAG_COLORS nor the default alternating colors."""
        used_hex = {c.name().lower() for c in getattr(self, "TAG_COLORS", {}).values()}
        used_hex.add(self._DEFAULT_ROW_A.name().lower())
        used_hex.add(self._DEFAULT_ROW_B.name().lower())

        for hx in self.AVAILABLE_TAG_COLOR_HEX:
            if hx.lower() not in used_hex:
                return QColor(hx)

        # Fallback: generate a hue that isn't in use (unlikely to be hit)
        for h in range(0, 360, 17):
            c = QColor.fromHsv(h, 200, 230)
            if c.name().lower() not in used_hex:
                return c

        # Last resort
        return QColor("#999999")

    
    def __init__(self):
        super().__init__()

        # Initialize QSettings for persisting column widths
        self.settings = QSettings("SnapchatParser", "ColumnWidths")
        
        # Dark mode support (disabled by default)
        self.dark_mode = False
        self.theme_manager = ThemeManager(self.dark_mode)
        self.setWindowTitle("Snapchat Parser")
        self.resize(1400, 900)
        
        self.all_messages = []
        self.messages_df = None  # OPTIMIZED: Canonical DataFrame store for all messages
        self.conversations = defaultdict(list)
        self.current_msg_indices = []
        self.user_id_to_username_map = {}  # Mapping of user_id -> username for conversion
        
        self.media_index = {}
        self.basenames = []
        self.token_index = None  # OPTIMIZED: Lazy - built on demand
        self._token_index_built = False  # Track if token_index has been built
        self.media_lookup_cache = {}  # Cache for media lookups to avoid reprocessing
        self._cached_all_conversations_indices = None  # Cache for "All Conversations" filtered indices
        self._cached_filter_mask = None  # OPTIMIZED: Cache filtered boolean mask
        self._last_conv_id_displayed = None  # Track last displayed conversation to avoid unnecessary refreshes
        self._last_filter_hash = None  # Track filter state to detect changes
        self._last_blur_state = None  # Track blur state to detect changes
        self._defer_media_processing = False  # Flag to defer media processing during Phase 4
        # Conversation-level caching for instant switching
        self._conversation_cache = {}  # dict: (conv_id, filter_hash, blur_state) -> messages_data list
        self._last_displayed_indices = None  # Track last displayed indices to avoid unnecessary updates
        self.media_zip_path = None
        self.media_extract_dir = os.path.join(tempfile.gettempdir(), "snapparser_media")
        
        # Notes system - store notes for conversations
        self.conversation_notes = {}  # dict: conv_id (str) -> note (str)
        os.makedirs(self.media_extract_dir, exist_ok=True)
        self.thumb_dir = os.path.join(tempfile.gettempdir(), "snapparser_thumbs")
        os.makedirs(self.thumb_dir, exist_ok=True)
        
        self.available_tags = set(self.TAG_COLORS.keys())
        self.hotkeys = {}
        self.blur_all = False
        self.blurred_thumbnails = set()  # Track individually blurred thumbnails by media_id
        self.reviewed = {}
        self.current_file_id = None
        self.keyword_lists = {}
        self.selected_keyword_list = None
        
        # Logging setting (default from global)
        self.logging_enabled = ENABLE_LOGGING
        
        self.config_path = os.path.expanduser("~/.SnapParser_Config.json")
        self.headers = ["Conversation ID", "Conversation Title", "Message ID", "Reply To", "Content Type", "Message Type", "Date", "Time", "Sender", "Receiver", "Message", "Media ID", "Media", "Tags", "One-on-One?", "Reactions", "Saved By", "Screenshotted By", "Replayed By", "Screen Recorded By", "Read By", "IP", "Port", "Source", "Line Number", "Group Members"]
        self.load_config()
        self.active_filters = {
            'from_date': None,
            'to_date': None,
            'sender_username': None,
            'message_type': None,
            'content_type': None,
            'is_saved': None,
            'is_saved_display': 'All Messages'
        }
        self.column_order = self.headers[:]
        self.hidden_columns = []
        self.init_ui()

        # Apply initial theme (light mode by default)
        self.apply_theme()

        # Apply logging setting from config (or default)
        self.apply_logging_setting(show_status=False)

        self.bind_hotkeys()
        QTimer.singleShot(200, self.auto_prompt_import)
        self.showMaximized()

        
    def _compute_row_brush(self, msg, row):
        tags = msg.get('tags', set()) or set()

        # 1) Priority tags first (unchanged)
        for t in ["CSAM", "Evidence", "Of Interest"]:
            if t in tags and t in self.TAG_COLORS:
                return QBrush(self.TAG_COLORS[t].lighter(130))

        # 2) Any other tag that has a color mapping
        colored = [t for t in sorted(tags) if t in self.TAG_COLORS]
        if colored:
            # pick the first deterministically (sorted for stability)
            return QBrush(self.TAG_COLORS[colored[0]].lighter(130))

        # 3) Fall back to theme-aware alternating scheme
        if self.dark_mode:
            # Dark mode alternating colors
            base = QColor(self.theme_manager.get_color('bg_table_alternate')) if (row % 2) else QColor(self.theme_manager.get_color('bg_table'))
        else:
            # Light mode alternating colors (original)
            base = self._DEFAULT_ROW_B if (row % 2) else self._DEFAULT_ROW_A
        return QBrush(base)
    
    def compute_row_color(self, msg, conv_id, row_index=0, keyword_state=None):
        """Compute row color for model - returns color string or QColor."""
        tags = msg.get('tags', set()) or set()
        
        # 1) Priority tags first
        for t in ["CSAM", "Evidence", "Of Interest"]:
            if t in tags and t in self.TAG_COLORS:
                return self.TAG_COLORS[t].lighter(130).name()
        
        # 2) Any other tag that has a color mapping
        colored = [t for t in sorted(tags) if t in self.TAG_COLORS]
        if colored:
            return self.TAG_COLORS[colored[0]].lighter(130).name()
        
        # 3) Sender-based alternation (simplified - use row index for now)
        if self.dark_mode:
            if row_index % 2:
                return self.theme_manager.get_color('bg_table_alternate')
            else:
                return self.theme_manager.get_color('bg_table')
        else:
            if row_index % 2:
                return self._DEFAULT_ROW_B.name()
            else:
                return self._DEFAULT_ROW_A.name()
    
    def get_media_path(self, media_id, msg_index):
        """Get media path(s) for a given media_id and message index.
        
        Returns a list of media paths if multiple media IDs are found (separated by ~),
        or a single path if only one is found, or None if none found.
        
        Media IDs can be separated by ~ (e.g., "b~token1~b~token2" or "token1~token2")
        """
        if not media_id or not media_id.strip():
            return None
        
        # Check if this is a reported file (flagged media)
        is_reported_file = False
        if msg_index is not None and msg_index < len(self.all_messages):
            msg = self.all_messages[msg_index]
            is_reported_file = msg.get('is_flagged_media', False)
        
        # For reported files, use the special lookup function that searches for filenames containing the media_id
        if is_reported_file:
            raw = str(media_id).strip()
            entries = find_reported_file_media(raw, self.basenames, self.media_lookup_cache)
            
            if entries:
                media_paths = []
                self._media_path_to_id_map = getattr(self, '_media_path_to_id_map', {})
                zpath, internal = entries[0]  # Use first match
                try:
                    extracted = extract_file_from_zip(zpath, internal, self.media_extract_dir)
                    if extracted and os.path.exists(extracted):
                        media_paths.append(extracted)
                        # Store mapping from path to individual media_id for blur tracking
                        self._media_path_to_id_map[extracted] = raw
                except Exception as e:
                    logger.error(f"Error extracting {internal}: {e}")
                
                if media_paths:
                    return media_paths[0] if len(media_paths) == 1 else media_paths
            # If no match found with reported file lookup, return None
            return None
        
        # Normal media lookup (existing logic)
        raw = media_id or ""
        media_ids = []
        
        # First, extract all b~ tokens (these are complete media IDs)
        b_tokens = re.findall(r'b~[A-Za-z0-9_\-]+', raw)
        for token in b_tokens:
            if token not in media_ids:
                media_ids.append(token)
        
        # Remove b~ tokens from raw string to find standalone tokens
        remaining = raw
        for token in b_tokens:
            remaining = remaining.replace(token, '', 1)
        
        # Now look for hex strings (32-char hex) in the remaining string
        # Split by ~ to handle multiple hex strings separated by ~
        parts = remaining.split('~')
        for part in parts:
            part = part.strip()
            if not part:
                continue
            # Check if it's a hex string
            if re.fullmatch(r"[0-9a-fA-F]{32}", part):
                hex_token = part.lower()
                if hex_token not in media_ids:
                    media_ids.append(hex_token)
            else:
                # Try to extract hex strings from this part
                hex_tokens = re.findall(r'[0-9a-fA-F]{32}', part)
                for hex_token in hex_tokens:
                    hex_token = hex_token.lower()
                    if hex_token not in media_ids:
                        media_ids.append(hex_token)
        
        if not media_ids:
            return None
        
        # OPTIMIZED: Build token_index on demand if needed
        if not self._token_index_built:
            self._ensure_token_index()
        
        # Get paths for all media IDs and store mapping
        media_paths = []
        self._media_path_to_id_map = getattr(self, '_media_path_to_id_map', {})
        for mid in media_ids:
            entries = find_media_by_media_id(mid, self.basenames, 
                                             self.token_index, 
                                             self.media_lookup_cache)
            
            if entries:
                zpath, internal = entries[0]
                try:
                    extracted = extract_file_from_zip(zpath, internal, self.media_extract_dir)
                    if extracted and os.path.exists(extracted):
                        media_paths.append(extracted)
                        # Store mapping from path to individual media_id for blur tracking
                        self._media_path_to_id_map[extracted] = mid
                except Exception as e:
                    logger.error(f"Error extracting {internal}: {e}")
        
        if not media_paths:
            return None
        
        # Return single path if only one, list if multiple
        return media_paths[0] if len(media_paths) == 1 else media_paths
    
    def _get_individual_media_ids(self, combined_media_id, media_paths):
        """Get individual media IDs for each media path.
        
        Returns a dictionary mapping media_path -> individual_media_id
        """
        path_to_id = {}
        if not hasattr(self, '_media_path_to_id_map'):
            return path_to_id
        
        for path in media_paths:
            if path in self._media_path_to_id_map:
                path_to_id[path] = self._media_path_to_id_map[path]
        
        return path_to_id


    def update_table_rows_for_msg_indices(self, msg_indices):
        """
        Efficiently update only the visible table rows that correspond to the given message indices.
        This updates the Tags column and then recomputes row backgrounds for the whole visible table
        (priority-tag override, else sender/receiver alternation), without repopulating the entire table.
        """
        if not msg_indices:
            return

        # For QTableView with model, we need to work through the model
        model = self.message_table.model()
        if not model:
            return
        
        # Build a mapping from message_index to table row by scanning model data
        msg_idx_to_row = {}
        row_count = model.rowCount()
        for row in range(row_count):
            # Get message index from model's UserRole data
            index = model.index(row, 0)
            msg_index = index.data(Qt.UserRole)
            if msg_index is not None and isinstance(msg_index, int):
                msg_idx_to_row[msg_index] = row

        # Batch UI updates to avoid lots of repaints
        self.message_table.setUpdatesEnabled(False)
        self.message_table.blockSignals(True)
        try:
            # Resolve Tags column once
            try:
                tags_col = self.headers.index("Tags")
            except ValueError:
                tags_col = -1

            # Collect rows that need updating
            rows_to_update = []
            for msg_idx in msg_indices:
                if msg_idx in msg_idx_to_row:
                    rows_to_update.append(msg_idx_to_row[msg_idx])

            # Trigger model refresh for affected rows
            # The model reads from all_messages which we've already updated
            if rows_to_update:
                # Emit dataChanged for all affected rows and columns
                top_left = model.index(min(rows_to_update), 0)
                bottom_right = model.index(max(rows_to_update), model.columnCount() - 1)
                model.dataChanged.emit(top_left, bottom_right)

            # Repaint all visible rows once using sender-change alternation with priority-tag override
            # (The model's data() method handles background colors automatically)
            self.recompute_visible_row_backgrounds()

        finally:
            self.message_table.blockSignals(False)
            self.message_table.setUpdatesEnabled(True)

            
    def recompute_visible_row_backgrounds(self):
        """Re-apply row backgrounds across visible rows with precedence:
           1) Priority tag (CSAM/Evidence/Of Interest) if present
           2) Any other tag that has a color in TAG_COLORS
           3) Sender-change alternation fallback
           
           For QTableView, this triggers a model refresh so the model's data() method
           can return the correct background colors.
        """
        model = self.message_table.model()
        if not model:
            return
        
        # Update the compute_row_color_func in the model so it uses the latest tag data
        # The model's data() method will use this function to return background colors
        if hasattr(model, 'compute_row_color_func'):
            # The model already has access to all_messages, so it will read updated tags
            # We just need to trigger a refresh
            row_count = model.rowCount()
            if row_count > 0:
                # Emit dataChanged for all rows to trigger repaint with new background colors
                top_left = model.index(0, 0)
                bottom_right = model.index(row_count - 1, model.columnCount() - 1)
                model.dataChanged.emit(top_left, bottom_right)




        
    def get_media_type_from_path(self, path):
        ext = os.path.splitext(path)[1].lower()
        image_exts = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.heic', '.heif']
        video_exts = ['.mp4', '.mov', '.webm', '.avi', '.mkv', '.ogg']
        if ext in image_exts:
            return 'image'
        elif ext in video_exts:
            return 'video'
        else:
            return 'other'
    
    def init_ui(self):
        
        tb = QToolBar()
        self.addToolBar(tb)
        style = QApplication.instance().style()
        actions = [
            ("Open ZIP", self.import_zip_dialog, QIcon(resource_path("icons/import.ico"))),
            ("Search", self.search_dialog, QIcon(resource_path("icons/search.ico"))),
            ("Filters", self.do_filter_dialog, QIcon(resource_path("icons/filter.ico"))),
            ("Tags", self.show_tagged, QIcon(resource_path("icons/tagged.ico"))),
            ("Hotkeys", self.manage_hotkeys, QIcon(resource_path("icons/hotkeys.ico"))),
            ("Keywords", self.manage_keywords, QIcon(resource_path("icons/keywords.ico"))),
        ]
        for label, fn, icon in actions:
            if label == "---":
                tb.addSeparator()
                continue
            act = tb.addAction(icon, label, fn)
            act.setToolTip(label)
        
        # Progress menu button (Save/Load Progress) - positioned between Keywords and Help
        self.progress_btn = QToolButton()
        self.progress_btn.setText("Progress")
        self.progress_btn.setIcon(style.standardIcon(QStyle.SP_DriveFDIcon))
        self.progress_btn.setToolTip("Save or load progress (reviewed conversations and tags)")
        self.progress_btn.setPopupMode(QToolButton.InstantPopup)
        progress_menu = QMenu(self)
        save_action = progress_menu.addAction(style.standardIcon(QStyle.SP_DriveFDIcon), "Save Progress")
        save_action.setToolTip("Save your current progress including reviewed conversations and tagged messages to a file")
        save_action.triggered.connect(self.save_progress)
        load_action = progress_menu.addAction(style.standardIcon(QStyle.SP_DirOpenIcon), "Load Progress")
        load_action.setToolTip("Load previously saved progress from a file")
        load_action.triggered.connect(self.load_progress)
        self.progress_btn.setMenu(progress_menu)
        tb.addWidget(self.progress_btn)
        
        # Export button - positioned next to Progress button
        export_action = tb.addAction(QIcon(resource_path("icons/export.ico")), "Export", self.export_dialog)
        export_action.setToolTip("Export data to HTML or CSV")
        
        # Notes menu button - positioned next to Export button
        self.notes_btn = QToolButton()
        self.notes_btn.setText("Notes")
        # Try to use a notes/document icon, fallback to file icon
        notes_icon = None
        notes_icon_path = resource_path("icons/notes.ico")
        if os.path.exists(notes_icon_path):
            notes_icon = QIcon(notes_icon_path)
        else:
            # Use standard file icon as fallback
            notes_icon = style.standardIcon(QStyle.SP_FileIcon)
        self.notes_btn.setIcon(notes_icon)
        self.notes_btn.setToolTip("Add or view notes for conversations")
        self.notes_btn.setPopupMode(QToolButton.InstantPopup)
        notes_menu = QMenu(self)
        add_note_action = notes_menu.addAction(style.standardIcon(QStyle.SP_FileIcon), "Add Note to Selected Conversation")
        add_note_action.setToolTip("Add or edit a note for the currently selected conversation")
        add_note_action.triggered.connect(self.add_note_to_conversation)
        view_notes_action = notes_menu.addAction(style.standardIcon(QStyle.SP_DirOpenIcon), "View Notes")
        view_notes_action.setToolTip("View all existing notes for all conversations")
        view_notes_action.triggered.connect(self.view_all_notes)
        self.notes_btn.setMenu(notes_menu)
        tb.addWidget(self.notes_btn)
        
        # Separator and Help
        tb.addSeparator()
        help_action = tb.addAction(style.standardIcon(QStyle.SP_MessageBoxQuestion), "Help", self.show_help_dialog)
        help_action.setToolTip("Help")
        
        self.blur_btn = QPushButton(style.standardIcon(QStyle.SP_ArrowDown), "Blur Media: Off")
        self.blur_btn.clicked.connect(self.toggle_blur)
        tb.addWidget(self.blur_btn)
        
        # Dark mode toggle button
        self.dark_mode_btn = QPushButton(style.standardIcon(QStyle.SP_ComputerIcon), "Dark Mode: Off")
        self.dark_mode_btn.clicked.connect(self.toggle_dark_mode)
        tb.addWidget(self.dark_mode_btn)
        
        self.stats_btn = QPushButton("Stats")
        self.stats_btn.clicked.connect(self.show_stats)
        tb.addWidget(self.stats_btn)

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        splitter = QSplitter(Qt.Horizontal)
        
        self.conv_selector = QComboBox()
        # self.conv_selector.setItemDelegate(HtmlDelegate())
        self.conv_selector.setEditable(False)
        self.conv_selector.setSizeAdjustPolicy(QComboBox.AdjustToContentsOnFirstShow)
        self.conv_selector.currentIndexChanged.connect(self.on_conv_selected_combobox)
        self.review_btn = QPushButton(style.standardIcon(QStyle.SP_DialogApplyButton), "Mark As Reviewed")
        self.review_btn.clicked.connect(self.mark_current_reviewed)
        self.review_btn.setEnabled(False)
        selector_box = QHBoxLayout()
        selector_label = QLabel("<b>Conversation Selector:</b>")
        selector_box.addWidget(selector_label)
        selector_box.addWidget(self.conv_selector, stretch=1)
        selector_box.addWidget(self.review_btn)
        main_layout.addLayout(selector_box)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        filter_box = QHBoxLayout()
        filter_box.addWidget(QLabel("<b>Active Filters:</b>"))
        self.filter_status_label = QLabel("None")
        filter_box.addWidget(self.filter_status_label)
        filter_box.addStretch()
        self.clear_filters_btn = QPushButton("Clear All Filters")
        self.clear_filters_btn.clicked.connect(self.clear_all_filters)
        self.clear_filters_btn.setVisible(False)
        filter_box.addWidget(self.clear_filters_btn)
        right_layout.addLayout(filter_box)

        # Create model for virtual scrolling
        self.message_model = MessageTableModel(self)
        # Set headers in the model
        self.message_model.headers = self.headers
        
        # Create table view instead of QTableWidget
        self.message_table = QTableView()
        self.message_table.setModel(self.message_model)
        self.message_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.message_table.setContextMenuPolicy(Qt.CustomContextMenu)
        copy_shortcut = QShortcut(QKeySequence.Copy, self.message_table)
        copy_shortcut.activated.connect(self.copy_selected_from_table)
        
        # Set delegates for HTML rendering and media thumbnails
        html_delegate = HtmlDelegate(self.message_table)
        self.message_table.setItemDelegate(html_delegate)
        # Set media delegate for Media column if it exists
        if "Media" in self.headers:
            media_col = self.headers.index("Media")
            media_delegate = MediaThumbnailDelegate(self.message_table, main_window=self)
            self.message_table.setItemDelegateForColumn(media_col, media_delegate)
        
        self.message_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.message_table.customContextMenuRequested.connect(self.table_ctx_menu)
        # Connect double-click to open group members dialog
        self.message_table.doubleClicked.connect(self.on_table_cell_double_clicked)
        self.message_table.horizontalHeader().setStretchLastSection(True)
        
        # Configure optimal table sizing with improved readability
        configure_table_optimal_sizing(self.message_table, self.headers, "main_table", self.settings)
        
        # Column widths are now set via configure_table_optimal_sizing() using user-determined optimal defaults
        # No need to override here - the defaults in default_widths dictionary will be applied
        
        # Note: Column width dialog is now shown after data import completes (in process_zip_data)
        
        # Connect row height adjustment when columns are resized
        self.message_table.horizontalHeader().sectionResized.connect(self.adjust_row_heights)
        self.message_table.horizontalHeader().setSectionsMovable(True)
        self.message_table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.message_table.horizontalHeader().customContextMenuRequested.connect(self.show_column_menu)
        self.message_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.message_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        # Make horizontal (and vertical) scrolling smooth/pixel-based
        self.message_table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.message_table.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)

        # Make grid lines more distinguishable
        self.message_table.setShowGrid(True)
        # Table styling will be applied by theme

        right_layout.addWidget(self.message_table)
        right_panel.setLayout(right_layout)
        main_layout.addWidget(right_panel)
        
        self.status = QStatusBar()
        self.setStatusBar(self.status)
        
        self.check_updates_btn = QPushButton("Check for Updates")
        self.check_updates_btn.clicked.connect(self.check_for_updates)
        self.status.addPermanentWidget(self.check_updates_btn)
        


    
    def apply_theme(self):
        """Apply the current theme stylesheet to the main window and all tables."""
        if self.dark_mode:
            stylesheet = self.theme_manager.get_stylesheet()
            # Add table-specific dark mode styling
            table_style = """
                QTableWidget {
                    background-color: %s;
                    color: %s;
                    gridline-color: %s;
                    alternate-background-color: %s;
                }
                QTableWidget::item {
                    border-bottom: 1px solid %s;
                }
                QTableWidget::item:selected {
                    background-color: %s;
                    border-bottom: 1px solid %s;
                }
                QHeaderView::section {
                    background-color: %s;
                    color: %s;
                    padding: 5px;
                    border: 1px solid %s;
                }
            """ % (
                self.theme_manager.get_color('bg_table'),
                self.theme_manager.get_color('text_primary'),
                self.theme_manager.get_color('border'),
                self.theme_manager.get_color('bg_table_alternate'),
                self.theme_manager.get_color('border'),
                self.theme_manager.get_color('bg_table_hover'),
                self.theme_manager.get_color('border'),
                self.theme_manager.get_color('bg_alternate'),
                self.theme_manager.get_color('text_primary'),
                self.theme_manager.get_color('border'),
            )
            full_stylesheet = stylesheet + table_style
        else:
            # Light mode - use default styling
            full_stylesheet = """
                QPushButton { padding: 5px; }
                QPushButton:hover { background-color: #e0e0e0; }
                QComboBox { padding: 5px; }
                QLineEdit { padding: 5px; }
                QGroupBox { border: 1px solid #d0d0d0; border-radius: 5px; padding: 10px; }
                QTableWidget {
                    gridline-color: #a0a0a0;
                }
                QTableWidget::item {
                    border-bottom: 1px solid #a0a0a0;
                }
                QTableWidget::item:selected {
                    border-bottom: 1px solid #a0a0a0;
                }
            """
        
        self.setStyleSheet(full_stylesheet)
        # Also apply to QApplication for dialogs
        QApplication.instance().setStyleSheet(full_stylesheet)
        
        # Explicitly apply styling to the main table
        if hasattr(self, 'message_table'):
            if self.dark_mode:
                # Apply dark mode table styling directly
                table_dark_style = """
                    QTableWidget {
                        background-color: %s;
                        color: %s;
                        gridline-color: %s;
                        alternate-background-color: %s;
                    }
                    QTableWidget::item {
                        border-bottom: 1px solid %s;
                        color: %s;
                    }
                    QTableWidget::item:selected {
                        background-color: %s;
                        border-bottom: 1px solid %s;
                        color: %s;
                    }
                    QHeaderView::section {
                        background-color: %s;
                        color: %s;
                        padding: 5px;
                        border: 1px solid %s;
                    }
                """ % (
                    self.theme_manager.get_color('bg_table'),
                    self.theme_manager.get_color('text_primary'),
                    self.theme_manager.get_color('border'),
                    self.theme_manager.get_color('bg_table_alternate'),
                    self.theme_manager.get_color('border'),
                    self.theme_manager.get_color('text_primary'),
                    self.theme_manager.get_color('bg_table_hover'),
                    self.theme_manager.get_color('border'),
                    self.theme_manager.get_color('text_primary'),
                    self.theme_manager.get_color('bg_alternate'),
                    self.theme_manager.get_color('text_primary'),
                    self.theme_manager.get_color('border'),
                )
                self.message_table.setStyleSheet(table_dark_style)
            else:
                # Light mode - reset to default
                self.message_table.setStyleSheet("""
            QTableWidget {
                gridline-color: #a0a0a0;
                        background-color: white;
                        color: black;
            }
            QTableWidget::item {
                border-bottom: 1px solid #a0a0a0;
            }
            QTableWidget::item:selected {
                border-bottom: 1px solid #a0a0a0;
            }
        """)

        # Update table palette for better color support
        palette = self.message_table.palette()
        if self.dark_mode:
            palette.setColor(QPalette.Base, QColor(self.theme_manager.get_color('bg_table')))
            palette.setColor(QPalette.AlternateBase, QColor(self.theme_manager.get_color('bg_table_alternate')))
            palette.setColor(QPalette.Text, QColor(self.theme_manager.get_color('text_primary')))
            palette.setColor(QPalette.Window, QColor(self.theme_manager.get_color('bg_table')))
        else:
            palette.setColor(QPalette.Base, QColor('#ffffff'))
            palette.setColor(QPalette.AlternateBase, QColor('#f5f5f5'))
            palette.setColor(QPalette.Text, QColor('#000000'))
            palette.setColor(QPalette.Window, QColor('#ffffff'))
        self.message_table.setPalette(palette)
        
        # Enable alternating row colors
        self.message_table.setAlternatingRowColors(True)
        
        # Force update and refresh row backgrounds
        self.message_table.viewport().update()
        self.message_table.repaint()
        if hasattr(self, 'current_msg_indices') and self.current_msg_indices:
            self.recompute_visible_row_backgrounds()
    
    def toggle_dark_mode(self):
        """Toggle between light and dark mode."""
        self.dark_mode = not self.dark_mode
        self.theme_manager = ThemeManager(self.dark_mode)
        
        # Update button text and icon
        style = QApplication.instance().style()
        if self.dark_mode:
            self.dark_mode_btn.setText("Dark Mode: On")
            self.dark_mode_btn.setIcon(style.standardIcon(QStyle.SP_ComputerIcon))
        else:
            self.dark_mode_btn.setText("Dark Mode: Off")
            self.dark_mode_btn.setIcon(style.standardIcon(QStyle.SP_ComputerIcon))
        
        # Apply the theme
        self.apply_theme()
        
        # Update status
        mode_text = "enabled" if self.dark_mode else "disabled"
        logger.info(f"Dark mode {mode_text}")
        if hasattr(self, 'status'):
            self.status.showMessage(f"Dark mode {mode_text}", 2000)

    def save_progress(self):
        """Save reviewed conversations and tagged messages to a JSON file."""
        if not self.all_messages:
            QMessageBox.warning(self, "No Data", "No messages loaded. Please load data first.")
            return
        
        # Get user's home directory
        user_home = os.path.expanduser("~")
        if not user_home or not os.path.exists(user_home):
            user_home = os.getcwd()
        
        # Create default filename with case identifier and timestamp
        case_id = self.current_file_id if self.current_file_id else "unknown_case"
        # Sanitize case_id for filename (remove invalid characters)
        safe_case_id = re.sub(r'[<>:"/\\|?*]', '_', str(case_id))[:50]  # Limit length
        default_filename = os.path.join(user_home, f"SnapchatParser_progress_{safe_case_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        # Get save file path
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Progress",
            default_filename,
            "JSON Files (*.json);;All Files (*)"
        )
        
        if not file_path:
            return  # User cancelled
        
        try:
            # Collect all tagged messages
            # Use composite key: conversation_id + message_id (or index if message_id is empty)
            # This ensures uniqueness and proper matching on load
            tagged_messages = {}
            for idx, msg in enumerate(self.all_messages):
                if not msg.get('tags'):
                    continue
                
                conv_id = str(msg.get('conversation_id', '')).strip()
                msg_id = msg.get('message_id')
                
                # Create a unique identifier for this message
                # Use composite key: conversation_id + message_id for uniqueness
                if msg_id:
                    try:
                        msg_id_str = str(msg_id).strip()
                        if msg_id_str:
                            # Normalize message_id (matching CSV parsing: str(int(x)))
                            try:
                                normalized_msg_id = str(int(float(msg_id_str)))
                            except (ValueError, TypeError):
                                normalized_msg_id = msg_id_str
                            
                            # Use composite key: conversation_id + message_id
                            if conv_id and normalized_msg_id:
                                unique_id = f"{conv_id}_{normalized_msg_id}"
                            elif normalized_msg_id:
                                # Fallback to just message_id if no conversation_id
                                unique_id = normalized_msg_id
                            else:
                                # Fallback to index if message_id is invalid
                                unique_id = str(idx)
                        else:
                            # Empty message_id, use index
                            unique_id = str(idx)
                    except Exception as e:
                        # Error normalizing, use index as fallback
                        unique_id = str(idx)
                        logger.debug(f"Error normalizing message_id for save, using index {idx}: {e}")
                else:
                    # No message_id, use index
                    unique_id = str(idx)
                
                # Save tags with unique identifier
                tagged_messages[unique_id] = list(msg['tags'])
            
            # Collect reviewed conversations by case_id
            reviewed_conversations = {}
            for case_id, conv_set in self.reviewed.items():
                if conv_set:  # Only include non-empty sets
                    reviewed_conversations[case_id] = list(conv_set)
            
            # Convert conversation_notes keys to strings for JSON serialization
            notes_for_save = {str(k): v for k, v in self.conversation_notes.items()}
            
            # Create progress data structure
            progress_data = {
                'version': APP_VERSION,
                'saved_at': datetime.datetime.now().isoformat(),
                'case_id': self.current_file_id if self.current_file_id else None,
                'reviewed_conversations': reviewed_conversations,
                'tagged_messages': tagged_messages,
                'conversation_notes': notes_for_save,
                'total_reviewed': sum(len(v) for v in self.reviewed.values()),
                'total_tagged': len(tagged_messages)
            }
            
            # Save to file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=4)
            
            total_reviewed = sum(len(v) for v in self.reviewed.values())
            total_notes = len(notes_for_save)
            self.status.showMessage(
                f"Progress saved: {total_reviewed} reviewed conversations, {len(tagged_messages)} tagged messages, {total_notes} notes"
            )
            
            msg = QMessageBox(self)
            if hasattr(self, 'theme_manager') and self.dark_mode:
                msg.setStyleSheet(self.theme_manager.get_dialog_stylesheet())
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("Progress Saved")
            msg.setText(
                f"Progress saved successfully!\n\n"
                f"Reviewed conversations: {total_reviewed}\n"
                f"Tagged messages: {len(tagged_messages)}\n"
                f"Notes: {total_notes}\n\n"
                f"File: {os.path.basename(file_path)}"
            )
            msg.exec()
            logger.info(f"Progress saved to {file_path}")
        except Exception as e:
            error_msg = f"Error saving progress: {str(e)}"
            logger.error(error_msg)
            msg = QMessageBox(self)
            if hasattr(self, 'theme_manager') and self.dark_mode:
                msg.setStyleSheet(self.theme_manager.get_dialog_stylesheet())
            msg.setIcon(QMessageBox.Critical)
            msg.setWindowTitle("Save Error")
            msg.setText(error_msg)
            msg.exec()
            self.status.showMessage("Error saving progress")

    def load_progress(self):
        """Load previously saved progress from a JSON file."""
        if not self.all_messages:
            QMessageBox.warning(self, "No Data", "No messages loaded. Please load data first.")
            return
        
        # Get user's home directory
        user_home = os.path.expanduser("~")
        if not user_home or not os.path.exists(user_home):
            user_home = os.getcwd()
        
        # Get load file path
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Load Progress",
            user_home,
            "JSON Files (*.json);;All Files (*)"
        )
        
        if not file_path:
            return  # User cancelled
        
        try:
            # Create progress dialog
            progress = QProgressDialog("Loading progress...", None, 0, 100, self)
            if hasattr(self, 'theme_manager') and self.dark_mode:
                progress.setStyleSheet(self.theme_manager.get_dialog_stylesheet())
            progress.setWindowTitle("Load Progress")
            progress.setWindowModality(Qt.WindowModal)
            progress.setMinimumDuration(0)
            progress.setValue(0)
            QApplication.processEvents()
            
            # Load progress data
            progress.setLabelText("Reading progress file...")
            progress.setValue(10)
            QApplication.processEvents()
            
            with open(file_path, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
            
            # Validate structure (notes are optional for backward compatibility)
            if 'reviewed_conversations' not in progress_data or 'tagged_messages' not in progress_data:
                raise ValueError("Invalid progress file format")
            
            # Load conversation notes (if present)
            progress.setLabelText("Loading conversation notes...")
            progress.setValue(25)
            QApplication.processEvents()
            
            loaded_notes = 0
            if 'conversation_notes' in progress_data:
                notes_data = progress_data.get('conversation_notes', {})
                # Convert string keys back to proper types if needed
                for conv_id_str, note in notes_data.items():
                    # Try to keep as string (conv_id format in Snapchat parser)
                    self.conversation_notes[conv_id_str] = note
                    loaded_notes += 1
            
            # Load reviewed conversations
            progress.setLabelText("Processing reviewed conversations...")
            progress.setValue(30)
            QApplication.processEvents()
            
            loaded_reviewed = 0
            reviewed_conversations = progress_data.get('reviewed_conversations', {})
            for case_id, conv_list in reviewed_conversations.items():
                if case_id not in self.reviewed:
                    self.reviewed[case_id] = set()
                for conv_id in conv_list:
                    self.reviewed[case_id].add(conv_id)
                    loaded_reviewed += 1
            
            # Build message lookup dictionary for efficient tag loading
            # Support both old format (just message_id) and new format (conversation_id_message_id or index)
            progress.setLabelText("Building message index...")
            progress.setValue(50)
            QApplication.processEvents()
            
            msg_id_to_message = {}
            for idx, msg in enumerate(self.all_messages):
                conv_id = str(msg.get('conversation_id', '')).strip()
                msg_id = msg.get('message_id')
                
                # Build lookup keys matching the save format
                if msg_id:
                    try:
                        msg_id_str = str(msg_id).strip()
                        if msg_id_str:
                            # Normalize message_id (matching CSV parsing: str(int(x)))
                            try:
                                normalized_msg_id = str(int(float(msg_id_str)))
                            except (ValueError, TypeError):
                                normalized_msg_id = msg_id_str
                            
                            # Index with composite key: conversation_id + message_id (new format)
                            if conv_id and normalized_msg_id:
                                composite_key = f"{conv_id}_{normalized_msg_id}"
                                msg_id_to_message[composite_key] = msg
                            
                            # Also index with just message_id (old format compatibility)
                            msg_id_to_message[normalized_msg_id] = msg
                            if msg_id_str != normalized_msg_id:
                                msg_id_to_message[msg_id_str] = msg
                    except Exception as e:
                        logger.debug(f"Error normalizing message_id {msg_id}: {e}")
                
                # Always index by position (for fallback when message_id is empty)
                msg_id_to_message[str(idx)] = msg
            
            # Apply tags to messages
            progress.setLabelText("Applying tags to messages...")
            progress.setValue(60)
            QApplication.processEvents()
            
            tagged_messages = progress_data.get('tagged_messages', {})
            total_tagged = len(tagged_messages)
            loaded_tags = 0
            not_found_ids = []
            
            for saved_id, tags_list in tagged_messages.items():
                # saved_id could be in various formats:
                # - Old format: just message_id (e.g., "1", "2", "14")
                # - New format: conversation_id_message_id (e.g., "conv123_1")
                # - Fallback: index (e.g., "0", "1", "13")
                
                saved_id_str = str(saved_id).strip()
                if not saved_id_str:
                    continue
                
                # Try to find the message
                target_msg = None
                found = False
                
                # Try direct lookup first (handles all formats)
                if saved_id_str in msg_id_to_message:
                    target_msg = msg_id_to_message[saved_id_str]
                    found = True
                else:
                    # Try parsing as composite key (conversation_id_message_id)
                    if '_' in saved_id_str:
                        parts = saved_id_str.split('_', 1)
                        if len(parts) == 2:
                            conv_part, msg_part = parts
                            # Try to normalize the message_id part
                            try:
                                normalized_msg_part = str(int(float(msg_part)))
                                composite_key = f"{conv_part}_{normalized_msg_part}"
                                if composite_key in msg_id_to_message:
                                    target_msg = msg_id_to_message[composite_key]
                                    found = True
                            except (ValueError, TypeError):
                                pass
                    
                    # Try as just message_id (old format compatibility)
                    if not found:
                        try:
                            normalized_id = str(int(float(saved_id_str)))
                            if normalized_id in msg_id_to_message:
                                target_msg = msg_id_to_message[normalized_id]
                                found = True
                        except (ValueError, TypeError):
                            pass
                
                if found and target_msg:
                    # Ensure tags is a set and merge with existing tags if any
                    if not isinstance(target_msg.get('tags'), set):
                        target_msg['tags'] = set()
                    # Add the loaded tags
                    target_msg['tags'].update(tags_list)
                    loaded_tags += 1
                    # Debug logging for first few tags
                    if loaded_tags <= 3:
                        logger.debug(f"Loaded tags for saved_id {saved_id_str}: {target_msg['tags']}")
                else:
                    not_found_ids.append(saved_id_str)
                    # Debug logging for first few not found
                    if len(not_found_ids) <= 3:
                        logger.debug(f"Could not find message with saved_id: {saved_id_str}")
                
                # Update progress every 100 messages
                if (idx + 1) % 100 == 0 or (idx + 1) == total_tagged:
                    progress_value = 60 + int(30 * (idx + 1) / total_tagged) if total_tagged > 0 else 90
                    progress.setValue(progress_value)
                    progress.setLabelText(f"Applying tags... ({loaded_tags}/{total_tagged})")
                    QApplication.processEvents()
            
            # Update conversation selector to show reviewed status
            progress.setLabelText("Updating conversation list...")
            progress.setValue(90)
            QApplication.processEvents()
            
            self.populate_selector()
            
            # Log summary for debugging
            if not_found_ids:
                logger.warning(f"Could not find {len(not_found_ids)} message IDs when loading tags")
                # Show first few for debugging
                logger.debug(f"First 5 not found IDs: {not_found_ids[:5]}")
                # Show sample of what's in the lookup
                sample_lookup_ids = list(msg_id_to_message.keys())[:5]
                logger.debug(f"Sample lookup IDs: {sample_lookup_ids}")
                # Show sample of actual message IDs from messages
                sample_msg_ids = [str(msg.get('message_id', '')) for msg in self.all_messages[:10] if msg.get('message_id')]
                logger.debug(f"Sample actual message IDs from messages: {sample_msg_ids}")
            
            # Refresh the current view to show updated tags
            progress.setLabelText("Refreshing display...")
            progress.setValue(95)
            QApplication.processEvents()
            
            # Force a full table refresh to show loaded tags
            # Clear the cache to ensure refresh happens
            if hasattr(self, '_last_conv_id_displayed'):
                self._last_conv_id_displayed = None
            if hasattr(self, '_last_filter_hash'):
                self._last_filter_hash = None
            if hasattr(self, '_last_displayed_indices'):
                self._last_displayed_indices = None
            
            # Refresh the table to show updated tags
            if hasattr(self, 'message_table') and hasattr(self, 'current_msg_indices'):
                # Get current conversation ID
                current_conv_id = self.conv_selector.currentData() if hasattr(self, 'conv_selector') and self.conv_selector else None
                
                # Refresh the table - this will repopulate it with updated tags
                try:
                    self.refresh_message_table(conv_id=current_conv_id)
                except Exception as e:
                    logger.error(f"Error refreshing table after loading tags: {e}")
                    # Fallback: manually update visible rows
                    try:
                        # For QTableView, trigger model refresh
                        model = self.message_table.model()
                        if model and model.rowCount() > 0:
                            # Emit dataChanged to trigger refresh
                            top_left = model.index(0, 0)
                            bottom_right = model.index(model.rowCount() - 1, model.columnCount() - 1)
                            model.dataChanged.emit(top_left, bottom_right)
                        self.recompute_visible_row_backgrounds()
                        self.message_table.viewport().update()
                        self.message_table.repaint()
                    except Exception as e2:
                        logger.error(f"Error in fallback table update: {e2}")
            
            progress.setValue(100)
            progress.close()
            
            self.status.showMessage(
                f"Progress loaded: {loaded_reviewed} reviewed conversations, {loaded_tags} tagged messages, {loaded_notes} notes"
            )
            
            msg = QMessageBox(self)
            if hasattr(self, 'theme_manager') and self.dark_mode:
                msg.setStyleSheet(self.theme_manager.get_dialog_stylesheet())
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("Progress Loaded")
            msg.setText(
                f"Progress loaded successfully!\n\n"
                f"Reviewed conversations restored: {loaded_reviewed}\n"
                f"Tagged messages restored: {loaded_tags}\n"
                f"Notes restored: {loaded_notes}\n\n"
                f"File: {os.path.basename(file_path)}"
            )
            msg.exec()
            logger.info(f"Progress loaded from {file_path}: {loaded_reviewed} reviewed, {loaded_tags} tagged, {loaded_notes} notes")
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON file: {str(e)}"
            logger.error(error_msg)
            msg = QMessageBox(self)
            if hasattr(self, 'theme_manager') and self.dark_mode:
                msg.setStyleSheet(self.theme_manager.get_dialog_stylesheet())
            msg.setIcon(QMessageBox.Critical)
            msg.setWindowTitle("Load Error")
            msg.setText(error_msg)
            msg.exec()
            self.status.showMessage("Error loading progress: Invalid file format")
        except Exception as e:
            error_msg = f"Error loading progress: {str(e)}"
            logger.error(error_msg)
            msg = QMessageBox(self)
            if hasattr(self, 'theme_manager') and self.dark_mode:
                msg.setStyleSheet(self.theme_manager.get_dialog_stylesheet())
            msg.setIcon(QMessageBox.Critical)
            msg.setWindowTitle("Load Error")
            msg.setText(error_msg)
            msg.exec()
            self.status.showMessage("Error loading progress")

    def check_for_updates(self):
        url = "https://api.github.com/repos/koebbe14/Snapchat-Parser/releases/latest"
        try:
            response = requests.get(url)
            if response.status_code == 404:
                QMessageBox.information(self, "Update Check", "No releases found in the repository.")
                return
            response.raise_for_status()
            data = response.json()
            latest_version = data.get('tag_name') or data.get('name')
            if latest_version:
                if latest_version != APP_VERSION:
                    QMessageBox.information(self, "Update Available", f"New version {latest_version} is available!\nCurrent version: {APP_VERSION}\nDownload from: {data['html_url']}")
                else:
                    QMessageBox.information(self, "Up to Date", "You are using the latest version.")
            else:
                QMessageBox.warning(self, "Update Check", "Could not determine latest version.")
        except requests.RequestException as e:
            QMessageBox.warning(self, "Update Check Failed", f"Error checking for updates: {str(e)}")
            
    def show_stats(self):
        conv_id = self.conv_selector.currentData()
        is_all = conv_id is None
        
        if is_all:
            msg_indices = list(range(len(self.all_messages)))
            title = "All Conversations Stats"
        else:
            msg_indices = self.conversations.get(conv_id, [])
            title = f"Conversation {conv_id} Stats"
        
        if not msg_indices:
            QMessageBox.information(self, "Stats", "No messages available.")
            return
        
        messages = [self.all_messages[i] for i in msg_indices]
        
        # Total Messages
        total_messages = len(messages)
        
        # Unique Conversations
        unique_convs = len(set(m.get('conversation_id') for m in messages if m.get('conversation_id')))
        
        # Unique Users
        users = set()
        for m in messages:
            users.add(m.get('sender_username') or m.get('sender') or '')
            users.add(m.get('recipient_username') or m.get('receiver') or '')
        users.discard('')
        unique_users = len(users)
        
        # Tagged Messages
        tagged_messages = sum(1 for m in messages if m.get('tags'))
        
        # Keyword Hits (count messages matching any keyword in all lists)
        keyword_hits = 0
        all_keywords = []
        for kw_list in self.keyword_lists.values():
            all_keywords.extend([kw[0].lower() for kw in kw_list])  # Ignore whole_word for simplicity
        all_keywords = set(all_keywords)  # Dedup
        for m in messages:
            text = str(m.get('text') or m.get('message') or '').lower()
            if any(kw in text for kw in all_keywords):
                keyword_hits += 1
        
        # Total Media
        total_media = sum(1 for m in messages if m.get('media_id') or m.get('content_id'))
        
        # Date Period
        timestamps = [m.get('timestamp') for m in messages if m.get('timestamp')]
        if timestamps:
            min_date = min(timestamps).strftime("%Y-%m-%d")
            max_date = max(timestamps).strftime("%Y-%m-%d")
            date_period = f"{min_date} to {max_date}"
        else:
            date_period = "N/A"
        
        # Display in a dialog
        dlg = QDialog(self)
        dlg.setWindowTitle(title)
        layout = QVBoxLayout(dlg)
        
        stats_text = f"""
        <b>Total Messages:</b> {total_messages}<br>
        <b>Unique Conversations:</b> {unique_convs}<br>
        <b>Unique Users:</b> {unique_users}<br>
        <b>Tagged Messages:</b> {tagged_messages}<br>
        <b>Keyword Hits:</b> {keyword_hits}<br>
        <b>Total Media:</b> {total_media}<br>
        <b>Date Period:</b> {date_period}
        """
        
        label = QLabel(stats_text)
        label.setWordWrap(True)
        layout.addWidget(label)
        
        btns = QDialogButtonBox(QDialogButtonBox.Ok)
        btns.accepted.connect(dlg.accept)
        layout.addWidget(btns)
        
        dlg.resize(400, 300)
        dlg.exec_()
        
    def copy_selected_cell(self, row, col):
        model = self.message_table.model()
        if not model:
            return

        index = model.index(row, col)
        if not index.isValid():
            return

        text = index.data(Qt.DisplayRole) or ""
        tooltip = index.data(Qt.ToolTipRole) or ""

        # Prefer tooltip if it's meaningfully different (e.g., media paths)
        if tooltip and tooltip != text:
            text = f"{text}\n{tooltip}"

        QApplication.clipboard().setText(text)
        QMessageBox.information(self, "Copied", "Cell copied to clipboard.")

       

    def copy_selected_from_table(self):
        model = self.message_table.model()
        if not model:
            QMessageBox.information(self, "Info", "No model")
            return
        
        # QTableView uses selectedIndexes() instead of selectedRanges()
        selected_indexes = self.message_table.selectedIndexes()
        if not selected_indexes:
            QMessageBox.information(self, "Info", "No selection")
            return

        # Extract unique rows from selected indexes
        rows = set()
        for index in selected_indexes:
            if index.isValid():
                rows.add(index.row())
        rows = sorted(rows)

        # Get headers from model
        headers = []
        for c in range(model.columnCount()):
            header_data = model.headerData(c, Qt.Horizontal, Qt.DisplayRole)
            headers.append(header_data if header_data else '')
        
        lines = ["\t".join(headers)]
        for r in rows:
            row_values = []
            for c in range(model.columnCount()):
                index = model.index(r, c)
                if index.isValid():
                    text = index.data(Qt.DisplayRole) or ""
                    row_values.append(str(text))
                else:
                    row_values.append("")
            lines.append("\t".join(row_values))

        QApplication.clipboard().setText("\n".join(lines))
        QMessageBox.information(self, "Copied", f"Copied {len(rows)} row(s) to clipboard")
    
    def _get_selected_message_indices(self):
        """Helper to get unique message indices from selected table rows."""
        selected_rows = self.message_table.selectionModel().selectedRows()
        if not selected_rows:
            return set()
        
        indices = set()
        for model_index in selected_rows:
            row = model_index.row()
            # Get the message index from the model's data (stored in UserRole for Message column)
            # This works even when the table is sorted, because the data is in the model
            model = self.message_table.model()
            if model:
                # Get message index from Message column's UserRole, or from model's messages_data
                try:
                    msg_col = self.headers.index("Message") if "Message" in self.headers else 0
                    index = model.index(row, msg_col)
                    msg_index = index.data(Qt.UserRole)
                    if msg_index is not None and isinstance(msg_index, int):
                        indices.add(msg_index)
                    else:
                        # Fallback: get from model's messages_data if accessible
                        if hasattr(model, 'messages_data') and row < len(model.messages_data):
                            msg_index, _, _ = model.messages_data[row]
                            if msg_index is not None:
                                indices.add(msg_index)
                except (ValueError, IndexError):
                    # Fallback: get from model's messages_data
                    if hasattr(model, 'messages_data') and row < len(model.messages_data):
                        msg_index, _, _ = model.messages_data[row]
            if msg_index is not None:
                indices.add(msg_index)
            else:
                # Fallback: if item data is not available, use the old method
                # This handles edge cases where items might not have data set
                if 0 <= row < len(self.current_msg_indices):
                    indices.add(self.current_msg_indices[row])
        return indices

    def add_tag_to_indices(self, tag, indices):
        # If this is a brand-new custom tag, assign an unused color so it’s unique.
        if tag not in self.TAG_COLORS:
            self.TAG_COLORS[tag] = self._get_unused_tag_color()

        modified = False
        for msg_index in indices:
            msg = self.all_messages[msg_index]
            tags = set(msg.get('tags', set()))
            if tag not in tags:
                tags.add(tag)
                msg['tags'] = tags
                modified = True

        if modified:
            self.available_tags.add(tag)
            self.save_config()
            # update only changed rows; schedule on the event loop to allow UI to finish selection changes
            QTimer.singleShot(0, lambda: self.update_table_rows_for_msg_indices(indices))

    def remove_tags_from_indices(self, indices):
        modified = False
        for msg_index in indices:
            msg = self.all_messages[msg_index]
            if msg.get('tags', set()):
                msg['tags'] = set()
                modified = True

        if modified:
            self.save_config()
            QTimer.singleShot(0, lambda: self.update_table_rows_for_msg_indices(indices))



    def table_ctx_menu(self, pos):
        # 1. Get current selection and index under cursor
        selected_rows = self.message_table.selectionModel().selectedRows()
        index = self.message_table.indexAt(pos)

        # If nothing selected but right-clicked on a cell → auto-select the row
        if not selected_rows and index.isValid():
            row = index.row()
            self.message_table.selectRow(row)
            selected_rows = self.message_table.selectionModel().selectedRows()

        # Still nothing? Do nothing.
        if not selected_rows:
            return

        message_indices = self._get_selected_message_indices()

        # 2. Build context menu
        menu = QMenu(self)

        # Tagging menu
        add_menu = menu.addMenu(f"Add Tag to {len(selected_rows)} Msg(s)")
        for t in sorted(self.available_tags):
            action = add_menu.addAction(t)
            action.triggered.connect(
                lambda checked, tag=t, indices=list(message_indices):
                    self.add_tag_to_indices(tag, indices)
            )

        # Remove tags
        remove_action = menu.addAction(f"Remove Tags from {len(selected_rows)} Msg(s)")
        remove_action.triggered.connect(
            lambda checked, indices=list(message_indices):
                self.remove_tags_from_indices(indices)
        )

        # Copy actions
        menu.addSeparator()
        copy_rows_action = menu.addAction("Copy Selected Rows")
        copy_cell_action = None
        if index.isValid():
            copy_cell_action = menu.addAction("Copy Selected Cell")

        # Mark reviewed
        mark_action = None
        if index.isValid():
            menu.addSeparator()
            mark_action = menu.addAction("Mark Conversation Reviewed")

        # 3. Execute menu
        action = menu.exec_(self.message_table.viewport().mapToGlobal(pos))
        
        if action == copy_rows_action:
            self.copy_selected_from_table()
            return

        if action == copy_cell_action and index.isValid():
            self.copy_selected_cell(index.row(), index.column())
            return

        if action == mark_action and index.isValid():
            row_under_cursor = index.row()
            # Get message index from model
            model = self.message_table.model()
            if model:
                msg_index = model.index(row_under_cursor, 0).data(Qt.UserRole)
                if msg_index is not None and isinstance(msg_index, int) and msg_index < len(self.all_messages):
                    msg = self.all_messages[msg_index]
                    conv_id = msg.get('conversation_id')
                    if conv_id:
                        self.toggle_reviewed(conv_id)
                        self.populate_selector()

    def on_table_cell_double_clicked(self, index):
        """Handle double-click on table cells, especially group member columns.
        
        Args:
            index: QModelIndex from QTableView.doubleClicked signal
        """
        if not index.isValid():
            return
        
        row = index.row()
        col = index.column()
        header = self.headers[col] if col < len(self.headers) else None
        
        if header == "Group Members":
            # Get full data from model's UserRole
            model = self.message_table.model()
            if model:
                full_data = index.data(Qt.UserRole)
                if full_data:
                    # Parse the combined data to extract usernames and user IDs
                    usernames = ''
                    user_ids = ''
                    if 'Usernames:' in full_data and 'User IDs:' in full_data:
                        parts = full_data.split('User IDs:')
                        usernames = parts[0].replace('Usernames:', '').strip()
                        user_ids = parts[1].strip() if len(parts) > 1 else ''
                    elif 'Usernames:' in full_data:
                        usernames = full_data.replace('Usernames:', '').strip()
                    elif 'User IDs:' in full_data:
                        user_ids = full_data.replace('User IDs:', '').strip()
                    else:
                        # Fallback: try to parse as original format
                        usernames = full_data
                    
                    dlg = GroupMembersDialog(usernames, user_ids, self)
                    dlg.exec_()
                    
                    # After dialog closes, clear selection to force proper HTML rendering
                    # The issue is that when a cell is selected, Qt may show raw HTML instead of rendering it
                    # Clearing and restoring selection forces the delegate to re-render properly
                    current_selection = self.message_table.selectionModel().selection()
                    self.message_table.clearSelection()
                    QApplication.processEvents()
                    # Restore selection if there was one
                    if current_selection:
                        self.message_table.selectionModel().select(current_selection, QItemSelectionModel.Select)
                    # Force the cell to update using the delegate
                    self.message_table.viewport().update()
        
        elif header in ["Saved By", "Screenshotted By", "Replayed By", "Read By"]:
            # Get full data from model's UserRole
            model = self.message_table.model()
            if model:
                full_data = index.data(Qt.UserRole)
                if full_data and isinstance(full_data, dict):
                    usernames = full_data.get('usernames', [])
                    user_ids = full_data.get('user_ids', [])
                    
                    # Only open dialog if there are users to display
                    if usernames or user_ids:
                        # Format usernames and user_ids as comma-separated strings
                        usernames_str = ', '.join(usernames) if usernames else ''
                        user_ids_str = ', '.join(user_ids) if user_ids else ''
                        
                        dlg = GroupMembersDialog(usernames_str, user_ids_str, self)
                        dlg.setWindowTitle(header)  # Set dialog title to column name
                        dlg.exec_()
                        
                        # After dialog closes, clear selection to force proper HTML rendering
                        current_selection = self.message_table.selectionModel().selection()
                        self.message_table.clearSelection()
                        QApplication.processEvents()
                        if current_selection:
                            self.message_table.selectionModel().select(current_selection, QItemSelectionModel.Select)
                        self.message_table.viewport().update()
        
        # Note: Media column double-click is handled by MediaThumbnailDelegate.editorEvent()
        # so we don't need to handle it here to avoid opening the file twice

    def load_config(self):
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    cfg = json.load(f)
                    self.available_tags = set(cfg.get('available_tags', list(self.TAG_COLORS.keys())))
                    self.hotkeys = cfg.get('hotkeys', {})
                    # 'reviewed' status is NOT loaded from config - only from progress JSON files
                    # Ignore any 'reviewed' key if it exists in old config files
                    self.keyword_lists = cfg.get('keyword_lists', {})
                    self.selected_keyword_list = cfg.get('selected_keyword_list')
                    self.column_order = cfg.get('column_order', self.headers[:])
                    self.hidden_columns = cfg.get('hidden_columns', [])
                    # Do NOT load active_filters from config anymore
                    # Notes are NOT stored in config - only in progress JSON files

                    logger.info("Configuration loaded successfully")
                    # Logging setting (fall back to whatever __init__ set)
                    self.logging_enabled = cfg.get('logging_enabled', self.logging_enabled)

            except Exception as e:
                logger.warning(f"load_config err: {e}")
        else:
            self.column_order = self.headers[:]
            self.hidden_columns = []

    def save_config(self):
        try:           
            cfg = {
                'available_tags': list(self.available_tags),
                'hotkeys': self.hotkeys,
                # 'reviewed' status is NOT saved to config - only to progress JSON files
                'keyword_lists': self.keyword_lists,
                'selected_keyword_list': self.selected_keyword_list,
                'column_order': self.column_order,
                'hidden_columns': self.hidden_columns,
                'logging_enabled': self.logging_enabled,
                # Notes are NOT stored in config - only in progress JSON files
            }
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(cfg, f, indent=2)
            logger.info("Configuration saved successfully")
        except Exception as e:
            logger.error(f"save_config err: {e}")

    def auto_prompt_import(self):
        # Center the window using the modern screen() method
        if self.screen():
            qr = self.frameGeometry()
            cp = self.screen().availableGeometry().center()
            qr.moveCenter(cp)
            self.move(qr.topLeft())
        self.import_zip_dialog()

    def import_zip_dialog(self):
        path, _ = QFileDialog.getOpenFileName(self, "Import Snapchat ZIP", "", "ZIP Files (*.zip)")
        if not path:
            logger.info("No ZIP file selected")
            return
        self.media_zip_path = path
        self.start_zip_loader(path)

    def start_zip_loader(self, zip_path):
        self.progress_dialog = MultiProgressDialog(self)
        self.progress_dialog.show()
        QApplication.processEvents()
        
        self.loader_thread = ZipLoaderThread(zip_path)
        self.loader_thread.finished_indexing.connect(self.process_zip_data)
        self.loader_thread.progress_update.connect(self.on_loader_progress)
        self.loader_thread.start()
    
    def on_loader_progress(self, percentage, message):
        """Handle progress updates from ZipLoaderThread - map to phases 1 and 2"""
        if not hasattr(self, 'progress_dialog') or not self.progress_dialog:
            return
        
        # Phase 1: 0-15% (ZIP Indexing)
        if percentage <= 15:
            if percentage == 0 and not hasattr(self, '_phase1_started'):
                logger.info(f"PHASE 1 START: {message}")
                self._phase1_started = True
                self._phase1_start_time = datetime.datetime.now()
            phase1_pct = int((percentage / 15) * 100) if percentage < 15 else 100
            self.progress_dialog.update_phase(1, phase1_pct, message)
            if percentage == 15:
                if hasattr(self, '_phase1_start_time'):
                    phase1_duration = (datetime.datetime.now() - self._phase1_start_time).total_seconds()
                    logger.info(f"PHASE 1 COMPLETE: Indexing complete (Duration: {phase1_duration:.2f} seconds)")
                self.progress_dialog.update_phase(1, 100, "Indexing complete")
        # Phase 2: 15-50% (CSV Parsing)
        elif percentage <= 50:
            if percentage == 15 and not hasattr(self, '_phase2_started'):
                logger.info(f"PHASE 2 START: {message}")
                self._phase2_started = True
                self._phase2_start_time = datetime.datetime.now()
            phase2_pct = int(((percentage - 15) / 35) * 100)
            self.progress_dialog.update_phase(2, phase2_pct, message)
            if percentage == 50:
                if hasattr(self, '_phase2_start_time'):
                    phase2_duration = (datetime.datetime.now() - self._phase2_start_time).total_seconds()
                    logger.info(f"PHASE 2 COMPLETE: CSV parsing complete (Duration: {phase2_duration:.2f} seconds)")
                self.progress_dialog.update_phase(2, 100, "CSV parsing complete")
        
        QApplication.processEvents()

    def process_zip_data(self, all_messages, conversations, basenames, conv_files, token_index, error_message):
        if error_message:
            if hasattr(self, 'progress_dialog') and self.progress_dialog:
                self.progress_dialog.close()
            QMessageBox.critical(self, "Import Error", error_message)
            return
        if not all_messages:
            if hasattr(self, 'progress_dialog') and self.progress_dialog:
                self.progress_dialog.close()
            QMessageBox.critical(self, "Import Error", "No messages or conversations could be loaded.")
            return
       
        # Phase 3: Post-processing loaded data
        logger.info(f"PHASE 3 START: Processing {len(all_messages):,} messages, {len(conversations)} conversations")
        phase3_start_time = datetime.datetime.now()
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.start_phase(3, f"Processing {len(all_messages):,} messages, {len(conversations)} conversations...")
            self.progress_dialog.update_phase(3, 20, f"Processing {len(all_messages):,} messages, {len(conversations)} conversations...")
            QApplication.processEvents()
       
        self.all_messages = all_messages
        self.conversations = conversations
        self.basenames = basenames
        self.conv_files = conv_files  # Store conv_files for hashing and other uses
        self.token_index = None  # OPTIMIZED: Lazy - will be built on demand
        self._token_index_built = False
        self.media_lookup_cache = {}  # Clear and reset cache for new import
        
        # Build user_id -> username mapping from all messages
        self.user_id_to_username_map = {}
        self._build_user_id_mapping(all_messages)
        
        # OPTIMIZED: Create messages_df with precomputed fields
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.update_phase(3, 65, "Creating optimized data structures...")
            QApplication.processEvents()
        
        # Build DataFrame with precomputed fields
        if all_messages:
            df_data = []
            for msg in all_messages:
                row = msg.copy()
                # Precompute derived fields
                ts = msg.get('timestamp')
                if ts:
                    row['date_str'] = ts.strftime("%Y-%m-%d") if hasattr(ts, 'strftime') else str(ts)[:10]
                    row['time_str'] = ts.strftime("%H:%M:%S") if hasattr(ts, 'strftime') else str(ts)[11:19]
                else:
                    row['date_str'] = 'N/A'
                    row['time_str'] = 'N/A'
                
                # Normalize sender/receiver
                row['sender_norm'] = str(msg.get('sender_username') or msg.get('sender') or '')
                row['receiver_norm'] = str(msg.get('recipient_username') or msg.get('receiver') or '')
                
                # Precompute search_text once
                search_cols = ['text', 'message', 'sender_username', 'recipient_username', 
                              'sender', 'receiver', 'upload_ip', 'media_id', 'content_id']
                search_parts = [str(msg.get(c, '')) for c in search_cols if msg.get(c)]
                row['search_text'] = ' '.join(search_parts).lower()
                
                df_data.append(row)
            
            self.messages_df = pd.DataFrame(df_data)
            self.messages_df['original_index'] = range(len(all_messages))  # Link back to all_messages
        else:
            self.messages_df = pd.DataFrame()

        # Update progress: Computing case ID
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.update_phase(3, 40, "Computing case identifier...")
            QApplication.processEvents()

        # OPTIMIZED: Use hash computed during loading (no re-reading)
        case_id = self.compute_conversations_hash()
        if not case_id:
            # Fallback to ZIP basename if hashing fails for some reason
            case_id = os.path.basename(self.media_zip_path)

        self.current_file_id = case_id

        if self.current_file_id not in self.reviewed:
            self.reviewed[self.current_file_id] = set()

        self.status.showMessage(
            f"Loaded {len(all_messages):,} messages from {len(conversations)} conversations."
        )
        
        # Update progress: Preparing data structures
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.update_phase(3, 60, "Preparing data structures...")
            QApplication.processEvents()

        # Reset filters for each new import
        self.active_filters = {
            'from_date': None,
            'to_date': None,
            'sender_username': None,
            'message_type': None,
            'content_type': None,
            'saved_by': None,
            'is_saved_display': 'All Messages'
        }
        self.update_filter_status_label()
        
        # Clear caches for new import
        self._cached_all_conversations_indices = None
        self._cached_filter_mask = None  # OPTIMIZED: Clear cached filter mask
        self._last_conv_id_displayed = None
        self._last_filter_hash = None
        self._cached_filter_hash = None
        self._last_displayed_indices = None
        self._token_index_built = False  # Reset token_index build flag

        # Update progress: Analyzing data
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.update_phase(3, 80, "Analyzing data for filter options...")
            QApplication.processEvents()
       
        # Analyze data for filter options
        self.unique_values = {
            'sender_username': sorted(list(set(m.get('sender_username') for m in all_messages if m.get('sender_username')))),
            'message_type': sorted(list(set(m.get('message_type') for m in all_messages if m.get('message_type')))),
            'content_type': sorted(list(set(m.get('content_type') for m in all_messages if m.get('content_type')))),
        }
       
        # Mark phase 3 as complete
        phase3_end_time = datetime.datetime.now()
        phase3_duration = (phase3_end_time - phase3_start_time).total_seconds()
        logger.info(f"PHASE 3 COMPLETE: Post-processing complete (Duration: {phase3_duration:.2f} seconds)")
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.update_phase(3, 100, "Post-processing complete")
        
        # Phase 4: UI Population - Selector
        logger.info(f"PHASE 4 START: Populating conversation selector ({len(conversations)} conversations)")
        phase4_start_time = datetime.datetime.now()
        
        # Set flag to defer media processing during Phase 4
        self._defer_media_processing = True
        
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.start_phase(4, f"Populating conversation selector ({len(conversations)} conversations)...")
            # Process events multiple times to ensure UI updates immediately
            for _ in range(10):
                QApplication.processEvents()
        
        # Allocate progress: 0-100% for selector within phase 4
        self.populate_selector(progress_dialog=self.progress_dialog, progress_start=0, progress_end=100, phase_num=4)
        
        # Clear flag after Phase 4 completes
        self._defer_media_processing = False
        
        # Mark phase 4 as complete
        phase4_end_time = datetime.datetime.now()
        phase4_duration = (phase4_end_time - phase4_start_time).total_seconds()
        logger.info(f"PHASE 4 COMPLETE: Conversation selector complete (Duration: {phase4_duration:.2f} seconds)")
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.update_phase(4, 100, "Conversation selector complete")
        
        # Phase 5: UI Population - Table
        # Ensure media processing is enabled for Phase 5
        self._defer_media_processing = False
        
        logger.info(f"PHASE 5 START: Populating message table")
        phase5_start_time = datetime.datetime.now()
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            # Process events before getting row count to ensure UI is responsive
            for _ in range(5):
                QApplication.processEvents()
            total_rows = len(self.get_filtered_messages(conv_id=None))
            self.progress_dialog.start_phase(5, f"Populating message table ({total_rows:,} rows)...")
            # Process events after starting phase to ensure UI updates
            for _ in range(10):
                QApplication.processEvents()
        
        # Pass progress to refresh_message_table (0-100% for phase 5)
        self.refresh_message_table(progress_dialog=self.progress_dialog, progress_start=0, progress_end=100, phase_num=5)
       
        # Mark phase 5 as complete and close immediately
        phase5_end_time = datetime.datetime.now()
        phase5_duration = (phase5_end_time - phase5_start_time).total_seconds()
        logger.info(f"PHASE 5 COMPLETE: Message table complete (Duration: {phase5_duration:.2f} seconds)")
        if hasattr(self, 'progress_dialog') and self.progress_dialog:
            self.progress_dialog.update_phase(5, 100, "Message table complete")
            # Process events to ensure UI updates before closing
            QApplication.processEvents()
            # Close immediately - resize operations will happen in background
            self.progress_dialog.close()
        
        # Show column width instruction dialog after data import completes
        self._show_column_width_dialog_after_import()
        
    def _build_user_id_mapping(self, all_messages):
        """
        Build a mapping of user_id -> username from all messages.
        Uses group_member_usernames and group_member_user_ids where they correspond positionally.
        """
        self.user_id_to_username_map = {}
        
        for msg in all_messages:
            usernames_str = str(msg.get('group_member_usernames', '')).strip()
            user_ids_str = str(msg.get('group_member_user_ids', '')).strip()
            
            if not usernames_str or not user_ids_str:
                continue
            
            # Parse usernames and user IDs
            if ',' in usernames_str:
                usernames = [u.strip() for u in usernames_str.split(',') if u.strip()]
            elif ';' in usernames_str:
                usernames = [u.strip() for u in usernames_str.split(';') if u.strip()]
            else:
                usernames = [usernames_str] if usernames_str else []
            
            if ',' in user_ids_str:
                user_ids = [uid.strip() for uid in user_ids_str.split(',') if uid.strip()]
            elif ';' in user_ids_str:
                user_ids = [uid.strip() for uid in user_ids_str.split(';') if uid.strip()]
            else:
                user_ids = [user_ids_str] if user_ids_str else []
            
            # Map user IDs to usernames (they correspond positionally)
            for i in range(min(len(usernames), len(user_ids))):
                user_id = user_ids[i]
                username = usernames[i]
                if user_id and username:
                    # Only add if not already mapped (first occurrence wins, or update if different)
                    if user_id not in self.user_id_to_username_map:
                        self.user_id_to_username_map[user_id] = username
                    # If already mapped but to a different username, keep the first one
                    # (or you could update - depends on preference)
    
    def compute_conversations_hash(self):
        """
        OPTIMIZED: Compute hash during ZIP loading using already-extracted CSV bytes.
        This avoids re-extracting files just for hashing.
        """
        if not hasattr(self, 'conv_files') or not self.conv_files:
            return None

        h = hashlib.sha256()
        
        # OPTIMIZED: Use get_file_bytes_from_zip instead of extracting to temp file
        for zip_with_conv, internal_conv in self.conv_files:
            try:
                raw = get_file_bytes_from_zip(zip_with_conv, internal_conv)
                if raw:
                    h.update(raw)
            except Exception as e:
                if logger.isEnabledFor(logging.WARNING):
                    logger.warning(f"Error hashing conversations.csv: {e}")

        return h.hexdigest()



    def populate_selector(self, progress_dialog=None, progress_start=0, progress_end=100, phase_num=None):
        populate_start_time = datetime.datetime.now()
        logger.info(f"POPULATE_SELECTOR START: total_convs={len(self.conversations)}")
        
        # CRITICAL: Block signals FIRST to prevent triggering refresh_message_table
        # which would process media and block progress updates
        self.conv_selector.blockSignals(True)
        self.conv_selector.setUpdatesEnabled(False)
        
        # Show progress IMMEDIATELY - before any operations
        total_convs = len(self.conversations)
        logger.info(f"POPULATE_SELECTOR: Showing 0% progress immediately")
        if progress_dialog and total_convs > 0:
            if phase_num and hasattr(progress_dialog, 'update_phase'):
                # Force multiple UI updates to ensure the 0% is visible IMMEDIATELY
                progress_dialog.update_phase(phase_num, 0, f"Preparing to sort {total_convs:,} conversations...")
                # Process events VERY aggressively to ensure UI updates are visible
                for _ in range(20):
                    QApplication.processEvents()
        
        # NOW we can safely manipulate the combo box without triggering signals
        # Fill combo with conversations; store conv_id in itemData
        self.conv_selector.clear()
        # Add the "All Conversations" entry (userData None)
        self.conv_selector.addItem("All Conversations", None)
        
        # Process events after clearing to ensure UI is responsive
        for _ in range(10):
            QApplication.processEvents()
        
        # Sort conversations - this can take time for large datasets
        # Note: We can't show progress during sorting, but we ensure UI is updated before
        logger.info(f"POPULATE_SELECTOR: Starting sort of {total_convs} conversations")
        sort_start_time = datetime.datetime.now()
        # Separate "Reported Files" from other conversations for special positioning
        all_conv_ids = list(self.conversations.keys())
        reported_files_id = '__REPORTED_FILES__' if '__REPORTED_FILES__' in all_conv_ids else None
        other_conv_ids = [cid for cid in all_conv_ids if cid != '__REPORTED_FILES__']
        sorted_conv_ids = sorted(other_conv_ids, key=lambda x: self.conversations[x][0], reverse=False)
        sort_end_time = datetime.datetime.now()
        sort_duration = (sort_end_time - sort_start_time).total_seconds()
        logger.info(f"POPULATE_SELECTOR: Sort complete (Duration: {sort_duration:.2f} seconds)")
        
        # Process events before updating progress to ensure UI is responsive
        for _ in range(10):
            QApplication.processEvents()
        
        # Update progress immediately after sorting - this is critical!
        logger.info(f"POPULATE_SELECTOR: Showing 1% progress after sorting")
        if progress_dialog and total_convs > 0:
            if phase_num and hasattr(progress_dialog, 'update_phase'):
                progress_dialog.update_phase(phase_num, 1, f"Sorting complete. Processing {total_convs:,} conversations...")
                # Process events VERY aggressively after sorting to ensure progress is visible
                for _ in range(15):
                    QApplication.processEvents()
        
        # Pre-cache reviewed status for all conversations (much faster than checking each time)
        reviewed_set = set()
        if self.current_file_id in self.reviewed:
            reviewed_set = self.reviewed[self.current_file_id]
        
        # Signals are already blocked, just ensure updates are disabled
        # (We already blocked signals and disabled updates at the start)
        
        # Step 1: Build the list of items to add (1-40% of progress) - OPTIMIZED
        logger.info(f"POPULATE_SELECTOR: Starting Step 1 - Building list of items (1-40%)")
        step1_start_time = datetime.datetime.now()
        items_to_add = []
        # Track last reported percentage to ensure we update at least every 1%
        last_reported_pct = 0  # Start at 0 to ensure first update happens
        
        # Update at the very start of processing
        if progress_dialog and total_convs > 0:
            if phase_num and hasattr(progress_dialog, 'update_phase'):
                progress_dialog.update_phase(phase_num, 1, f"Starting to process {total_convs:,} conversations...")
                for _ in range(3):
                    QApplication.processEvents()
        
        for conv_idx, conv_id in enumerate(sorted_conv_ids):
            # Calculate progress percentage based on current position
            if progress_dialog and total_convs > 0:
                # First 40% of progress is for building the list (1-40% overall)
                # Use floating point for more accurate calculation
                progress_ratio = (conv_idx + 1) / total_convs if total_convs > 0 else 0
                list_build_pct = 1 + int(progress_ratio * 39)  # 1% to 40%
                
                # Update if we've progressed at least 1% OR every 5 items OR at key milestones
                should_update = (
                    conv_idx == 0 or  # Always at start
                    list_build_pct > last_reported_pct + 0.5 or  # At least 0.5% progress (more frequent)
                    conv_idx % 5 == 0 or  # Every 5 items minimum (more frequent)
                    conv_idx == total_convs // 10 or  # 10% milestone
                    conv_idx == total_convs // 4 or  # 25% milestone
                    conv_idx == total_convs // 2 or  # 50% milestone
                    conv_idx == (total_convs * 3) // 4 or  # 75% milestone
                    conv_idx == total_convs - 1  # Always at end
                )
                
                if should_update:
                    last_reported_pct = list_build_pct
                    message = f"Processing conversations: {conv_idx + 1:,}/{total_convs:,}"
                    logger.debug(f"POPULATE_SELECTOR Step 1: Updating progress to {list_build_pct}% (conv_idx={conv_idx}, total={total_convs})")
                    if phase_num and hasattr(progress_dialog, 'update_phase'):
                        progress_dialog.update_phase(phase_num, list_build_pct, message)
                        # Process events multiple times to ensure UI updates
                        for _ in range(3):
                            QApplication.processEvents()
            
            # Fast reviewed check using pre-cached set
            is_reviewed = conv_id in reviewed_set

            # Special handling for "Reported Files" conversation
            if conv_id == '__REPORTED_FILES__':
                name = 'Reported Files'
            else:
                # Optimize: only access message data once, and cache the latest message index
                latest_msg_index = self.conversations[conv_id][-1]
                latest_msg = self.all_messages[latest_msg_index]
                
                # Optimize: faster participant name generation (avoid sorting when not needed)
                sender = latest_msg.get('sender_username') or latest_msg.get('sender') or 'Unknown'
                receiver = latest_msg.get('recipient_username') or latest_msg.get('receiver') or 'Unknown'
                
                # Build name more efficiently - only sort if both are different and not Unknown
                if sender == receiver or (sender == 'Unknown' and receiver == 'Unknown'):
                    name = sender if sender != 'Unknown' else 'Unknown'
                elif sender == 'Unknown':
                    name = receiver
                elif receiver == 'Unknown':
                    name = sender
                else:
                    # Only sort when we have two different non-Unknown participants
                    name = ", ".join(sorted([sender, receiver]))

            # Plain text — NO HTML
            display_name = f"{name} (Reviewed)" if is_reviewed else name
            items_to_add.append((display_name, conv_id, is_reviewed))
        
        step1_end_time = datetime.datetime.now()
        step1_duration = (step1_end_time - step1_start_time).total_seconds()
        logger.info(f"POPULATE_SELECTOR Step 1 COMPLETE: Built {len(items_to_add)} items (Duration: {step1_duration:.2f} seconds)")
        
        # Step 2: Batch add items to combo box (40-90% of progress) - OPTIMIZED
        logger.info(f"POPULATE_SELECTOR: Starting Step 2 - Adding items to combo box (40-90%)")
        step2_start_time = datetime.datetime.now()
        total_items = len(items_to_add)
        reviewed_indices = []  # Track which indices need styling
        # Track last reported percentage to ensure we update at least every 1%
        last_reported_pct = 40  # Start at 40%
        
        # Update at the start of Step 2
        if progress_dialog and total_items > 0:
            if phase_num and hasattr(progress_dialog, 'update_phase'):
                progress_dialog.update_phase(phase_num, 40, f"Adding {total_items:,} items to selector...")
                for _ in range(3):
                    QApplication.processEvents()
        
        # Add "Reported Files" right after "All Conversations" if it exists
        reported_files_item = None
        if reported_files_id:
            # Check if it was in items_to_add and remove it (we'll add it separately)
            for i, (display_name, conv_id, is_reviewed) in enumerate(items_to_add):
                if conv_id == '__REPORTED_FILES__':
                    reported_files_item = items_to_add.pop(i)
                    total_items = len(items_to_add)  # Update count after removal
                    break
            # If not found in items_to_add, build it manually
            if reported_files_item is None:
                is_reviewed = reported_files_id in reviewed_set
                reported_files_item = ('Reported Files', reported_files_id, is_reviewed)
        
        # Add "Reported Files" right after "All Conversations"
        if reported_files_item:
            display_name, conv_id, is_reviewed = reported_files_item
            self.conv_selector.addItem(display_name, conv_id)
            if is_reviewed:
                reviewed_indices.append(self.conv_selector.count() - 1)
        
        for item_idx, (display_name, conv_id, is_reviewed) in enumerate(items_to_add):
            self.conv_selector.addItem(display_name, conv_id)
            
            # Track reviewed items for batch styling
            if is_reviewed:
                reviewed_indices.append(self.conv_selector.count() - 1)
            
            # Update progress VERY frequently
            if progress_dialog and total_items > 0:
                # 40-90% of progress is for adding items
                # Use floating point for more accurate calculation
                progress_ratio = (item_idx + 1) / total_items if total_items > 0 else 0
                item_add_pct = 40 + int(progress_ratio * 50)  # 40% to 90%
                
                # Update if we've progressed at least 0.5% OR every 5 items OR at key milestones
                should_update = (
                    item_idx == 0 or  # Always at start
                    item_add_pct > last_reported_pct + 0.5 or  # At least 0.5% progress (more frequent)
                    item_idx % 5 == 0 or  # Every 5 items minimum (more frequent)
                    item_idx == total_items // 10 or  # 10% milestone
                    item_idx == total_items // 4 or  # 25% milestone
                    item_idx == total_items // 2 or  # 50% milestone
                    item_idx == (total_items * 3) // 4 or  # 75% milestone
                    item_idx == total_items - 1  # Always at end
                )
                
                if should_update:
                    last_reported_pct = item_add_pct
                    message = f"Adding to selector: {item_idx + 1:,}/{total_items:,}"
                    logger.debug(f"POPULATE_SELECTOR Step 2: Updating progress to {item_add_pct}% (item_idx={item_idx}, total={total_items})")
                    if phase_num and hasattr(progress_dialog, 'update_phase'):
                        progress_dialog.update_phase(phase_num, item_add_pct, message)
                        # Process events multiple times to ensure UI updates
                        for _ in range(3):
                            QApplication.processEvents()
        
        step2_end_time = datetime.datetime.now()
        step2_duration = (step2_end_time - step2_start_time).total_seconds()
        logger.info(f"POPULATE_SELECTOR Step 2 COMPLETE: Added {total_items} items to selector (Duration: {step2_duration:.2f} seconds)")
        
        # Step 3: Apply styling to reviewed items in batch (90-98% of progress) - OPTIMIZED
        logger.info(f"POPULATE_SELECTOR: Starting Step 3 - Applying styling (90-98%)")
        step3_start_time = datetime.datetime.now()
        total_reviewed = len(reviewed_indices)
        if reviewed_indices and progress_dialog:
            if phase_num and hasattr(progress_dialog, 'update_phase'):
                progress_dialog.update_phase(phase_num, 90, f"Applying styling to {total_reviewed:,} reviewed conversations...")
                QApplication.processEvents()
        
        # Update progress during styling if there are many reviewed items
        style_update_interval = max(1, min(50, total_reviewed // 20))  # Update every 5% or every 50 items
        for style_idx, idx in enumerate(reviewed_indices):
            item = self.conv_selector.model().item(idx)
            if item:  # Safety check
                item.setForeground(Qt.red)
                font = item.font()
                font.setBold(True)
                item.setFont(font)
            
            # Update progress during styling for large sets
            if progress_dialog and total_reviewed > 50 and (style_idx % style_update_interval == 0 or style_idx == total_reviewed - 1):
                style_pct = min(98, 90 + int((style_idx / total_reviewed) * 8)) if total_reviewed > 0 else 90
                if phase_num and hasattr(progress_dialog, 'update_phase'):
                    progress_dialog.update_phase(phase_num, style_pct, f"Styling reviewed conversations: {style_idx + 1:,}/{total_reviewed:,}")
                    QApplication.processEvents()
        
        step3_end_time = datetime.datetime.now()
        step3_duration = (step3_end_time - step3_start_time).total_seconds()
        logger.info(f"POPULATE_SELECTOR Step 3 COMPLETE: Styled {total_reviewed} reviewed items (Duration: {step3_duration:.2f} seconds)")
        
        # Re-enable updates and signals
        self.conv_selector.blockSignals(False)
        self.conv_selector.setUpdatesEnabled(True)
        
        # Final progress update - ensure we show 100% when complete
        if progress_dialog and total_convs > 0:
            message = f"Conversation selector complete: {total_convs:,} conversations"
            if phase_num and hasattr(progress_dialog, 'update_phase'):
                progress_dialog.update_phase(phase_num, 100, message)
            QApplication.processEvents()

        populate_end_time = datetime.datetime.now()
        populate_duration = (populate_end_time - populate_start_time).total_seconds()
        logger.info(f"POPULATE_SELECTOR COMPLETE: Total duration: {populate_duration:.2f} seconds (Sort: {sort_duration:.2f}s, Step1: {step1_duration:.2f}s, Step2: {step2_duration:.2f}s, Step3: {step3_duration:.2f}s)")

        # Select first entry
        self.conv_selector.setCurrentIndex(0)
        self.review_btn.setEnabled(False)


    def on_conv_selected_combobox(self, index):
        # Skip if we're in Phase 4 (populating selector) to avoid blocking progress
        if getattr(self, '_defer_media_processing', False):
            logger.debug("Skipping on_conv_selected_combobox during Phase 4 to avoid blocking progress")
            return
        
        # Combo stores conversation id as userData
        if index < 0:
            return

        conv_id = self.conv_selector.itemData(index)

        # Refresh messages with status indicator
        self.refresh_message_table_with_status(
            conv_id,
            "Loading conversation messages...",
            "Conversation loaded."
        )

        # Update review button + combo box appearance
        if conv_id:
            is_reviewed = (
                self.current_file_id in self.reviewed and
                conv_id in self.reviewed[self.current_file_id]
            )

            # Update button text/state
            self.review_btn.setText(f"Mark {'Unreviewed' if is_reviewed else 'Reviewed'}")
            self.review_btn.setEnabled(True)

            # Update the combo box *displayed* text style (collapsed view)
            if is_reviewed:
                # Red + bold for reviewed conversations
                self.conv_selector.setStyleSheet(
                    "QComboBox { color: red; font-weight: bold; }"
                )
            else:
                # Normal style for unreviewed conversations
                self.conv_selector.setStyleSheet("")
        else:
            # "All Conversations" selected
            self.review_btn.setText("Mark As Reviewed")
            self.review_btn.setEnabled(False)
            # Reset combo style for the "All Conversations" entry
            self.conv_selector.setStyleSheet("")

   
    def mark_current_reviewed(self):
        conv_id = self.conv_selector.currentData()
        if not conv_id:
            # Nothing to mark for "All Conversations"
            return

        self.start_long_operation("Updating reviewed status and refreshing view...")
        try:
            # Check current status before toggle
            is_reviewed = (
                self.current_file_id in self.reviewed and
                conv_id in self.reviewed[self.current_file_id]
            )
            advance = not is_reviewed  # Advance only if we are marking as reviewed (was unreviewed)

            # Toggle reviewed state
            self.toggle_reviewed(conv_id)

            # --- IMPORTANT PART: block signals while rebuilding the selector ---
            self.conv_selector.blockSignals(True)
            self.populate_selector()
            # Find the index of this conv_id in the newly populated list
            current_index = -1
            for i in range(self.conv_selector.count()):
                if self.conv_selector.itemData(i) == conv_id:
                    current_index = i
                    break

            # Decide which index we ultimately want selected
            target_index = current_index
            if current_index != -1 and advance:
                # Advance to next if not the last conversation
                next_index = current_index + 1
                if next_index < self.conv_selector.count():
                    target_index = next_index

            # Re-enable signals before changing the index, so the usual
            # on_conv_selected_combobox logic (refresh, styling, status) runs once.
            self.conv_selector.blockSignals(False)

            if target_index is not None and target_index >= 0:
                self.conv_selector.setCurrentIndex(target_index)
                # setCurrentIndex will trigger on_conv_selected_combobox

        finally:
            self.end_long_operation("Review status updated.")


    def toggle_reviewed(self, conv_id):
        if not self.current_file_id: return
        
        if conv_id in self.reviewed[self.current_file_id]:
            self.reviewed[self.current_file_id].remove(conv_id)
            logger.info(f"Conversation {conv_id} marked UNREVIEWED.")
        else:
            self.reviewed[self.current_file_id].add(conv_id)
            logger.info(f"Conversation {conv_id} marked REVIEWED.")
            
        self.save_config()

    def _ensure_token_index(self):
        """OPTIMIZED: Build token_index lazily on first media lookup"""
        if not self._token_index_built and self.media_zip_path:
            _, _, _, self.token_index = build_media_index(self.media_zip_path, build_token_index=True)
            self._token_index_built = True
    
    def get_filtered_messages(self, conv_id=None, apply_filters=True, query_params=None):
        if conv_id == None: conv_id = None
        
        # OPTIMIZED: Use messages_df instead of rebuilding DataFrame
        if self.messages_df is None or self.messages_df.empty:
            return []
        
        # Get initial message indices
        if conv_id and conv_id in self.conversations:
            message_indices = self.conversations[conv_id]
        elif conv_id is None:
            message_indices = list(range(len(self.all_messages)))
        else:
            return []
        
        if not message_indices: return []
        
        # OPTIMIZED: Use boolean mask on messages_df instead of rebuilding DataFrame
        if apply_filters:
            # Start with boolean mask for all messages
            mask = pd.Series(False, index=self.messages_df.index)
            mask.iloc[message_indices] = True
            
            # Apply filters using vectorized operations on messages_df
            if self.active_filters['from_date'] is not None:
                mask = mask & (self.messages_df['timestamp'] >= self.active_filters['from_date'])
            if self.active_filters['to_date'] is not None:
                mask = mask & (self.messages_df['timestamp'] < self.active_filters['to_date'])
            
            for key in ['sender_username', 'message_type', 'content_type', 'saved_by']:
                val = self.active_filters.get(key)
                if val is not None and key in self.messages_df.columns:
                    mask = mask & (self.messages_df[key] == val)
            
            # OPTIMIZED: Use precomputed search_text field
            if query_params and query_params.get('query'):
                query = query_params['query'].lower()
                exact_match = query_params.get('exact_match', False)
                if exact_match:
                    pat = r'\b' + re.escape(query) + r'\b'
                    mask = mask & self.messages_df['search_text'].str.contains(pat, na=False)
                else:
                    mask = mask & self.messages_df['search_text'].str.contains(re.escape(query), na=False)
            
            # Keyword list filter
            kw_list_name = query_params.get('keyword_list') if query_params else None
            if kw_list_name and kw_list_name in self.keyword_lists:
                keywords = self.keyword_lists[kw_list_name]
                if keywords:
                    kw_mask = pd.Series(False, index=self.messages_df.index)
                    for keyword, whole_word in keywords:
                        k = keyword.lower()
                        if not k:
                            continue
                        if whole_word:
                            kw_mask = kw_mask | self.messages_df['search_text'].str.contains(r'\b' + re.escape(k) + r'\b', na=False)
                        else:
                            kw_mask = kw_mask | self.messages_df['search_text'].str.contains(re.escape(k), na=False)
                    mask = mask & kw_mask
            
            # Get filtered indices and sort by timestamp (oldest to newest - chronological order)
            if 'timestamp' in self.messages_df.columns:
                # Use the existing mask to get sorted indices
                filtered_df = self.messages_df[mask].sort_values(by='timestamp', ascending=True)
                filtered_indices = filtered_df['original_index'].tolist()
            else:
                filtered_indices = self.messages_df[mask]['original_index'].tolist()
        else:
            filtered_indices = message_indices
            # Sort by timestamp (oldest to newest - chronological order)
            if filtered_indices and 'timestamp' in self.messages_df.columns:
                # Filter by original_index and sort
                filtered_df = self.messages_df[self.messages_df['original_index'].isin(filtered_indices)]
                filtered_df_sorted = filtered_df.sort_values(by='timestamp', ascending=True)
                filtered_indices = filtered_df_sorted['original_index'].tolist()
            elif filtered_indices:
                # Fallback: sort by index if timestamp not available
                filtered_indices = sorted(filtered_indices)

        return filtered_indices

    def refresh_message_table(self, conv_id=None, progress_dialog=None, progress_start=0, progress_end=100, phase_num=None):
        if conv_id is None:
            conv_id = self.conv_selector.currentData() if self.conv_selector.currentData() else None
        
        # Compute current filter hash
        import hashlib
        filter_str = str(sorted(self.active_filters.items()))
        current_filter_hash = hashlib.md5(filter_str.encode()).hexdigest()
        
        # Check if we're switching to the same conversation with same filters - skip refresh if nothing changed
        # BUT: Always refresh if blur state might have changed (blur_all is not part of filter hash)
        # We'll check blur state separately - if blur changed, we need to refresh
        if conv_id == self._last_conv_id_displayed and current_filter_hash == self._last_filter_hash:
            # Check if blur state has changed by comparing with a stored blur state
            last_blur_state = getattr(self, '_last_blur_state', None)
            if last_blur_state == self.blur_all:
                # Same conversation, same filters, same blur state - no refresh needed
                return
            # Blur state changed - need to refresh
            self._last_blur_state = self.blur_all
        
        # Update tracking variables
        self._last_filter_hash = current_filter_hash
        self._last_conv_id_displayed = conv_id
        self._last_blur_state = self.blur_all  # Track blur state for cache invalidation
        
        # Use cached indices for "All Conversations" if available and filters haven't changed
        use_cached_indices = False
        if conv_id is None:
            # Check if we can use cached "All Conversations" indices
            cached_filter_hash = getattr(self, '_cached_filter_hash', None)
            if (self._cached_all_conversations_indices is not None and 
                cached_filter_hash == current_filter_hash):
                self.current_msg_indices = self._cached_all_conversations_indices
                use_cached_indices = True
            else:
                self.current_msg_indices = self.get_filtered_messages(conv_id=conv_id)
                self._cached_all_conversations_indices = self.current_msg_indices
                self._cached_filter_hash = current_filter_hash
        else:
            self.current_msg_indices = self.get_filtered_messages(conv_id=conv_id)
            # Clear cache when viewing specific conversation
            self._cached_all_conversations_indices = None
            self._cached_filter_hash = None
            # Clear displayed indices cache when switching to specific conversation
            self._last_displayed_indices = None
        
        # Check if indices are the same as what's currently displayed - skip repopulation if so
        # Quick check: same length and same object reference (for cached case) or same content
        if (use_cached_indices and 
            hasattr(self, '_last_displayed_indices') and 
            self._last_displayed_indices is not None and
            len(self._last_displayed_indices) == len(self.current_msg_indices)):
            # If same object reference, definitely skip
            if self._last_displayed_indices is self.current_msg_indices:
                return
            # Otherwise check content (but only if lengths match for performance)
            if self._last_displayed_indices == self.current_msg_indices:
                return
        
        # Store current indices for next comparison (store reference, not copy, for performance)
        self._last_displayed_indices = self.current_msg_indices
        
        # Pass progress to populate_message_table
        self.populate_message_table(self.all_messages, {conv_id: self.current_msg_indices}, conv_id,
                                    progress_dialog=progress_dialog, progress_start=progress_start, progress_end=progress_end, phase_num=phase_num)
        self.recompute_visible_row_backgrounds()


    def populate_message_table(self, all_messages, conversations, conv_id,
                               progress_dialog=None, progress_start=0, progress_end=100, phase_num=None):
        # Check cache first - use cached data if available
        import hashlib
        filter_str = str(sorted(self.active_filters.items()))
        current_filter_hash = hashlib.md5(filter_str.encode()).hexdigest()
        cache_key = (conv_id, current_filter_hash, self.blur_all)
        
        # Check if we have cached data for this exact state
        if cache_key in self._conversation_cache:
            cached_data = self._conversation_cache[cache_key]
            # Update model with cached data - instant!
            self.message_model.setMessages(
                cached_data['messages_data'],
                cached_data['headers'],
                self.compute_row_color,
                self.get_media_path,
                self.user_id_to_username_map,
                self.dark_mode,
                self.theme_manager,
                self.all_messages,
                self.messages_df
            )
            # Reapply default column widths (don't auto-size, use defaults)
            QTimer.singleShot(50, lambda: self._reapply_default_column_widths())
            return
        
        # If there's nothing to show, bail out early
        if not self.current_msg_indices:
            self.message_model.setMessages([], self.headers, self.compute_row_color, 
                                         self.get_media_path, self.user_id_to_username_map,
                                         self.dark_mode, self.theme_manager, self.all_messages, self.messages_df)
            return

        total_rows = len(self.current_msg_indices)
        if total_rows == 0:
            self.message_model.setMessages([], self.headers, self.compute_row_color,
                                         self.get_media_path, self.user_id_to_username_map,
                                         self.dark_mode, self.theme_manager, self.all_messages, self.messages_df)
            return

        # Prepare messages_data list for the model
        messages_data = []
        progress_range = progress_end - progress_start
        increment_per_row = progress_range / total_rows if total_rows > 0 else 0

        # Initial progress update
        if progress_dialog:
            if conv_id:
                message = f"Preparing table: {total_rows:,} messages in conversation..."
            else:
                message = f"Preparing table: {total_rows:,} messages (all conversations)..."
            
            if phase_num and hasattr(progress_dialog, 'update_phase'):
                progress_dialog.update_phase(phase_num, int(progress_start), message)
            else:
                if hasattr(progress_dialog, 'setValue'):
                    progress_dialog.setValue(int(progress_start))
                if hasattr(progress_dialog, 'setLabelText'):
                    progress_dialog.setLabelText(message)
            QApplication.processEvents()

        # Build messages_data list - this is much faster than creating table items
        for r, msg_index in enumerate(self.current_msg_indices):
            msg = self.all_messages[msg_index]
            messages_data.append((msg_index, msg, conv_id))
            
            # Update progress periodically
            if progress_dialog and (r % 100 == 0 or r == 0 or r == total_rows - 1):
                current_progress = progress_start + (increment_per_row * r)
                pct = int((r / total_rows) * 100) if total_rows > 0 else 0
                if conv_id:
                    message = f"Preparing messages: {r+1:,}/{total_rows:,} ({pct}%)"
                else:
                    message = f"Preparing messages: {r+1:,}/{total_rows:,} ({pct}%)"
                
                if phase_num and hasattr(progress_dialog, 'update_phase'):
                    progress_dialog.update_phase(phase_num, int(min(current_progress, progress_end)), message)
                else:
                    if hasattr(progress_dialog, 'setValue'):
                        progress_dialog.setValue(int(min(current_progress, progress_end)))
                    if hasattr(progress_dialog, 'setLabelText'):
                        progress_dialog.setLabelText(message)
                QApplication.processEvents()

        # Cache the prepared data for instant switching later
        self._conversation_cache[cache_key] = {
            'messages_data': messages_data,
            'headers': self.headers
        }
        
        # Limit cache size to prevent memory issues (keep last 10 conversations)
        if len(self._conversation_cache) > 10:
            # Remove oldest entry (simple FIFO - in practice you might want LRU)
            oldest_key = next(iter(self._conversation_cache))
            del self._conversation_cache[oldest_key]
        
        # Update model with new data - this is instant with virtual scrolling!
        self.message_model.setMessages(
            messages_data,
            self.headers,
            self.compute_row_color,
            self.get_media_path,
            self.user_id_to_username_map,
            self.dark_mode,
            self.theme_manager,
            self.all_messages,
            self.messages_df
        )
        
        # Final progress update
        if progress_dialog:
            if phase_num and hasattr(progress_dialog, 'update_phase'):
                progress_dialog.update_phase(phase_num, int(progress_end), "Table ready")
            else:
                if hasattr(progress_dialog, 'setValue'):
                    progress_dialog.setValue(int(progress_end))
        
        # Reapply default column widths after a short delay (non-blocking).
        # Row heights already use the default section size configured in
        # configure_table_optimal_sizing, so we avoid an expensive pass over
        # every row here to keep the UI responsive after import.
        QTimer.singleShot(100, self._reapply_default_column_widths)

        
    def adjust_row_heights(self):
        """Recalculate and adjust row heights so text wraps properly when columns resize."""
        # With QTableView and virtual scrolling, row heights are handled automatically
        # This method is kept for compatibility but doesn't need to do anything
        pass
    
    def _show_column_width_dialog_after_import(self):
        """Show column width instruction dialog after data import completes."""
        # Check if user has chosen to not show this dialog again
        self.settings.beginGroup("FirstRun")
        dont_show_again = self.settings.value("column_width_dialog_dont_show", False, type=bool)
        self.settings.endGroup()
        
        if not dont_show_again:
            # Show the dialog
            dialog = FirstRunColumnWidthDialog(self)
            dialog.exec_()
            
            # Save the user's preference based on checkbox
            self.settings.beginGroup("FirstRun")
            if not dialog.should_show_again():
                self.settings.setValue("column_width_dialog_dont_show", True)
            self.settings.endGroup()
            self.settings.sync()
    
    def _reapply_default_column_widths(self):
        """Reapply default column widths, respecting saved user preferences."""
        if not self.message_table or not self.headers:
            return
        
        # Define optimal default widths (same as in configure_table_optimal_sizing)
        default_widths = {
            "Conversation ID": 540,
            "Conversation Title": 200,
            "Message ID": 100,
            "Reply To": 100,
            "Content Type": 330,
            "Message Type": 240,
            "Date": 180,
            "Time": 140,
            "Sender": 280,
            "Receiver": 280,
            "Message": 740,
            "Media ID": 750,
            "Media": 180,
            "Tags": 160,
            "One-on-One?": 100,
            "Reactions": 330,
            "Saved By": 280,
            "Screenshotted By": 280,
            "Replayed By": 280,
            "Screen Recorded By": 280,
            "Read By": 280,
            "IP": 150,
            "Port": 100,
            "Source": 400,
            "Line Number": 100,
            "Group Members": 280,
        }
        
        # Load saved column widths from QSettings first (if available)
        saved_widths = {}
        if self.settings:
            self.settings.beginGroup("TableColumnWidths_main_table")
            for i, header_name in enumerate(self.headers):
                if i >= self.message_table.model().columnCount():
                    continue
                saved_width = self.settings.value(header_name, None)
                if saved_width is not None:
                    try:
                        width = int(saved_width)
                        width = max(50, min(2000, width))  # Reasonable constraints
                        saved_widths[i] = width
                    except (ValueError, TypeError):
                        pass
            self.settings.endGroup()
        
        # Apply column widths: saved widths > defaults > current width
        for i, header_name in enumerate(self.headers):
            if i >= self.message_table.model().columnCount():
                continue
            
            # Priority: 1) Saved width, 2) Default width, 3) Keep current width
            if i in saved_widths:
                target_width = saved_widths[i]
            elif header_name in default_widths:
                target_width = default_widths[header_name]
            else:
                # Keep current width if no default specified
                target_width = self.message_table.columnWidth(i)
                # Apply reasonable constraints
                if target_width < 50:
                    target_width = 50
                elif target_width > 1000:
                    target_width = 1000
            
            self.message_table.setColumnWidth(i, target_width)
    
    def _resize_rows_with_thumbnails(self):
        """Set appropriate row heights for rows with thumbnails."""
        model = self.message_table.model()
        if not model:
            return
        
        # Find Media column index
        try:
            media_col = self.headers.index("Media")
        except ValueError:
            return
        
        # Set row height for all rows that have media thumbnails
        for row in range(model.rowCount()):
            # Check if this row has media
            media_index = model.index(row, media_col)
            if media_index.isValid():
                media_info = model.data(media_index, Qt.UserRole)
                if media_info and isinstance(media_info, dict):
                    content_path = media_info.get('content_path', '')
                    if content_path and os.path.exists(content_path):
                        ext = os.path.splitext(content_path)[1].lower()
                        is_video = ext in ['.mp4', '.webm', '.ogg', '.mov', '.avi']
                        is_image = ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.heic', '.heif']
                        if is_video or is_image:
                            # Set row height to accommodate thumbnail (THUMBNAIL_SIZE[1] = 100px) + label (25px)
                            # Use consistent height for all media rows
                            self.message_table.setRowHeight(row, THUMBNAIL_SIZE[1] + 25)
                            continue
            
            # For non-media rows, set consistent height to match media rows
            # This prevents overlapping and ensures uniform row heights
            self.message_table.setRowHeight(row, THUMBNAIL_SIZE[1] + 25)


    def bind_hotkeys(self):
        # Clear existing shortcuts to rebind new ones
        for shortcut in self.findChildren(QShortcut):
            # Deleting the object is sufficient to clear the hotkey binding and disconnect signals
            shortcut.deleteLater()
            
        # Register custom tag hotkeys
        for tag, key_seq_str in self.hotkeys.items():
            if key_seq_str:
                shortcut = QShortcut(QKeySequence(key_seq_str), self)
                shortcut.activated.connect(lambda t=tag: self.add_tag_to_selection(t))

                    
    def add_tag_to_selection(self, tag):
        indices = self._get_selected_message_indices()
        if not indices:
            return
        self.add_tag_to_indices(tag, indices)

    def toggle_blur(self):
        self.blur_all = not self.blur_all
        style = QApplication.instance().style()
        self.blur_btn.setText(f"Blur Media: {'On' if self.blur_all else 'Off'}")
        self.blur_btn.setIcon(style.standardIcon(QStyle.SP_ArrowUp) if self.blur_all else style.standardIcon(QStyle.SP_ArrowDown))
        
        # Clear cache to force refresh when blur state changes
        if hasattr(self, '_last_conv_id_displayed'):
            self._last_conv_id_displayed = None
        if hasattr(self, '_last_filter_hash'):
            self._last_filter_hash = None
        if hasattr(self, '_last_displayed_indices'):
            self._last_displayed_indices = None
        if hasattr(self, '_last_blur_state'):
            self._last_blur_state = None  # Force refresh by clearing blur state cache
        # Clear conversation cache when blur state changes
        self._conversation_cache.clear()
        
        self.refresh_message_table_with_status(
            None,
            "Refreshing messages with new blur setting...",
            "Messages refreshed."
        )

    def show_tagged(self):
        # Filter all messages for tagged ones
        tagged_indices = [i for i, msg in enumerate(self.all_messages) if msg.get('tags')]
        
        if not tagged_indices:
            QMessageBox.information(self, "Tags", "No messages have been tagged yet.")
            return

        # Use MessageViewerDialog to display the subset
        dlg = MessageViewerDialog(tagged_indices, self.all_messages, self.basenames, 
                                  self.media_extract_dir, self.thumb_dir, self.blur_all, parent=self)
        dlg.exec_()
    

    def bind_hotkeys(self):
        # Clear existing shortcuts to rebind new ones
        for shortcut in self.findChildren(QShortcut):
            # Deleting the object is sufficient to clear the hotkey binding and disconnect signals
            shortcut.deleteLater()
            
        # Register custom tag hotkeys
        for tag, key_seq_str in self.hotkeys.items():
            if key_seq_str:
                shortcut = QShortcut(QKeySequence(key_seq_str), self)
                shortcut.activated.connect(lambda t=tag: self.add_tag_to_selection(t))

                    
    def add_tag_to_selection(self, tag):
        indices = self._get_selected_message_indices()
        if not indices:
            return
        self.add_tag_to_indices(tag, indices)

    def toggle_blur(self):
        self.blur_all = not self.blur_all
        style = QApplication.instance().style()
        self.blur_btn.setText(f"Blur Media: {'On' if self.blur_all else 'Off'}")
        self.blur_btn.setIcon(style.standardIcon(QStyle.SP_ArrowUp) if self.blur_all else style.standardIcon(QStyle.SP_ArrowDown))
        
        # Clear cache to force refresh when blur state changes
        if hasattr(self, '_last_conv_id_displayed'):
            self._last_conv_id_displayed = None
        if hasattr(self, '_last_filter_hash'):
            self._last_filter_hash = None
        if hasattr(self, '_last_displayed_indices'):
            self._last_displayed_indices = None
        if hasattr(self, '_last_blur_state'):
            self._last_blur_state = None  # Force refresh by clearing blur state cache
        
        self.refresh_message_table_with_status(
            None,
            "Refreshing messages with new blur setting...",
            "Messages refreshed."
        )
    
    def add_note_to_conversation(self):
        """Open dialog to add/edit note for the selected conversation."""
        current_item = self.conv_selector.currentData()
        if current_item is None:
            QMessageBox.information(self, "No Selection", "Please select a conversation first.")
            return
        
        conv_id = current_item
        if conv_id not in self.conversations:
            QMessageBox.information(self, "Invalid Selection", "Please select a valid conversation.")
            return
        
        # Get conversation title for display
        conv_title = ""
        if conv_id and len(self.conversations.get(conv_id, [])) > 0:
            first_msg_idx = self.conversations[conv_id][0]
            if first_msg_idx < len(self.all_messages):
                first_msg = self.all_messages[first_msg_idx]
                conv_title = first_msg.get('conversation_title', conv_id)
        
        # Get current note
        current_note = self.conversation_notes.get(conv_id, "")
        
        # Create dialog
        dialog = QDialog(self)
        dialog.setWindowTitle("Add Note to Conversation")
        dialog.setWindowFlags(dialog.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        layout = QVBoxLayout(dialog)
        
        # Conversation info
        info_label = QLabel(f"<b>Conversation:</b> {conv_title or conv_id}")
        layout.addWidget(info_label)
        
        # Notes text area
        notes_label = QLabel("Notes:")
        layout.addWidget(notes_label)
        notes_text = QTextEdit()
        notes_text.setPlainText(current_note)
        notes_text.setPlaceholderText("Enter notes about this conversation...")
        layout.addWidget(notes_text)
        
        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        # Resizable dialog
        dialog.setMinimumSize(600, 400)
        dialog.resize(600, 400)
        
        # Apply dark mode stylesheet if enabled
        if hasattr(self, 'theme_manager') and self.dark_mode:
            dialog.setStyleSheet(self.theme_manager.get_dialog_stylesheet())
        
        if dialog.exec_() == QDialog.Accepted:
            new_note = notes_text.toPlainText()
            if new_note.strip():
                self.conversation_notes[conv_id] = new_note
            elif conv_id in self.conversation_notes:
                # Remove note if empty
                del self.conversation_notes[conv_id]
            # Notes are saved in progress JSON, not config
            self.status.showMessage("Note saved", 3000)
    
    def _get_conversation_display_name(self, conv_id):
        """Get the conversation display name in user1,user2 format."""
        if conv_id not in self.conversations or len(self.conversations[conv_id]) == 0:
            return conv_id
        
        # Get the latest message from the conversation
        latest_msg_idx = self.conversations[conv_id][-1]
        if latest_msg_idx >= len(self.all_messages):
            return conv_id
        
        latest_msg = self.all_messages[latest_msg_idx]
        sender = latest_msg.get('sender_username') or latest_msg.get('sender') or 'Unknown'
        receiver = latest_msg.get('recipient_username') or latest_msg.get('receiver') or 'Unknown'
        
        # Build name using same logic as populate_selector
        if sender == receiver or (sender == 'Unknown' and receiver == 'Unknown'):
            name = sender if sender != 'Unknown' else 'Unknown'
        elif sender == 'Unknown':
            name = receiver
        elif receiver == 'Unknown':
            name = sender
        else:
            # Sort when we have two different non-Unknown participants
            name = ", ".join(sorted([sender, receiver]))
        
        return name
    
    def view_all_notes(self):
        """Open dialog to view all existing notes."""
        if not self.conversation_notes:
            QMessageBox.information(self, "No Notes", "No notes have been added yet.")
            return
        
        # Create dialog
        dialog = QDialog(self)
        dialog.setWindowTitle("View All Notes")
        dialog.setWindowFlags(dialog.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        layout = QVBoxLayout(dialog)
        
        # Create scroll area for notes
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        # Add each note
        for conv_id, note in sorted(self.conversation_notes.items()):
            # Get conversation display name (user1,user2 format)
            conv_display_name = self._get_conversation_display_name(conv_id)
            
            # Create note group box
            note_group = QGroupBox(f"Conversation: {conv_display_name}")
            note_layout = QVBoxLayout()
            note_text = QTextEdit()
            note_text.setPlainText(note)
            note_text.setReadOnly(True)
            note_text.setMaximumHeight(150)  # Limit height for readability
            note_layout.addWidget(note_text)
            note_group.setLayout(note_layout)
            scroll_layout.addWidget(note_group)
        
        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll)
        
        # Close button
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        # Resizable dialog
        dialog.setMinimumSize(700, 500)
        dialog.resize(700, 500)
        
        # Apply dark mode stylesheet if enabled
        if hasattr(self, 'theme_manager') and self.dark_mode:
            dialog.setStyleSheet(self.theme_manager.get_dialog_stylesheet())
        
        dialog.exec_()
    
    def manage_hotkeys(self):
        # Pass TAG_COLORS so the dialog knows which tags are protected defaults
        dlg = ManageHotkeysDialog(self.available_tags, self.hotkeys, self.TAG_COLORS, self) 
        if dlg.exec_() == QDialog.Accepted:
            new_tags, new_hotkeys = dlg.get_hotkeys_and_tags()
            
            # Update internal state
            self.available_tags = new_tags
            self.hotkeys = new_hotkeys
            
            self.save_config()
            self.bind_hotkeys() # Rebind to apply new keys
            self.refresh_message_table_with_status(
                None,
                "Refreshing messages with updated tag hotkeys...",
                "Messages refreshed."
            )

 
    def manage_keywords(self):
            current_kws = self.keyword_lists # self.keyword_lists is a dictionary
            
            # FIX: Change the dialog class name from KeywordEditorDialog to ManageKeywordListsDialog
            dlg = ManageKeywordListsDialog(current_kws, parent=self) 
            
            if dlg.exec_() == QDialog.Accepted:
                new_kws = dlg.get_keyword_lists()
                self.keyword_lists = new_kws
                self.save_config()
                QMessageBox.information(self, "Keywords Saved", "Keyword lists have been saved. Re-run filters to apply changes.")
                
    def refresh_message_table_with_status(self, conv_id=None,
                                          message="Refreshing messages...",
                                          done_message="Messages refreshed."):
        """
        Helper to refresh the main message table with a busy cursor and status message.
        """
        self.start_long_operation(message)
        try:
            # pass conv_id through; refresh_message_table will resolve None to current selection
            self.refresh_message_table(conv_id)
        finally:
            self.end_long_operation(done_message)



    def do_filter_dialog(self):
        dlg = FilterDialog(self.unique_values, self.active_filters, self)
        if dlg.exec_() == QDialog.Accepted:
            self.active_filters = dlg.get_filters()
            self.save_config()
            self.update_filter_status_label()
            # Clear cache when filters change
            self._cached_all_conversations_indices = None
            self._cached_filter_hash = None
            self._cached_filter_mask = None  # OPTIMIZED: Clear cached filter mask
            self.refresh_message_table_with_status(
                None,
                "Applying filters and refreshing messages...",
                "Filters applied."
            )

            
    def start_long_operation(self, message="Working, be patient..."):
        """Show a busy cursor + status message during long operations."""
        try:
            if hasattr(self, "status"):
                self.status.showMessage(message)
        except Exception:
            pass  # Fail-safe, don't crash if status bar isn't ready yet

        QApplication.setOverrideCursor(Qt.WaitCursor)
        QApplication.processEvents()  # Let the UI update immediately

    def end_long_operation(self, message=None):
        """Restore cursor and optionally show a completion message."""
        QApplication.restoreOverrideCursor()
        try:
            if hasattr(self, "status"):
                if message:
                    # Show completion message for 3 seconds
                    self.status.showMessage(message, 3000)
                else:
                    self.status.clearMessage()
        except Exception:
            pass
            
    def apply_logging_setting(self, show_status=False):
        """
        Enable or disable logging based on self.logging_enabled.
        When enabled, attach a RotatingFileHandler for LOG.
        When disabled, remove that handler and stop writing.
        Log file is saved in the same directory as config.json.
        """
        enabled = bool(self.logging_enabled)

        root_logger = logging.getLogger()  # root
        module_logger = logger            # our module-level logger

        # Ensure log level is set (even if no handlers are attached yet)
        root_logger.setLevel(logging.DEBUG)
        module_logger.setLevel(logging.DEBUG)

        # Get log file path - use same directory as config.json
        config_dir = os.path.dirname(self.config_path)
        log_path = os.path.join(config_dir, LOG)

        # Remove any existing RotatingFileHandler for our log file
        for handler in list(root_logger.handlers):
            if isinstance(handler, RotatingFileHandler):
                # Check that this handler is for our LOG file
                try:
                    handler_path = getattr(handler, "baseFilename", "")
                    # Check if it matches either the old location or new location
                    if (os.path.basename(handler_path) == os.path.basename(LOG) or 
                        handler_path == log_path or 
                        handler_path == LOG):
                        root_logger.removeHandler(handler)
                        handler.close()
                except Exception:
                    # Fail-safe: if anything is weird, just remove it
                    root_logger.removeHandler(handler)
                    handler.close()

        if enabled:
            # Ensure the log directory exists
            os.makedirs(config_dir, exist_ok=True)
            # Attach a fresh RotatingFileHandler for our file
            file_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3)
            file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
            root_logger.addHandler(file_handler)

            root_logger.disabled = False
            module_logger.disabled = False
        else:
            # No handler attached; logging calls do nothing
            root_logger.disabled = True
            module_logger.disabled = True

        if show_status and hasattr(self, "status"):
            msg = "Logging enabled" if enabled else "Logging disabled"
            self.status.showMessage(msg, 3000)


    def toggle_logging_enabled(self, checked):
        """
        Slot for the 'Enable Logging' menu item.
        """
        self.logging_enabled = bool(checked)

        # Apply to logger and optionally show status
        self.apply_logging_setting(show_status=True)

        # Persist to config
        self.save_config()


    def clear_all_filters(self):
        self.start_long_operation("Clearing filters and refreshing messages...")
        try:
            self.active_filters = {
                'from_date': None, 'to_date': None, 'sender_username': None,
                'message_type': None, 'content_type': None, 'saved_by': None,
                'is_saved_display': 'All Messages'
            }
            self.save_config()
            self.update_filter_status_label()
            # Clear cache when filters change
            self._cached_all_conversations_indices = None
            self._cached_filter_hash = None
            self._cached_filter_mask = None  # OPTIMIZED: Clear cached filter mask
            self.refresh_message_table()
        finally:
            self.end_long_operation("Filters cleared.")


    # In SnapParserMain.update_filter_status_label:
    def update_filter_status_label(self):
        active = []
        if self.active_filters['from_date'] or self.active_filters['to_date']:
            f = self.active_filters['from_date'].strftime("%Y-%m-%d") if self.active_filters['from_date'] else 'Start'
            t = (self.active_filters['to_date'] - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d") if self.active_filters['to_date'] else 'End'
            active.append(f"Date: {f} to {t}")
        
        for key, display in [('sender_username', 'Sender'), ('message_type', 'Type'), ('content_type', 'Content')]:
            if self.active_filters.get(key):
                active.append(f"{display}: {self.active_filters[key]}")

        if self.active_filters.get('is_saved_display') != 'All Messages':
            active.append(f"Saved: {self.active_filters['is_saved_display']}")
            
        status = "; ".join(active) if active else "None"
        self.filter_status_label.setText(status)
        self.clear_filters_btn.setVisible(bool(active))


    def search_dialog(self):
        dlg = SearchDialog(self.keyword_lists, self)
        if dlg.exec_() == QDialog.Accepted:
            query_params = dlg.get_search_params()
            
            # Search logic is mostly contained in get_filtered_messages
            current_conv_id = self.conv_selector.currentData() if self.conv_selector.currentData() else None
            
            # Get messages matching search *within* current filters and conversation context
            search_results_indices = self.get_filtered_messages(conv_id=current_conv_id, apply_filters=True, query_params=query_params)
            
            if not search_results_indices:
                QMessageBox.information(self, "Search", "No messages matched the search criteria.")
                return

            # Display results in a MessageViewerDialog
            query_text = query_params.get('query', '') or ''
            dlg = MessageViewerDialog(search_results_indices, self.all_messages, self.basenames,
                                      self.media_extract_dir, self.thumb_dir, self.blur_all, parent=self,
                                      highlight_query=query_text)

            dlg.setWindowTitle(f"Search Results: {len(search_results_indices)} Messages")
            dlg.exec_()
            
    def export_dialog(self):
        dlg = ExportOptionsDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            options = dlg.get_options()
            self.perform_export(options)

    def perform_export(self, options):
        file_path, _ = QFileDialog.getSaveFileName(self, "Export Report", "", "HTML Files (*.html)" if options['format'] == 'HTML' else "CSV Files (*.csv)")
        if not file_path:
            return
        if options['format'] == 'HTML':
            media_dir = os.path.join(os.path.dirname(file_path), 'media')
            os.makedirs(media_dir, exist_ok=True)
            thumb_dir = os.path.join(media_dir, 'thumbs')
            os.makedirs(thumb_dir, exist_ok=True)
        else:
            media_dir = None
            thumb_dir = None
        # 1. Determine the set of message indices to export
        messages_to_export = []
        if options.get('scope_all'):
            messages_to_export = list(range(len(self.all_messages)))
        elif options.get('scope_selected'):
            current_item = self.conv_selector.currentData()
            conv_id = current_item if current_item else None
            messages_to_export = self.conversations.get(conv_id, [])
        elif options.get('scope_tagged'):
            messages_to_export = [i for i, msg in enumerate(self.all_messages) if msg.get('tags')]
        if not messages_to_export:
            QMessageBox.warning(self, "Export", "No messages selected for export.")
            return
        
        # 2. Prepare data
        export_data = [self.all_messages[i] for i in messages_to_export]
        # Add conversation_id to each msg if not present
        for msg in export_data:
            if 'conversation_id' not in msg:
                msg['conversation_id'] = '' # Or lookup if needed
        df = pd.DataFrame(export_data)
        # Build conv_to_users map
        conv_to_users = defaultdict(set)
        for msg in export_data:
            conv_id = msg.get('conversation_id', '')
            sender = msg.get('sender_username') or msg.get('sender') or ''
            receiver = msg.get('recipient_username') or msg.get('receiver') or ''
            conv_to_users[conv_id].add(sender)
            conv_to_users[conv_id].add(receiver)
        conv_to_users = {k: ', '.join(sorted(v - {''})) for k, v in conv_to_users.items()}
        # 3. Sort
        if options['sort_by'] == "User/Conversation (Default)":
            df = df.sort_values(by=['conversation_id', 'timestamp'])
        else: # Timestamp
            df = df.sort_values(by='timestamp')
        # 4. Select fields (map your col_data keys to headers)
        selected_fields = options['fields']
        col_map = { # Map headers to df columns
            'Conversation': 'conversation_id',
            'Conversation Title': 'conversation_title',
            'Message ID': 'message_id',
            'Reply To': 'reply_to_message_id',
            'Content Type': 'content_type',
            'Message Type': 'message_type',
            'Date': 'timestamp', # Process to date str later
            'Time': 'timestamp', # Process to time str later
            'Sender': 'sender_username',
            'Receiver': 'recipient_username',
            'Message': 'text',
            'Media ID': 'media_id',
            'Media': 'media_id', # Special handling
            'Tags': 'tags',
            'Saved By': 'saved_by',
            'One-on-One?': 'is_one_on_one',
            'IP': 'upload_ip',
            'Port': 'source_port_number',
            'Reactions': 'reactions',
            'Screenshotted By': 'screenshotted_by',
            'Replayed By': 'replayed_by',
            'Screen Recorded By': 'screen_recorded_by',
            'Read By': 'read_by',
            'Source': 'source',
            'Line #': 'source_line',
        }
        # Handle Group Members column separately - combine usernames and user IDs
        if 'Group Members' in selected_fields:
            # Combine the two columns
            df['Group Members'] = df.apply(
                lambda row: (
                    f"Usernames: {row.get('group_member_usernames', '')}\nUser IDs: {row.get('group_member_user_ids', '')}"
                    if pd.notna(row.get('group_member_usernames')) and str(row.get('group_member_usernames', '')).strip() and 
                       pd.notna(row.get('group_member_user_ids')) and str(row.get('group_member_user_ids', '')).strip()
                    else f"Usernames: {row.get('group_member_usernames', '')}" if pd.notna(row.get('group_member_usernames')) and str(row.get('group_member_usernames', '')).strip()
                    else f"User IDs: {row.get('group_member_user_ids', '')}" if pd.notna(row.get('group_member_user_ids')) and str(row.get('group_member_user_ids', '')).strip()
                    else ''
                ), axis=1
            )
            # Remove the individual columns if they exist
            if 'group_member_usernames' in df.columns:
                df = df.drop(columns=['group_member_usernames'])
            if 'group_member_user_ids' in df.columns:
                df = df.drop(columns=['group_member_user_ids'])
        
        # Select only the columns that are in selected_fields and exist in col_map
        # BUT preserve conversation_id for filtering even if not selected
        cols_to_select = [col_map[f] for f in selected_fields if f in col_map]
        if 'Group Members' in selected_fields:
            cols_to_select.append('Group Members')
        # Always include conversation_id for filtering, even if not in selected_fields
        if 'conversation_id' not in cols_to_select and 'conversation_id' in df.columns:
            cols_to_select.append('conversation_id')
        df = df[[c for c in cols_to_select if c in df.columns]]
        df.columns = [f if f != 'Group Members' or 'Group Members' not in selected_fields else 'Group Members' for f in selected_fields if (f in col_map or f == 'Group Members')] if 'Group Members' not in selected_fields else [f if f != 'Group Members' else 'Group Members' for f in selected_fields if (f in col_map or f == 'Group Members')]
        
        # Fix column names - map back to selected field names
        rename_dict = {}
        field_idx = 0
        for col in df.columns:
            if field_idx < len(selected_fields):
                rename_dict[col] = selected_fields[field_idx]
                field_idx += 1
        df = df.rename(columns=rename_dict)
        
        # Replace Conversation with users_str if present
        if 'Conversation' in selected_fields:
            df['Conversation'] = df['Conversation'].map(conv_to_users)
        # Process Date/Time if selected
        if 'Date' in selected_fields:
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        if 'Time' in selected_fields:
            df['Time'] = df['Time'].dt.strftime('%H:%M:%S')
        unique_dates = []
        if options['sort_by'] == "Timestamp" and 'Date' in selected_fields:
            unique_dates = sorted(df['Date'].dropna().unique()) # Sorted ascending YYYY-MM-DD
        # Build conversation mapping: conv_id -> display_text (title or user names)
        # This will be used for the dropdown: conv_id as value, display_text as label
        conv_id_to_display = {}
        for msg in export_data:
            conv_id = msg.get('conversation_id', '')
            if conv_id and conv_id not in conv_id_to_display:
                # Prefer conversation title, fallback to user names, then conv_id
                conv_title = msg.get('conversation_title', '')
                if conv_title:
                    conv_id_to_display[conv_id] = conv_title
                elif conv_id in conv_to_users:
                    conv_id_to_display[conv_id] = conv_to_users[conv_id]
                else:
                    conv_id_to_display[conv_id] = conv_id
        if 'Tags' in selected_fields:
            df['Tags'] = df['Tags'].apply(lambda t: ', '.join(sorted(t)) if isinstance(t, set) else '')
        if 'Reactions' in selected_fields:
            df['Reactions'] = df['Reactions'].apply(lambda r: parse_reactions(r) if pd.notna(r) else '')
        
        # Format Group Members column for compact display and store full data
        # Create mapping from DataFrame index to original export_data index
        group_members_full_data = {}
        if 'Group Members' in selected_fields:
            for df_idx, (orig_idx, row) in enumerate(df.iterrows()):
                # Map DataFrame row index to export_data index
                export_idx = messages_to_export[df_idx] if df_idx < len(messages_to_export) else None
                if export_idx is not None and export_idx < len(export_data):
                    usernames = str(export_data[export_idx].get('group_member_usernames', '')).strip()
                    user_ids = str(export_data[export_idx].get('group_member_user_ids', '')).strip()
                    combined = ''
                    if usernames and user_ids:
                        combined = f"Usernames: {usernames}\nUser IDs: {user_ids}"
                    elif usernames:
                        combined = f"Usernames: {usernames}"
                    elif user_ids:
                        combined = f"User IDs: {user_ids}"
                    group_members_full_data[df_idx] = combined
        
        # Format display text for Group Members
        if 'Group Members' in selected_fields:
            df['Group Members'] = df['Group Members'].apply(
                lambda x: format_group_member_display(x)[0] if pd.notna(x) and str(x).strip() else ''
            )
        
        # Collect internals for hashes
        all_internals = set([cf[1] for cf in self.conv_files])
        total_media_entries = 0
        for msg in export_data:
            media_id = str(msg.get('media_id') or msg.get('content_id') or '')
            if media_id:
                entries = find_media_by_media_id(media_id, self.basenames)
                if 'Media' in selected_fields and options['format'] == 'HTML':
                    total_media_entries += len(entries)
                for _, internal in entries:
                    all_internals.add(internal)
        
        # Calculate total work for progress: hashes + media entries + 1 for final generation
        total_files_for_hash = len(all_internals)
        total_steps = total_files_for_hash + total_media_entries + 1  # +1 for final report gen
        progress = QProgressDialog("Exporting...", "Cancel", 0, total_steps, self)
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)  # Show immediately
        if total_steps < 10:
            # Skip progress for tiny exports
            pass  # Continue without showing
        else:
            progress.show()
            QApplication.processEvents()
        
        progress.setLabelText("Computing file hashes...")
        hashes = {}
        current_step = 0
        for internal in sorted(all_internals):  # Sorted for consistency, but optional
            for base, zpath, i in self.basenames:
                if i == internal:
                    data = get_file_bytes_from_zip(zpath, i)
                    if data:
                        hashes[internal] = hashlib.md5(data).hexdigest()
                    current_step += 1
                    progress.setValue(current_step)
                    if progress.wasCanceled():
                        return  # Early exit on cancel
                    QApplication.processEvents()  # Keep UI responsive
                    break
        
        # Export hashes to CSV
        hashes_csv_path = os.path.join(os.path.dirname(file_path), 'file_hashes.csv')
        with open(hashes_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['File Name', 'MD5'])
            for internal, md5 in sorted(hashes.items()):
                filename = os.path.basename(internal)
                writer.writerow([filename, md5])
        # In the HTML, replace {hashes_html} with a link
        # In the HTML, replace {hashes_html} with a link
        hashes_html = f'<h2>File MD5 Hashes</h2><p><a href="file_hashes.csv" target="_blank">View File Hashes CSV</a></p>'
        # 5. Media handling with blur
        if 'Media' in selected_fields and options['format'] == 'HTML':
            progress.setLabelText("Processing media...")
            media_results = []
            for idx, row in df.iterrows():
                media_id = row['Media']
                if not media_id:
                    media_results.append('')
                    continue
                entries = find_media_by_media_id(media_id, self.basenames)
                html_imgs = []
                for zpath, internal in entries:
                    base = os.path.basename(internal)
                    dest = os.path.join(media_dir, base)
                    i = 1
                    name, ext = os.path.splitext(base)
                    while os.path.exists(dest):
                        dest = os.path.join(media_dir, f"{name}_{i}{ext}")
                        i += 1
                    # extract to dest
                    parts = internal.split("!")
                    cur = zpath
                    try:
                        for p in parts:
                            with zipfile.ZipFile(cur, 'r') as z:
                                if p == parts[-1]:
                                    with open(dest, 'wb') as dst:
                                        shutil.copyfileobj(z.open(p), dst)
                                    break
                                else:
                                    raw = z.read(p)
                                    cur = io.BytesIO(raw)
                    except:
                        html_imgs.append('<span>Preview Not Available</span>')
                        current_step += 1 # Still increment even on error
                        progress.setValue(current_step)
                        if progress.wasCanceled():
                            return
                        continue
                    if os.path.exists(dest):
                        is_blurred = options['blur_all'] or (options['blur_csam'] and 'CSAM' in row.get('Tags', ''))
                        if is_blurred:
                            # Blur the full media if blurred
                            media_type = self.get_media_type_from_path(dest)
                            if media_type == 'image':
                                im = Image.open(dest)
                                im = im.filter(ImageFilter.GaussianBlur(105))
                                im.save(dest)
                            elif media_type == 'video':
                                temp_dest = dest + '.tmp.mp4'
                                cap = cv2.VideoCapture(dest)
                                if cap.isOpened():
                                    fps = cap.get(cv2.CAP_PROP_FPS)
                                    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                                    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                                    scale_factor = 2
                                    down_width = width // scale_factor
                                    down_height = height // scale_factor
                                    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
                                    out = cv2.VideoWriter(temp_dest, fourcc, fps, (width, height))
                                    while True:
                                        ret, frame = cap.read()
                                        if not ret:
                                            break
                                        down_frame = cv2.resize(frame, (down_width, down_height), interpolation=cv2.INTER_AREA)
                                        blurred_down = cv2.blur(down_frame, (61, 61))
                                        blurred_frame = cv2.resize(blurred_down, (width, height), interpolation=cv2.INTER_LINEAR)
                                        out.write(blurred_frame)
                                    cap.release()
                                    out.release()
                                    shutil.move(temp_dest, dest)
                                else:
                                    os.remove(dest)
                                    dest = None
                        if dest: # Only proceed if dest exists (not removed)
                            thumb = generate_thumbnail(dest, thumb_dir)
                            if thumb and os.path.exists(thumb):
                                im = Image.open(thumb)
                                if is_blurred:
                                    im = im.filter(ImageFilter.GaussianBlur(11))
                                im.save(thumb)
                                rel_thumb = os.path.relpath(thumb, os.path.dirname(file_path))
                                rel_original = os.path.relpath(dest, os.path.dirname(file_path))
                                media_type = self.get_media_type_from_path(dest)
                                label = 'IMG' if media_type == 'image' else 'VID' if media_type == 'video' else 'OTHER'
                                img_html = f'<div class="media-container"><a href="{rel_original}" target="_blank"><img src="{rel_thumb}" width="100" alt="Media" style="cursor:pointer;"></a><span class="media-type">{label}</span></div>'
                                html_imgs.append(img_html)
                            else:
                                html_imgs.append('<span>Preview Not Available</span>')
                        else:
                            html_imgs.append('<span>Preview Not Available</span>')
                    else:
                        html_imgs.append('<span>Preview Not Available</span>')
                    # Increment progress per entry (even on error/skip)
                    current_step += 1
                    progress.setValue(current_step)
                    if progress.wasCanceled():
                        return
                    QApplication.processEvents()
                media_results.append(' '.join(html_imgs) if html_imgs else media_id)
            df['Media'] = media_results
        elif 'Media' in selected_fields:
            df['Media'] = df['Media'] # keep id (unchanged)
        
        progress.setLabelText("Generating report...")
        
        # 6. Row coloring based on tags and alternating senders
        def get_tag_style(tags_str):
            tags = tags_str.split(', ') if tags_str else []
            if 'CSAM' in tags:
                return 'background-color: #ff4c4c;'
            elif 'Evidence' in tags:
                return 'background-color: #ffc04c;'
            elif 'Of Interest' in tags:
                return 'background-color: #ffff4c;'
            return ''
        if 'Tags' in selected_fields:
            df['tag_style'] = df['Tags'].apply(get_tag_style)
        # 7. Generate HTML
        if options['format'] == 'HTML':
            # Compute stats on export_data
            total_messages = len(export_data)
            unique_convs = len(set(m.get('conversation_id', '') for m in export_data if m.get('conversation_id')))
            users = set()
            for m in export_data:
                users.add(m.get('sender_username') or m.get('sender') or '')
                users.add(m.get('recipient_username') or m.get('receiver') or '')
            users.discard('')
            unique_users = len(users)
            tagged_messages = sum(1 for m in export_data if m.get('tags'))
            tag_counts = defaultdict(int)
            for m in export_data:
                for tag in m.get('tags', set()):
                    tag_counts[tag] += 1
            tag_breakdown = '<br>'.join([f"{tag}: {count}" for tag, count in sorted(tag_counts.items())])
            keyword_hits = 0
            all_keywords = []
            for kw_list in self.keyword_lists.values():
                all_keywords.extend([kw[0].lower() for kw in kw_list])
            all_keywords = set(all_keywords)
            for m in export_data:
                text = str(m.get('text') or m.get('message') or '').lower()
                if any(kw in text for kw in all_keywords):
                    keyword_hits += 1
            total_media = sum(1 for m in export_data if m.get('media_id') or m.get('content_id'))
            timestamps = [m.get('timestamp') for m in export_data if m.get('timestamp')]
            if timestamps:
                min_date = min(timestamps).strftime("%Y-%m-%d")
                max_date = max(timestamps).strftime("%Y-%m-%d")
                date_period = f"{min_date} to {max_date}"
            else:
                date_period = "N/A"
            # Base table with escape=False for <img>
            table_html = df.to_html(index=False, escape=False)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(table_html, 'html.parser')
            tbody = soup.find('tbody')
            
            # Add notes rows before table rows if notes are included
            include_notes = 'Notes' in selected_fields
            if include_notes and tbody:
                # Get conversation IDs from export data and create note mapping
                notes_to_export = {}
                conv_titles = {}
                for msg_idx in messages_to_export:
                    if msg_idx < len(self.all_messages):
                        msg = self.all_messages[msg_idx]
                        conv_id = msg.get('conversation_id', '')
                        if conv_id and conv_id in self.conversation_notes and self.conversation_notes[conv_id]:
                            notes_to_export[conv_id] = self.conversation_notes[conv_id]
                            if conv_id not in conv_titles:
                                # Get conversation display name (user1,user2 format)
                                sender = msg.get('sender_username') or msg.get('sender') or 'Unknown'
                                receiver = msg.get('recipient_username') or msg.get('receiver') or 'Unknown'
                                if sender == receiver or (sender == 'Unknown' and receiver == 'Unknown'):
                                    conv_titles[conv_id] = sender if sender != 'Unknown' else 'Unknown'
                                elif sender == 'Unknown':
                                    conv_titles[conv_id] = receiver
                                elif receiver == 'Unknown':
                                    conv_titles[conv_id] = sender
                                else:
                                    conv_titles[conv_id] = ", ".join(sorted([sender, receiver]))
                
                # Track which conversations we've already added notes for
                notes_added = set()
                current_conv_id = None
                
                # Insert note rows before messages from that conversation
                data_rows = tbody.find_all('tr')
                rows_to_insert = []
                for r_idx, tr in enumerate(data_rows):
                    if r_idx >= len(df):
                        break
                    msg = df.iloc[r_idx]
                    # Get conversation ID from original export data
                    if r_idx < len(messages_to_export):
                        orig_msg_idx = messages_to_export[r_idx]
                        if orig_msg_idx < len(export_data):
                            msg_conv_id = export_data[orig_msg_idx].get('conversation_id', '')
                            if msg_conv_id != current_conv_id:
                                current_conv_id = msg_conv_id
                                # Add note row if this conversation has a note and we haven't added it yet
                                if msg_conv_id in notes_to_export and msg_conv_id not in notes_added:
                                    note = notes_to_export[msg_conv_id]
                                    # Get conversation display name (user1,user2 format) from conv_titles
                                    conv_display_name = conv_titles.get(msg_conv_id, msg_conv_id)
                                    note_row = soup.new_tag('tr', **{'class': 'conversation-note', 'data-conversation': html.escape(str(msg_conv_id))})
                                    note_cell = soup.new_tag('td', colspan=len(selected_fields))
                                    note_cell['style'] = (
                                        "background: linear-gradient(to right, #d4e6f1 0%, #e8f4f8 100%); "
                                        "border-left: 4px solid #3498db; "
                                        "padding: 12px 15px; "
                                        "margin: 5px 0; "
                                        "font-size: 13px; "
                                        "line-height: 1.6; "
                                        "box-shadow: 0 2px 4px rgba(0,0,0,0.1);"
                                    )
                                    note_cell_content = soup.new_tag('div', style='display: flex; align-items: flex-start;')
                                    icon_span = soup.new_tag('span', style='font-size: 16px; margin-right: 8px; color: #2980b9;')
                                    icon_span.string = '📝'
                                    note_cell_content.append(icon_span)
                                    content_div = soup.new_tag('div', style='flex: 1;')
                                    title_div = soup.new_tag('div', style='font-weight: 600; color: #2c3e50; margin-bottom: 5px; font-size: 14px;')
                                    title_div.string = f'Investigative Note: {html.escape(str(conv_display_name))}'
                                    content_div.append(title_div)
                                    note_div = soup.new_tag('div', style='color: #34495e; white-space: pre-wrap;')
                                    note_div.string = html.escape(note)
                                    content_div.append(note_div)
                                    note_cell_content.append(content_div)
                                    note_cell.append(note_cell_content)
                                    note_row.append(note_cell)
                                    rows_to_insert.append((r_idx, note_row))
                                    notes_added.add(msg_conv_id)
                
                # Insert note rows in reverse order to maintain indices
                for r_idx, note_row in reversed(rows_to_insert):
                    if r_idx < len(data_rows):
                        data_rows[r_idx].insert_before(note_row)
                    else:
                        tbody.append(note_row)
            
            if tbody:
                data_rows = tbody.find_all('tr')
                last_sender = None
                last_conv = None
                alt_toggle = False
                df_row_idx = 0
                for r_idx, tr in enumerate(data_rows):
                    # Skip note rows
                    if 'conversation-note' in tr.get('class', []):
                        continue
                    if df_row_idx >= len(df):
                        break
                    msg = df.iloc[df_row_idx]
                    sender = msg['Sender'] if 'Sender' in selected_fields else ''
                    conv = msg['Conversation'] if 'Conversation' in selected_fields else ''
                    
                    # Add data-conversation attribute for filtering (ALWAYS set this for all rows)
                    # Use conversation_id as the value (unique identifier) for reliable filtering
                    # Get conversation_id using DataFrame index (preserved after sorting) to map back to export_data
                    if df_row_idx < len(df):
                        try:
                            # After sorting, DataFrame index still points to original export_data indices
                            # Use df.index to get the original index, then look up conversation_id from export_data
                            original_idx = df.index[df_row_idx]
                            if original_idx < len(export_data):
                                conv_id = export_data[original_idx].get('conversation_id', '')
                                if conv_id:
                                    # Convert to string, strip whitespace, and escape for HTML
                                    conv_id_str = str(conv_id).strip()
                                    if conv_id_str:
                                        tr['data-conversation'] = html.escape(conv_id_str)
                        except (KeyError, IndexError, AttributeError):
                            # If anything fails, skip setting the attribute (row won't be filterable by conversation)
                            pass
                    
                    tag_style = df['tag_style'].iloc[df_row_idx] if 'tag_style' in df else ''
                    if tag_style:
                        style = tag_style
                    else:
                        if conv != last_conv:
                            last_sender = None
                            alt_toggle = False
                            last_conv = conv
                        if sender != last_sender:
                            alt_toggle = not alt_toggle
                            last_sender = sender
                        style = 'background-color: #add8e6;' if not alt_toggle else 'background-color: #d3d3d3;'
                    tr['style'] = style
                    df_row_idx += 1
            # Remove 'tag_style' column from the table if present
            col_index = -1
            headers = soup.find_all('th')
            for i, th in enumerate(headers):
                if th.text.strip() == 'tag_style':
                    col_index = i
                    th.decompose() # Remove the header cell
                    break
            if col_index != -1:
                # Remove the corresponding cell in each data row
                for tr in soup.find('tbody').find_all('tr'):
                    cells = tr.find_all('td')
                    if len(cells) > col_index:
                        cells[col_index].decompose()
            
            # Modify Group Members column to be clickable with popup (before converting to string)
            headers = soup.find_all('th')
            group_members_col_idx = -1
            for i, th in enumerate(headers):
                text = th.text.strip()
                if text == 'Group Members':
                    group_members_col_idx = i
                    break
            
            # Process each row to add click handlers and data attributes
            data_rows = soup.find('tbody').find_all('tr') if soup.find('tbody') else []
            
            # Handle Group Members column - make clickable with blue text (ONLY column with hyperlinks)
            for r_idx, tr in enumerate(data_rows):
                cells = tr.find_all('td')
                
                # Handle Group Members column
                if group_members_col_idx >= 0 and len(cells) > group_members_col_idx:
                    cell = cells[group_members_col_idx]
                    full_data = group_members_full_data.get(r_idx, '')
                    if full_data:
                        display_text = cell.get_text(strip=True)
                        if display_text and display_text != full_data:  # Only modify if it's the compact view
                            # Parse the combined data to extract usernames and user IDs
                            usernames = ''
                            user_ids = ''
                            if 'Usernames:' in full_data and 'User IDs:' in full_data:
                                parts = full_data.split('User IDs:')
                                usernames = parts[0].replace('Usernames:', '').strip()
                                user_ids = parts[1].strip() if len(parts) > 1 else ''
                            elif 'Usernames:' in full_data:
                                usernames = full_data.replace('Usernames:', '').strip()
                            elif 'User IDs:' in full_data:
                                user_ids = full_data.replace('User IDs:', '').strip()
                            
                            # Escape HTML for JavaScript
                            # html module is already imported at top level
                            escaped_usernames = html.escape(usernames) if usernames else ''
                            escaped_userids = html.escape(user_ids) if user_ids else ''
                            # Make cell clickable with blue text
                            cell['style'] = (cell.get('style', '') + ' color: blue; cursor: pointer; text-decoration: underline;').strip()
                            cell['onclick'] = f"showGroupMembers({r_idx}, '{escaped_usernames}', '{escaped_userids}')"
                            cell['title'] = 'Click to view all group members'
                
                # Note: Other columns (Screenshotted By, Replayed By, Read By, Saved By, Screen Recorded By)
                # are NOT converted to hyperlinks - only Group Members column has hyperlinks
            
            table_html = str(soup)
            # Wrap table in div for horizontal scrolling
            table_html = f'<div class="table-wrapper">{table_html}</div>'
            # Use BeautifulSoup to modify table (re-parse for further modifications)
            soup = BeautifulSoup(table_html, 'html.parser')
            table = soup.find('table')
            if table:
                table['class'] = table.get('class', []) + ['dataframe']
            headers = soup.find_all('th')
            for idx, th in enumerate(headers):
                text = th.text.strip()
                if text == 'Date':
                    th['style'] = (th.get('style', '') + ' min-width: 100px;').strip()
                elif text == 'Message':
                    th['style'] = (th.get('style', '') + ' min-width: 200px; max-width: 600px; white-space: normal; word-wrap: break-word;').strip()
                # NEW: Add narrower widths for specific columns (adjust px values as needed)
                elif text in ['Port', 'One-on-One?']: # Example: Very narrow columns (e.g., numbers or yes/no)
                    th['style'] = (th.get('style', '') + ' width: 60px; max-width: 60px;').strip() # Fixed narrow width
                elif text in ['IP', 'Message ID']: # Example: Slightly wider but capped (e.g., IPs or IDs)
                    th['style'] = (th.get('style', '') + ' min-width: 80px; max-width: 200px; white-space: normal; word-wrap: break-word;').strip() # Allow wrapping with max width
                elif text == 'Media ID': # NEW: Max width with word wrapping for long IDs
                    th['style'] = (th.get('style', '') + ' max-width: 200px; white-space: normal; word-wrap: break-word;').strip() # Adjust max-width px as needed
                # Add onclick for sorting
                th['onclick'] = f"sortTable({idx})"
                th['style'] = (th.get('style', '') + ' cursor: pointer; position: relative;').strip()
                # Add resizer element for column resizing
                resizer = soup.new_tag('div', **{'class': 'resizer'})
                resizer['onmousedown'] = f"startResize(event, {idx})"
                th.append(resizer)
            table_html = str(soup)
            # Legend with button-like styles (colors adjusted to match screenshot)
            legend_html = '''
            <h2>Color Legend</h2>
            <div class="legend">
                <span class="legend-item csam">CSAM</span>
                <span class="legend-item evidence">Evidence</span>
                <span class="legend-item interest">Of Interest</span>
                <span class="legend-item sender1">Sender 1</span>
                <span class="legend-item sender2">Sender 2</span>
            </div>
            '''
            # Build tag filter options dynamically from all unique tags in the export
            tag_options = '<option value="">All Tags</option>'
            if tag_counts:
                # Sort tags for consistent ordering
                for tag in sorted(tag_counts.keys()):
                    escaped_tag = html.escape(tag)
                    tag_options += f'<option value="{escaped_tag}">{escaped_tag}</option>'
            
            filter_html = f'''
            <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px; flex-wrap: wrap;">
            <input type="text" id="search" onkeyup="filterTable()" placeholder="Search in table...">
            <select id="tagFilter" onchange="filterTable()">
            {tag_options}
            </select>
            <button id="darkModeToggle" onclick="toggleDarkMode()" style="padding: 5px 15px; cursor: pointer; border: 1px solid #ccc; border-radius: 4px; background-color: #fff; color: #000;">🌙 Dark Mode</button>
            </div>
            '''
            if unique_dates:
                date_options = ''.join([f'<option value="{d}">{d}</option>' for d in unique_dates])
                filter_html += f'''
            <select id="dateFilter" onchange="filterTable()">
            <option value="">All Dates</option>
            {date_options}
            </select>
            '''
            if conv_id_to_display:
                # Sort by display text for better UX, but use conv_id as the value
                sorted_conv_items = sorted(conv_id_to_display.items(), key=lambda x: x[1])
                conv_options = ''.join([f'<option value="{html.escape(conv_id)}">{html.escape(display_text)}</option>' for conv_id, display_text in sorted_conv_items])
                filter_html += f'''
            <select id="convFilter" onchange="filterTable()">
            <option value="all">All Conversations</option>
            {conv_options}
            </select>
            '''
        
            # Script for filtering and sorting
            script_html = '''
            <script>
            function filterTable() {
                // 1. Get filter values
                var search = document.getElementById("search") ? document.getElementById("search").value.toUpperCase() : "";
                var tag = document.getElementById("tagFilter") ? document.getElementById("tagFilter").value : "";
                var dateFilter = document.getElementById("dateFilter") ? document.getElementById("dateFilter").value : "";
                var selector = document.getElementById("convFilter");
                var selectedConvId = selector ? selector.value : "";
                
                // 2. Get table and headers for column-based filtering
                var table = document.querySelector(".dataframe");
                if (!table) return;
                var tr = table.getElementsByTagName("tr");
                if (tr.length === 0) return;
                var headers = tr[0].getElementsByTagName("th");
                var tags_col = -1;
                var date_col = -1;
                
                // Find column indices - use textContent to avoid HTML elements in header
                for (var j = 0; j < headers.length; j++) {
                    // Get text content, removing any child elements (like resizer divs)
                    var headerElement = headers[j];
                    var headerText = '';
                    // Try to get text content directly
                    if (headerElement.textContent) {
                        headerText = headerElement.textContent.trim();
                    } else if (headerElement.innerText) {
                        headerText = headerElement.innerText.trim();
                    } else {
                        // Fallback: get innerHTML and strip tags
                        var temp = headerElement.innerHTML.replace(/<[^>]*>/g, '').trim();
                        headerText = temp;
                    }
                    // Match "Tags" exactly (case-sensitive)
                    if (headerText === "Tags") {
                        tags_col = j;
                    } else if (headerText === "Date") {
                        date_col = j;
                    }
                }
                
                // 3. Select all relevant rows (messages and conversation notes)
                // Get all rows from tbody, but filter out header row
                var tbody = table.querySelector('tbody');
                var rows = tbody ? Array.from(tbody.getElementsByTagName('tr')) : [];
                
                // 4. Iterate over all rows and apply the filter logic
                for (var i = 0; i < rows.length; i++) {
                    var row = rows[i];
                    var show = true;
                    var td = row.getElementsByTagName("td");
                    
                    // Skip conversation note rows (they have class 'conversation-note')
                    var isNoteRow = row.classList && row.classList.contains('conversation-note');
                    
                    // Get the Conversation ID from the row's data attribute
                    var rowConvId = row.getAttribute('data-conversation');
                    if (rowConvId) {
                        rowConvId = rowConvId.trim();
                    }
                    
                    // Conversation filter: 'all' shows everything, otherwise match by Conversation ID
                    // Use strict equality for exact matching (no partial matches)
                    if (selectedConvId && selectedConvId !== "all") {
                        // Normalize both values: trim whitespace and ensure they're strings
                        var normalizedSelected = String(selectedConvId).trim();
                        var normalizedRow = String(rowConvId || '').trim();
                        show = show && (normalizedSelected === normalizedRow);
                    }
                    
                    // Search filter
                    if (show && search) {
                        var textMatch = false;
                        for (var j = 0; j < td.length; j++) {
                            if (td[j].innerHTML.toUpperCase().indexOf(search) > -1) {
                                textMatch = true;
                                break;
                            }
                        }
                        show = show && textMatch;
                    }
                    
                    // Tag filter - skip note rows (they don't have tags)
                    if (tag && !isNoteRow && tags_col !== -1 && td.length > tags_col) {
                        var tags_text = (td[tags_col].textContent || td[tags_col].innerText || '').trim();
                        if (tags_text) {
                            // Split tags by comma and trim each tag, then check if the selected tag exists
                            var tags_array = tags_text.split(',').map(function(t) { return t.trim(); });
                            var tagFound = tags_array.indexOf(tag) !== -1;
                            show = show && tagFound;
                        } else {
                            // If no tags in this row and a tag is selected, hide the row
                            show = false;
                        }
                    } else if (tag && isNoteRow) {
                        // Hide note rows when filtering by tag (they don't have tags)
                        show = false;
                    }
                    
                    // Date filter
                    if (show && dateFilter && date_col !== -1 && td.length > date_col) {
                        var row_date = td[date_col].innerHTML.trim();
                        show = show && (row_date === dateFilter);
                    }
                    
                    // Apply visibility
                    row.style.display = show ? "" : "none";
                }
            }
            function sortTable(colIndex) {
                var table = document.querySelector(".dataframe");
                var rows = Array.from(table.rows).slice(1);
                var ascending = table.getAttribute('data-sort-asc') === 'true';
                rows.sort((a, b) => {
                    var A = a.cells[colIndex].innerText.trim();
                    var B = b.cells[colIndex].innerText.trim();
                    return (A < B ? -1 : A > B ? 1 : 0) * (ascending ? 1 : -1);
                });
                rows.forEach(row => table.tBodies[0].appendChild(row));
                table.setAttribute('data-sort-asc', !ascending);
            }
            function showGroupMembers(rowIdx, usernames, userids) {
                // Create or get modal
                var modal = document.getElementById('groupMembersModal');
                if (!modal) {
                    modal = document.createElement('div');
                    modal.id = 'groupMembersModal';
                    modal.style.cssText = 'display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0,0,0,0.4);';
                    modal.innerHTML = '<div class="modal-content" style="background-color: #fefefe; margin: 15% auto; padding: 20px; border: 1px solid #888; width: 80%; max-width: 600px; border-radius: 5px; transition: background-color 0.3s ease, border-color 0.3s ease, color 0.3s ease;"><span style="color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer;" onclick="closeGroupMembersModal()">&times;</span><div id="groupMembersContent"></div></div>';
                    document.body.appendChild(modal);
                }
                
                // Parse and format data
                var parseData = function(dataStr) {
                    if (!dataStr || !dataStr.trim()) return [];
                    var parts = dataStr.split(',').map(function(s) { return s.trim(); }).filter(function(s) { return s; });
                    if (parts.length === 0 && dataStr.trim()) return [dataStr.trim()];
                    return parts;
                };
                
                var usernameList = parseData(usernames);
                var useridList = parseData(userids);
                
                var content = '<h2>Group Members</h2>';
                if (usernameList.length > 0) {
                    content += '<h3>Usernames (' + usernameList.length + '):</h3>';
                    content += '<div style="max-height: 200px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; margin-bottom: 15px;">';
                    usernameList.forEach(function(u) {
                        content += '<div>' + u + '</div>';
                    });
                    content += '</div>';
                }
                if (useridList.length > 0) {
                    if (usernameList.length > 0) content += '<br>';
                    content += '<h3>User IDs (' + useridList.length + '):</h3>';
                    content += '<div style="max-height: 200px; overflow-y: auto; border: 1px solid #ddd; padding: 10px;">';
                    useridList.forEach(function(id) {
                        content += '<div>' + id + '</div>';
                    });
                    content += '</div>';
                }
                if (usernameList.length === 0 && useridList.length === 0) {
                    content += '<p>No group member data available.</p>';
                }
                
                document.getElementById('groupMembersContent').innerHTML = content;
                modal.style.display = 'block';
            }
            function closeGroupMembersModal() {
                var modal = document.getElementById('groupMembersModal');
                if (modal) {
                    modal.style.display = 'none';
                }
            }
            // Close modal when clicking outside of it
            window.onclick = function(event) {
                var modal = document.getElementById('groupMembersModal');
                if (event.target == modal) {
                    closeGroupMembersModal();
                }
            }
            function toggleDarkMode() {
                document.body.classList.toggle('dark-mode');
                var button = document.getElementById('darkModeToggle');
                if (document.body.classList.contains('dark-mode')) {
                    button.textContent = '☀️ Light Mode';
                    button.style.backgroundColor = '#2d2d2d';
                    button.style.color = '#e0e0e0';
                    button.style.borderColor = '#444';
                } else {
                    button.textContent = '🌙 Dark Mode';
                    button.style.backgroundColor = '#fff';
                    button.style.color = '#000';
                    button.style.borderColor = '#ccc';
                }
                // Save preference to localStorage
                localStorage.setItem('darkMode', document.body.classList.contains('dark-mode'));
            }
            // Load dark mode preference on page load (default: off)
            window.addEventListener('DOMContentLoaded', function() {
                var savedDarkMode = localStorage.getItem('darkMode');
                if (savedDarkMode === 'true') {
                    toggleDarkMode(); // Toggle to dark mode if saved preference is true
                }
            });
            // Column resizing functionality
            var resizing = false;
            var resizingCol = -1;
            var startX = 0;
            var startWidth = 0;
            function startResize(e, colIndex) {
                e.stopPropagation(); // Prevent sorting when clicking resizer
                resizing = true;
                resizingCol = colIndex;
                startX = e.pageX;
                var table = document.querySelector('.dataframe');
                if (table && table.rows.length > 0) {
                    var headerCell = table.rows[0].cells[colIndex];
                    if (headerCell) {
                        startWidth = headerCell.offsetWidth;
                    }
                }
                document.addEventListener('mousemove', doResize);
                document.addEventListener('mouseup', stopResize);
                e.preventDefault();
            }
            function doResize(e) {
                if (!resizing || resizingCol === -1) return;
                var table = document.querySelector('.dataframe');
                if (!table || table.rows.length === 0) return;
                
                var diff = e.pageX - startX;
                var newWidth = Math.max(50, startWidth + diff); // Minimum width of 50px
                
                // Apply width to header and all cells in that column
                var headerCell = table.rows[0].cells[resizingCol];
                if (headerCell) {
                    headerCell.style.width = newWidth + 'px';
                    headerCell.style.minWidth = newWidth + 'px';
                    headerCell.style.maxWidth = newWidth + 'px';
                    // Ensure wrapping is enabled
                    headerCell.style.whiteSpace = 'normal';
                    headerCell.style.wordWrap = 'break-word';
                }
                
                // Apply to all data cells in this column
                for (var i = 1; i < table.rows.length; i++) {
                    var cell = table.rows[i].cells[resizingCol];
                    if (cell) {
                        cell.style.width = newWidth + 'px';
                        cell.style.minWidth = newWidth + 'px';
                        cell.style.maxWidth = newWidth + 'px';
                        // Ensure wrapping is enabled
                        cell.style.whiteSpace = 'normal';
                        cell.style.wordWrap = 'break-word';
                    }
                }
            }
            function stopResize() {
                resizing = false;
                resizingCol = -1;
                document.removeEventListener('mousemove', doResize);
                document.removeEventListener('mouseup', stopResize);
            }
            </script>
            '''
            style_html = """
            <style>
                body { font-family: Arial, sans-serif; background-color: #f4f4f9; transition: background-color 0.3s ease, color 0.3s ease; }
                body.dark-mode { background-color: #1e1e1e; color: #e0e0e0; }
                .table-wrapper {
                }
                table { border-collapse: collapse; width: 100%; margin: 20px 0; table-layout: auto; }
                th, td { 
                    border: 1px solid #ddd; 
                    padding: 8px; 
                    text-align: left; 
                    transition: background-color 0.3s ease, color 0.3s ease, border-color 0.3s ease;
                    word-wrap: break-word;
                    overflow-wrap: break-word;
                    hyphens: auto;
                    max-width: 500px;
                }
                body.dark-mode th, body.dark-mode td { border-color: #444; }
                th {
                    background-color: #f2f2f2;
                    position: sticky;
                    top: 0;
                    z-index: 2;
                    cursor: pointer;
                    user-select: none;
                    min-width: 80px;
                }
                body.dark-mode th { background-color: #2d2d2d; color: #e0e0e0; }
                th .resizer {
                    position: absolute;
                    top: 0;
                    right: 0;
                    width: 5px;
                    height: 100%;
                    cursor: col-resize;
                    user-select: none;
                    background: transparent;
                    z-index: 10;
                }
                th .resizer:hover {
                    background: rgba(0, 0, 0, 0.2);
                }
                body.dark-mode th .resizer:hover {
                    background: rgba(255, 255, 255, 0.2);
                }
                tr:nth-child(even) { background-color: #f9f9f9; }
                body.dark-mode tr:nth-child(even) { background-color: #2a2a2a; }
                body.dark-mode tr:nth-child(odd) { background-color: #1e1e1e; }
                img { max-width: 100px; height: auto; }
                img:hover {
                    transform: scale(1.5);
                    transition: transform 0.3s ease;
                    box-shadow: 0 4px 8px rgba(0,0,0,0.2);
                    cursor: pointer;
                }
                .legend {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                    margin-bottom: 20px;
                }
                .legend-item {
                    padding: 5px 10px;
                    border-radius: 5px;
                    font-weight: bold;
                    color: black;
                    transition: color 0.3s ease;
                }
                body.dark-mode .legend-item {
                    color: #e0e0e0;
                }
                .csam { background-color: #ff4c4c; } /* Light Red */
                .evidence { background-color: #ffc04c; } /* Light Orange */
                .interest { background-color: #ffff4c; } /* Light Yellow */
                .sender1 { background-color: #add8e6; } /* Light Blue */
                .sender2 { background-color: #d3d3d3; } /* Light Gray */
                .export-summary {
                    background-color: #f9f9f9;
                    border: 1px solid #ddd;
                    padding: 10px;
                    margin-bottom: 20px;
                    border-radius: 5px;
                    transition: background-color 0.3s ease, border-color 0.3s ease, color 0.3s ease;
                }
                body.dark-mode .export-summary {
                    background-color: #2d2d2d;
                    border-color: #444;
                    color: #e0e0e0;
                }
                .export-summary dl {
                    margin: 0;
                }
                .export-summary dt {
                    font-weight: bold;
                    margin-top: 10px;
                }
                .export-summary dd {
                    margin-left: 20px;
                }
               
                .summary-table {
                    width: auto;
                    border-collapse: collapse;
                    margin: 0;
                }
                .summary-table th, .summary-table td {
                    border: none;
                    padding: 2px 5px;
                    text-align: left;
                    vertical-align: top;
                }
                .summary-table th {
                    font-weight: bold;
                    white-space: nowrap;
                }
                .summary-table td {
                    word-wrap: break-word;
                }
                td {
                    white-space: normal;
                    word-break: break-word;
                }
                /* Specific column width constraints for better layout */
                td:has(img) {
                    max-width: 150px;
                }
                /* Prevent extremely wide columns while allowing wrapping */
                .dataframe td {
                    max-width: 500px;
                    min-width: 50px;
                }
                .dataframe th {
                    max-width: 500px;
                    min-width: 80px;
                }
                .hashes-table {
                    font-size: 12px;
                }
                .hashes-table th, .hashes-table td {
                    padding: 4px;
                }
                .media-container {
                    position: relative;
                    display: inline-block;
                }
                .media-type {
                    position: absolute;
                    top: 5px;
                    left: 5px;
                    background-color: rgba(0, 0, 0, 0.5);
                    color: white;
                    padding: 2px 5px;
                    font-size: 10px;
                    font-weight: bold;
                    border-radius: 3px;
                }
                /* NEW: Global narrower widths for specific columns (using :nth-child based on order in selected_fields) */
                th:nth-child(16), td:nth-child(16) { /* Example: If "IP" is 16th in headers */
                    max-width: 100px; overflow: hidden; text-overflow: ellipsis;
                }
                th:nth-child(17), td:nth-child(17) { /* Example: If "Port" is 17th */
                    width: 60px; max-width: 60px;
                }
                th:nth-child(11), td:nth-child(11) { /* Example: If "Media ID" is 11th in headers */
                    max-width: 200px; white-space: normal; word-wrap: break-word;
                }
                /* Dark mode styles for modal */
                .modal-content { transition: background-color 0.3s ease, border-color 0.3s ease, color 0.3s ease; }
                body.dark-mode .modal-content {
                    background-color: #2d2d2d !important;
                    border-color: #666 !important;
                    color: #e0e0e0 !important;
                }
                body.dark-mode #groupMembersModal {
                    background-color: rgba(0,0,0,0.7);
                }
                /* Dark mode styles for input and select elements */
                input, select, button {
                    transition: background-color 0.3s ease, color 0.3s ease, border-color 0.3s ease;
                }
                body.dark-mode input, body.dark-mode select {
                    background-color: #2d2d2d;
                    color: #e0e0e0;
                    border-color: #444;
                }
                body.dark-mode h2, body.dark-mode h3 {
                    color: #e0e0e0;
                }
            </style>
            """
            # Export Summary HTML
            scope_text = '; '.join([k.replace('scope_', '').capitalize() for k in options if k.startswith('scope_') and options[k]])
            fields_text = ', '.join(selected_fields)
            blur_text = 'CSAM' if options['blur_csam'] else 'All' if options['blur_all'] else 'None'
            exported_on = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            summary_html = f'''
            <div class="export-summary">
                <h3>Export Summary</h3>
                <table class="summary-table">
                    <tr><th>Scope:</th><td>{scope_text}</td></tr>
                    <tr><th>Format:</th><td>HTML</td></tr>
                    <tr><th>Sorted by:</th><td>{options['sort_by']}</td></tr>
                    <tr><th>Fields:</th><td>{fields_text}</td></tr>
                    <tr><th>Blur:</th><td>{blur_text}</td></tr>
                    <tr><th>Exported on:</th><td>{exported_on}</td></tr>
                    <tr><th>Total Messages:</th><td>{total_messages}</td></tr>
                    <tr><th>Unique Conversations:</th><td>{unique_convs}</td></tr>
                    <tr><th>Unique Users:</th><td>{unique_users}</td></tr>
                    <tr><th>Tagged Messages:</th><td>{tagged_messages}</td></tr>
                    <tr><th>Tag Breakdown:</th><td>{tag_breakdown}</td></tr>
                    <tr><th>Keyword Hits:</th><td>{keyword_hits}</td></tr>
                    <tr><th>Total Media:</th><td>{total_media}</td></tr>
                    <tr><th>Date Period:</th><td>{date_period}</td></tr>
                </table>
            </div>
            '''
            # Add "All Notes" section if notes are included
            all_notes_html = ''
            notes_script = ''
            include_notes = 'Notes' in selected_fields
            if include_notes:
                # Get all notes for conversations in export
                conv_ids_in_export = set()
                for msg_idx in messages_to_export:
                    if msg_idx < len(self.all_messages):
                        msg = self.all_messages[msg_idx]
                        conv_id = msg.get('conversation_id', '')
                        if conv_id:
                            conv_ids_in_export.add(conv_id)
                
                notes_to_export = {conv_id: self.conversation_notes[conv_id] 
                                   for conv_id in conv_ids_in_export 
                                   if conv_id in self.conversation_notes and self.conversation_notes[conv_id]}
                
                if notes_to_export:
                    all_notes_html = '''
            <div style='margin: 20px 0;'>
                <button id='toggleAllNotesBtn' onclick='toggleAllNotes()' style='padding: 10px 20px; background-color: #3498db; color: white; border: none; border-radius: 5px; cursor: pointer; font-size: 14px; margin-bottom: 10px;'>View All Notes</button>
            </div>
            <div id='allNotesSection' style='display: none; margin: 20px 0;'>
                <h3>All Investigative Notes</h3>
            '''
                    for conv_id, note in sorted(notes_to_export.items()):
                        # Get conversation display name (user1,user2 format)
                        conv_display_name = self._get_conversation_display_name(conv_id)
                        all_notes_html += f'''
                <div style='
                    background: linear-gradient(to right, #d4e6f1 0%, #e8f4f8 100%);
                    border-left: 4px solid #3498db;
                    padding: 12px 15px;
                    margin: 10px 0;
                    font-size: 13px;
                    line-height: 1.6;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    border-radius: 4px;'>
                    <div style='display: flex; align-items: flex-start;'>
                        <span style='font-size: 16px; margin-right: 8px; color: #2980b9;'>📝</span>
                        <div style='flex: 1;'>
                            <div style='font-weight: 600; color: #2c3e50; margin-bottom: 5px; font-size: 14px;'>
                                <strong>Conversation: {html.escape(str(conv_display_name))}</strong> - Investigative Note:
                            </div>
                            <div style='color: #34495e; white-space: pre-wrap;'>
                                {html.escape(note)}
                            </div>
                        </div>
                    </div>
                </div>
            '''
                    all_notes_html += '</div>'
                    
                    notes_script = '''
            <script>
            function toggleAllNotes() {
                const allNotesSection = document.getElementById('allNotesSection');
                const toggleButton = document.getElementById('toggleAllNotesBtn');
                if (allNotesSection.style.display === 'none' || allNotesSection.style.display === '') {
                    allNotesSection.style.display = 'block';
                    toggleButton.textContent = 'Hide All Notes';
                } else {
                    allNotesSection.style.display = 'none';
                    toggleButton.textContent = 'View All Notes';
                }
            }
            </script>
            '''
            
            # Full HTML
            html_content = f"""
            <html>
            <head>
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>SnapParser Export - {options['sort_by']} - {datetime.datetime.now().strftime('%Y-%m-%d')}</title>
                {style_html}
                {script_html}
                {notes_script}
            </head>
            <body>
                <h1>SnapParser Export Report</h1>
                {summary_html}
                {all_notes_html}
                {legend_html}
                {filter_html}
                {table_html}
                {hashes_html}
                <p>Generated by SnapParser v{APP_VERSION}</p>
            </body>
            </html>
            """
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
        elif options['format'] == 'CSV':
            df.to_csv(file_path, index=False)
        
        progress.setValue(total_steps)
        QApplication.processEvents()
        # Optional: Small delay if needed for users to see completion
        QTimer.singleShot(500, progress.close)  # Auto-close after 0.5s
        
        QMessageBox.information(self, "Export Successful", f"Exported {len(export_data)} messages to {file_path}")
    
    def show_help_dialog(self):
        dlg = HelpDialog(self.TAG_COLORS, self)
        dlg.exec_()

    def show_column_menu(self, pos):
        menu = QMenu(self)
        config_action = menu.addAction("Configure/Reorder Columns")
        action = menu.exec_(self.message_table.mapToGlobal(pos))
        if action == config_action:
            self.configure_columns()

    def configure_columns(self):
        dlg = ColumnConfigDialog(self.headers, self.column_order, self.hidden_columns, self)
        if dlg.exec_() == QDialog.Accepted:
            self.column_order, self.hidden_columns = dlg.get_config()
            self.save_config()
            self.apply_column_config()
    
    def apply_column_config(self):
        header = self.message_table.horizontalHeader()
        
        # 1. Apply order
        for target_visual, header_name in enumerate(self.column_order):
            try:
                logical_index = self.headers.index(header_name)
            except ValueError:
                continue 
            
            current_visual = header.visualIndex(logical_index)
            if current_visual != target_visual:
                 header.moveSection(current_visual, target_visual)

        # 2. Apply visibility
        for i in range(header.count()):
            # Get header text from model (QTableView doesn't have horizontalHeaderItem)
            header_text = self.message_table.model().headerData(i, Qt.Horizontal, Qt.DisplayRole)
            if header_text is None:
                header_text = self.headers[i] if i < len(self.headers) else ''
            self.message_table.setColumnHidden(i, header_text in self.hidden_columns)
                    
        # 3. Make Media column user-resizable (previously fixed)
        if "Media" in self.headers:
            media_logical_index = self.headers.index("Media")
            header = self.message_table.horizontalHeader()
            header.setSectionResizeMode(media_logical_index, QHeaderView.Interactive)
            # Ensure Media column is at least the default width (180px)
            if self.message_table.columnWidth(media_logical_index) < 180:
                self.message_table.setColumnWidth(media_logical_index, 180)


    def closeEvent(self, event):
        """
        Gracefully stop background threads, clean temp directories, save config,
        and allow the main window to close cleanly.
        """

        # 1. Attempt to stop the ZipLoaderThread safely
        try:
            if hasattr(self, "loader_thread") and self.loader_thread is not None:
                if self.loader_thread.isRunning():
                    # Politely request interruption
                    self.loader_thread.requestInterruption()

                    # Allow it time to exit cleanly
                    self.loader_thread.wait(1500)

                    # As a last resort, force terminate
                    if self.loader_thread.isRunning():
                        self.loader_thread.terminate()
                        self.loader_thread.wait()
        except Exception as e:
            logger.error(f"Error stopping loader thread: {e}")

        # 2. Cleanup persistent temp dirs
        try:
            if os.path.exists(self.media_extract_dir):
                shutil.rmtree(self.media_extract_dir, ignore_errors=True)
            if os.path.exists(self.thumb_dir):
                shutil.rmtree(self.thumb_dir, ignore_errors=True)
            logger.info("Temporary directories cleaned up.")
        except Exception as e:
            logger.error(f"Error cleaning persistent temp dirs: {e}")

        # 3. Cleanup leftover "case hashing" temp dirs (snap_conv_hash_*) and conv extract dirs
        try:
            import glob
            temp_base = tempfile.gettempdir()

            # Hashing dirs
            for path in glob.glob(os.path.join(temp_base, "snap_conv_hash_*")):
                shutil.rmtree(path, ignore_errors=True)

            # Extract dirs (only old ones)
            for path in glob.glob(os.path.join(temp_base, "snap_conv_*")):
                shutil.rmtree(path, ignore_errors=True)

        except Exception as e:
            logger.error(f"Error cleaning hash temp dirs: {e}")

        # 4. Save config
        self.save_config()

        # 5. Call base closeEvent ONCE (you were calling it twice)
        super().closeEvent(event)

        
if __name__ == '__main__':
    # Add main application logic here if needed
    import traceback
    try:
        app = QApplication(sys.argv)
        # Apply dark mode globally for QMessageBox and QFileDialog
        win = SnapParserMain()
        # Global stylesheet is already applied in apply_theme()
        win.show()
        sys.exit(app.exec_())
    except Exception as e:
        # Print to console even if logging is disabled
        error_msg = f"Fatal application error: {e}\n"
        error_msg += traceback.format_exc()
        print(error_msg, file=sys.stderr)
        # Also try to log if logger is available
        try:
            logger.critical(f"Fatal application error: {e}", exc_info=True)
        except:
            pass
        # Keep window open so user can see the error
        input("\nPress Enter to exit...")
        sys.exit(1)