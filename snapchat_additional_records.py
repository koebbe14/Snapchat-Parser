# -*- coding: utf-8 -*-
"""
Parse and display Snapchat legal production CSVs (non-chat) from the same ZIP as conversations.
Layout follows snapchat_master_production_records_parser_schema.md: optional NO DATA,
repeating blocks of legend + '===========================' + CSV table.
"""
from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import pandas as pd

from PyQt5.QtCore import QAbstractTableModel, QModelIndex, Qt, QThread, pyqtSignal
from PyQt5.QtGui import QBrush, QColor, QPalette, QPen
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMenu,
    QMessageBox,
    QProgressDialog,
    QPushButton,
    QScrollArea,
    QSplitter,
    QStackedWidget,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QTableView,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

_SEP_DASH_LINE = re.compile(r"^-{20,}$")
SEP_EQ_LINE = re.compile(r"^={15,}$")

TARGET_USERNAME_RE = re.compile(
    r'Target username\s+""([^""]+)""\s+is associated with User ID\s+""([^""]+)""', re.I
)

_LEGEND_BULLET = re.compile(r"^[\-\*•\u2022]\s*")

# Tree item UserRole: ("file", zpath, internal) or ("section", zpath, internal, section_index)
# UserRole + 1 on file items: last focused section index (int)
_ROLE_FILE = "file"
_ROLE_SECTION = "section"
# Public alias for callers building the same tree (e.g. export dialog).
ADDITIONAL_TREE_ROLE_FILE = _ROLE_FILE

_ROW_KEY_SEP = "\x1f"


def stable_row_key(zip_path: str, zpath: str, internal: str, section_index: int, row_index: int) -> str:
    """Stable JSON-safe row id for additional production CSV rows (survives tree navigation)."""
    return _ROW_KEY_SEP.join(
        [
            str(zip_path or ""),
            str(zpath or ""),
            str(internal or ""),
            str(int(section_index)),
            str(int(row_index)),
        ]
    )


def parse_stable_row_key(key: str) -> Optional[Tuple[str, str, str, int, int]]:
    if not key or _ROW_KEY_SEP not in key:
        return None
    parts = key.split(_ROW_KEY_SEP, 4)
    if len(parts) != 5:
        return None
    try:
        return (parts[0], parts[1], parts[2], int(parts[3]), int(parts[4]))
    except ValueError:
        return None


def apply_alternating_table_palette(table_view: QTableView, theme_manager) -> None:
    """Match main message table: transparent base, sender2 alternate, themed selection."""
    if theme_manager is None or table_view is None:
        return
    palette = table_view.palette()
    palette.setColor(QPalette.Text, QColor(theme_manager.get_color("text_primary")))
    palette.setColor(QPalette.WindowText, QColor(theme_manager.get_color("text_primary")))
    palette.setColor(QPalette.Base, QColor("transparent"))
    palette.setColor(QPalette.AlternateBase, QColor(theme_manager.get_color("sender2")))
    palette.setColor(QPalette.Window, QColor("transparent"))
    hover_color = "#e0e0e0" if not getattr(theme_manager, "dark_mode", False) else "#555555"
    palette.setColor(QPalette.Highlight, QColor(hover_color))
    palette.setColor(QPalette.HighlightedText, QColor(theme_manager.get_color("text_primary")))
    table_view.setPalette(palette)
    table_view.setAlternatingRowColors(True)


def tree_group_label_for_internal(internal: str) -> str:
    """Top-level tree group: parent folder of the CSV inside the innermost archive path."""
    parts = internal.split("!")
    logical = parts[-1].replace("\\", "/")
    parent = os.path.dirname(logical)
    if parent:
        base = os.path.basename(parent.rstrip("/"))
        return base if base else "Archive root"
    if len(parts) >= 2:
        return os.path.splitext(os.path.basename(parts[-2]))[0] or "Archive root"
    return "Archive root"


def fill_additional_file_tree_widget(
    tree: QTreeWidget,
    paths: List[Tuple[str, str]],
    responsive_map: Dict[Tuple[str, str], bool],
    *,
    checkable: bool = False,
    default_checked: bool = True,
) -> None:
    """
    Populate tree like AdditionalRecordsDialog: group folder → CSV basename.
    Only includes paths where responsive_map[(zpath, internal)] is True.
    When checkable, group rows use auto-tristate; leaves store UserRole (_ROLE_FILE, zpath, internal).
    """
    tree.clear()
    groups: Dict[str, List[Tuple[str, str]]] = {}
    for zpath, internal in paths:
        key = (zpath, internal)
        if not responsive_map.get(key, False):
            continue
        label = tree_group_label_for_internal(internal)
        groups.setdefault(label, []).append((zpath, internal))

    def sort_key(name: str) -> Tuple[int, str]:
        if name == "Archive root":
            return (1, name)
        return (0, name.lower())

    cs = Qt.Checked if default_checked else Qt.Unchecked
    for root_name in sorted(groups.keys(), key=sort_key):
        top = QTreeWidgetItem([root_name])
        top.setExpanded(True)
        if checkable:
            top.setFlags(top.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsAutoTristate)
            top.setCheckState(0, cs)
        for zpath, internal in sorted(groups[root_name], key=lambda x: x[1].lower()):
            child = QTreeWidgetItem([os.path.basename(internal)])
            child.setData(0, Qt.UserRole, (_ROLE_FILE, zpath, internal))
            if checkable:
                child.setFlags(child.flags() | Qt.ItemIsUserCheckable)
                child.setCheckState(0, cs)
            top.addChild(child)
        tree.addTopLevelItem(top)


def _colset(columns: List[str]) -> frozenset:
    return frozenset(str(c).strip().lower() for c in columns if str(c).strip())


_GENERIC_PREAMBLE = re.compile(
    r"^(snapchat account information|subscriber information|data legend|the following table)\b",
    re.I,
)


def _clean_preamble_title(sec: ParsedSection) -> str:
    t = (sec.preamble_title or "").strip().replace("\n", " ")
    while "  " in t:
        t = t.replace("  ", " ")
    if len(t) < 4 or _GENERIC_PREAMBLE.match(t):
        return ""
    return t[:56] + ("…" if len(t) > 56 else "")


def _ip_data_section_label(cls: frozenset) -> str:
    """Labels align with ip_data delimiter blocks (auth, login history, etc.)."""
    if "authentication_device_descriptor" in cls:
        return "Login history (device & challenges)"
    if "login_count" in cls or "startup_time" in cls or "credential_compromised" in cls:
        return "Login session metadata"
    if "completed_authentication" in cls or "login_status" in cls:
        return "Login & authentication sessions"
    if "source_port_number" in cls and "media_id" in cls:
        return "Media connections (IP, port & media)"
    if "sender_name" in cls or "action_type" in cls or "recipient" in cls:
        return "User connections & actions"
    if "verification_method" in cls:
        return "Verification & security events"
    if "first_seen_time" in cls or "last_seen_time" in cls:
        return "IP retention (first & last seen)"
    if "auth_session_id" in cls or "cloud_account_id" in cls or "client_source_port" in cls:
        return "Authentication events (IP & session)"
    if cls <= frozenset({"ip", "timestamp", "user_agent"}) or (
        len(cls) <= 4 and "ip" in cls and "timestamp" in cls and "user_agent" in cls
    ):
        return "Network activity (IP & user agent)"
    return "IP-related records"


def _subscriber_info_section_label(cls: frozenset) -> str:
    if "username" in cls and "user_id" in cls:
        return "Core account information"
    if "birthdate" in cls or "last_active" in cls or "follower_count" in cls or "2fa_status" in cls:
        return "Verification & activity"
    if "snap_privacy" in cls or "story_privacy" in cls:
        return "Privacy settings"
    if "bitmoji" in "".join(cls):
        return "Bitmoji"
    if "email_address" in cls and len(cls) <= 5:
        return "Community email"
    return "Subscriber information"


def _subscriber_account_change_label(cls: frozenset) -> str:
    if "action" in cls and "reason" in cls:
        return "Account field change log"
    if "old_value" in cls and "new_value" in cls:
        return "Account value snapshot"
    return "Account changes"


def _basename_default_section_label(base: str) -> Optional[str]:
    """Single-purpose production files (one typical table)."""
    return {
        "detected_url.csv": "Detected URLs",
        "device_advertising_id.csv": "Device advertising identifiers",
        "geo_locations.csv": "Geolocation records",
        "loc_priv_sets.csv": "Location privacy settings",
        "memories.csv": "Memories metadata",
        "public_profile.csv": "Public profile",
        "public_story.csv": "Public story",
        "push_tokens.csv": "Push notification tokens",
        "reported_comments.csv": "Reported comments",
        "reported_conversations.csv": "Reported conversations",
        "reported_group_conversations.csv": "Reported group conversations",
        "reported_media_metadata.csv": "Reported media",
        "reported_public_profile.csv": "Reported public profile",
        "shared_story.csv": "Shared story",
        "story.csv": "Story",
        "ai_conversations.csv": "AI conversations",
    }.get(base)


def logical_tree_section_label(basename: str, sec: ParsedSection, section_index: int) -> str:
    """Human-readable tree label for one parsed section."""
    base = os.path.basename(basename).lower()
    cls = _colset(list(sec.dataframe.columns))

    if base == "ip_data.csv":
        return _ip_data_section_label(cls)
    if base == "subscriber_info.csv":
        return _subscriber_info_section_label(cls)
    if base == "subscriber_account_change_history.csv":
        return _subscriber_account_change_label(cls)

    single = _basename_default_section_label(base)
    if single is not None:
        pre = _clean_preamble_title(sec)
        if pre and base not in ("push_tokens.csv", "device_advertising_id.csv"):
            return f"{single} — {pre}"
        return single

    pre = _clean_preamble_title(sec)
    if pre:
        return pre
    stem = os.path.splitext(base)[0].replace("_", " ").strip()
    pretty = (stem[:1].upper() + stem[1:]) if stem else "Records"
    return f"{pretty} ({section_index + 1})"


def visible_section_tree_rows(parsed: ParsedProductionRecord) -> List[Tuple[int, str]]:
    """(original_section_index, display_label) for each parsed section (including 0-row tables)."""
    pairs: List[Tuple[int, str]] = []
    basename = parsed.basename
    for i, sec in enumerate(parsed.sections):
        pairs.append((i, logical_tree_section_label(basename, sec, i)))
    counts: Dict[str, int] = {}
    out: List[Tuple[int, str]] = []
    for orig_i, L in pairs:
        key = L.lower()
        counts[key] = counts.get(key, 0) + 1
        if counts[key] > 1:
            out.append((orig_i, f"{L} ({counts[key]})"))
        else:
            out.append((orig_i, L))
    return out


def _legend_norm_token(s: str) -> str:
    """Compare column names to legend keys ignoring spaces, underscores, hyphens."""
    return re.sub(r"[^\w]+", "", (s or "").lower())


def _merge_legend_keys(store: Dict[str, str], left_blob: str, right: str) -> None:
    """Associate description with each comma/semicolon-separated key on the left (first description wins)."""
    right = (right or "").strip()
    if not right:
        return
    parts = [p.strip().strip("\"'") for p in re.split(r"[,;|]", left_blob) if p.strip()]
    if not parts:
        return
    for k in parts:
        kl = k.lower()
        if not kl or kl in ("the following", "see below", "data legend"):
            continue
        store.setdefault(kl, right)


def _legend_column_descriptions(legend_text: str) -> Dict[str, str]:
    """Map lowercase column name -> description string from production legend lines."""
    out: Dict[str, str] = {}
    if not legend_text:
        return out
    for raw in legend_text.splitlines():
        ln = raw.strip()
        if not ln:
            continue
        ln = _LEGEND_BULLET.sub("", ln).strip()
        if not ln:
            continue
        low = ln.lower()
        if low.startswith("data legend") or low.startswith("==="):
            continue
        # Tab: "column<TAB>description" (no colon)
        if "\t" in ln and ":" not in ln:
            cells = [c.strip() for c in ln.split("\t") if c.strip()]
            if len(cells) >= 2 and len(cells[0]) < 120:
                _merge_legend_keys(out, cells[0], " ".join(cells[1:]).strip())
            continue
        if ":" in ln:
            left, right = ln.split(":", 1)
            left, right = left.strip(), right.strip()
            if not right:
                continue
            _merge_legend_keys(out, left, right)
            continue
        if " - " in ln:
            left, right = ln.split(" - ", 1)
            left, right = left.strip(), right.strip()
            if left and right and len(left) < 200:
                _merge_legend_keys(out, left, right)
    return out


def column_tooltips_from_legend(legend_text: str, columns: List[str]) -> List[str]:
    """
    One tooltip per column: description for that column only (no cross-field bleed).
    Matches legend keys exactly or by normalized token (spaces/underscores ignored).
    """
    mapping = _legend_column_descriptions(legend_text)
    norm_reverse: Dict[str, str] = {}
    for k, v in mapping.items():
        nk = _legend_norm_token(k)
        if nk and nk not in norm_reverse:
            norm_reverse[nk] = v

    fallback = "No per-column description found in the production legend."
    tips: List[str] = []
    for col in columns:
        col_s = str(col).strip()
        col_l = col_s.lower()
        tip = mapping.get(col_l)
        if tip is None:
            tip = norm_reverse.get(_legend_norm_token(col_l))
        tips.append(tip.strip() if tip else fallback)
    return tips


def header_tooltips_for_section(sec: ParsedSection) -> List[str]:
    """Column header tooltips: per-column legend text only."""
    return column_tooltips_from_legend(sec.legend_text, list(sec.dataframe.columns))


@dataclass
class ParsedSection:
    preamble_title: str
    legend_text: str
    columns: List[str]
    dataframe: pd.DataFrame


@dataclass
class ParsedProductionRecord:
    basename: str
    internal_path: str
    target_username: Optional[str] = None
    user_id: Optional[str] = None
    date_range_line: Optional[str] = None
    banner_text: str = ""
    responsive: bool = True
    no_data_note: str = ""
    sections: List[ParsedSection] = field(default_factory=list)
    raw_error: str = ""


def _strip_eq_line(line: str) -> bool:
    s = line.strip()
    return bool(SEP_EQ_LINE.match(s))


def _is_dash_sep(line: str) -> bool:
    return bool(_SEP_DASH_LINE.match(line.strip()))


def decode_csv_bytes(raw: bytes) -> str:
    if not raw:
        return ""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin1", errors="replace")


def parse_production_record_text(basename: str, internal_path: str, text: str) -> ParsedProductionRecord:
    out = ParsedProductionRecord(basename=basename, internal_path=internal_path, banner_text="")
    if not text or not str(text).strip():
        out.responsive = False
        out.no_data_note = "Empty file."
        return out

    lines = text.splitlines()
    upper = text.upper()

    first_sep = None
    for idx, ln in enumerate(lines):
        if _is_dash_sep(ln):
            first_sep = idx
            break

    # Whole-file empty production: Snapchat returns banner + NO DATA with no delimiter blocks
    if first_sep is None:
        out.banner_text = text[:4000].strip()
        if "NO RESPONSIVE DATA FOUND" in upper:
            out.responsive = False
            pos = upper.find("NO RESPONSIVE DATA FOUND")
            out.no_data_note = text[pos : pos + 1200].strip()
            return out
        out.banner_text = text[:2000].strip()
        out.raw_error = "Expected production section delimiter (---------------------------) not found."
        return out

    out.banner_text = "\n".join(lines[:first_sep]).strip()

    joined_head = out.banner_text + "\n" + "\n".join(lines[first_sep : min(len(lines), first_sep + 5)])
    m = TARGET_USERNAME_RE.search(joined_head)
    if m:
        out.target_username = m.group(1).strip()
        out.user_id = m.group(2).strip()
    for ln in lines[:30]:
        if "date range searched" in ln.lower():
            out.date_range_line = ln.strip()
            break

    i = first_sep
    n = len(lines)
    while i < n:
        if not _is_dash_sep(lines[i]):
            i += 1
            continue
        i += 1
        preamble_lines: List[str] = []
        while i < n and not _strip_eq_line(lines[i]) and not _is_dash_sep(lines[i]):
            preamble_lines.append(lines[i])
            i += 1
        if i >= n:
            break
        if _is_dash_sep(lines[i]):
            continue
        if not _strip_eq_line(lines[i]):
            continue
        i += 1
        while i < n and not lines[i].strip():
            i += 1
        if i >= n:
            break
        try:
            header = next(csv.reader([lines[i]]))
        except Exception:
            i += 1
            continue
        i += 1
        header = [h.strip() for h in header]
        rows: List[List[str]] = []
        while i < n:
            if _is_dash_sep(lines[i]):
                break
            try:
                row = next(csv.reader([lines[i]]))
            except Exception:
                i += 1
                continue
            if not any(c.strip() for c in row):
                i += 1
                continue
            if len(row) == 1 and "NO RESPONSIVE DATA FOUND" in row[0].strip().upper():
                i += 1
                continue
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                row = row[: len(header)]
            rows.append(row)
            i += 1

        title_line = ""
        for pl in preamble_lines:
            s = pl.strip()
            if s and "data legend" not in s.lower():
                title_line = s[:120]
                break
        if not title_line:
            title_line = (preamble_lines[0].strip()[:120] if preamble_lines else "(section)") or "(section)"

        legend_text = "\n".join(preamble_lines).strip()

        df = pd.DataFrame(rows, columns=header) if rows else pd.DataFrame(columns=header)
        for c in df.columns:
            df[c] = df[c].astype(str).replace({"nan": ""})
        out.sections.append(
            ParsedSection(
                preamble_title=title_line,
                legend_text=legend_text,
                columns=list(header),
                dataframe=df,
            )
        )
    return out


class DataFrameTableModel(QAbstractTableModel):
    def __init__(
        self,
        df: Optional[pd.DataFrame] = None,
        header_tooltips: Optional[List[str]] = None,
        zip_path: Optional[str] = None,
        zpath: Optional[str] = None,
        internal: Optional[str] = None,
        section_index: int = 0,
        main_window: Any = None,
    ):
        super().__init__()
        self._df = df if df is not None else pd.DataFrame()
        self._header_tooltips = list(header_tooltips or [])
        self._zip_path = zip_path
        self._zpath = zpath
        self._internal = internal
        self._section_index = int(section_index)
        self._main = main_window

    def set_dataframe(self, df: pd.DataFrame):
        self.beginResetModel()
        self._df = df if df is not None else pd.DataFrame()
        self.endResetModel()

    def update_row_context(
        self,
        zip_path: Optional[str],
        zpath: Optional[str],
        internal: Optional[str],
        section_index: int,
        main_window: Any = None,
    ):
        self._zip_path = zip_path
        self._zpath = zpath
        self._internal = internal
        self._section_index = int(section_index)
        if main_window is not None:
            self._main = main_window

    def row_key_for_row(self, row: int) -> Optional[str]:
        if self._df.empty or self._zip_path is None or self._zpath is None or self._internal is None:
            return None
        if row < 0 or row >= len(self._df):
            return None
        return stable_row_key(self._zip_path, self._zpath, self._internal, self._section_index, row)

    def _tags_for_row(self, row: int) -> Set[str]:
        rk = self.row_key_for_row(row)
        if not rk or self._main is None:
            return set()
        raw = getattr(self._main, "additional_record_tags", {}).get(rk)
        if not raw:
            return set()
        return set(raw) if not isinstance(raw, set) else set(raw)

    def _tag_priority_background(self, tags: Set[str]) -> Optional[QColor]:
        if not tags or self._main is None or not hasattr(self._main, "TAG_COLORS"):
            return None
        TAG = self._main.TAG_COLORS
        for t in ["CSAM", "Evidence", "Child Notable/Age Difficult", "Of Interest"]:
            if t in tags and t in TAG:
                return QColor(TAG[t].lighter(130))
        colored = [x for x in sorted(tags) if x in TAG]
        if colored:
            return QColor(TAG[colored[0]].lighter(130))
        return None

    def _alternating_background(self, row: int) -> Optional[QColor]:
        if self._main is None or not hasattr(self._main, "theme_manager"):
            return None
        alt = row % 2 == 1
        return QColor(self._main.theme_manager.get_color("sender2" if alt else "sender1"))

    def _background_for_row(self, row: int) -> Optional[QColor]:
        if self._df.empty or row < 0 or row >= len(self._df):
            return None
        tags = self._tags_for_row(row)
        bg = self._tag_priority_background(tags)
        if bg is not None:
            return bg
        return self._alternating_background(row)

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._df)

    def columnCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return max(1, len(self._df.columns))

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or self._df.empty:
            return None
        if role == Qt.DisplayRole:
            try:
                v = self._df.iat[index.row(), index.column()]
                return "" if pd.isna(v) else str(v)
            except Exception:
                return None
        if role == Qt.UserRole:
            return self.row_key_for_row(index.row())
        if role == Qt.BackgroundRole:
            return self._background_for_row(index.row())
        if role == Qt.ForegroundRole:
            if self._main is not None and hasattr(self._main, "theme_manager"):
                return QColor(self._main.theme_manager.get_color("text_table"))
            return None
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Vertical:
            if role == Qt.DisplayRole:
                return str(section + 1)
            if role == Qt.BackgroundRole:
                if 0 <= section < len(self._df):
                    return self._background_for_row(section)
                return None
            if role == Qt.ForegroundRole:
                if self._main is not None and hasattr(self._main, "theme_manager"):
                    return QColor(self._main.theme_manager.get_color("text_table"))
                return None
            return None

        cols = list(self._df.columns) if not self._df.empty else []
        if role == Qt.DisplayRole:
            if self._df.empty:
                return ""
            return str(cols[section]) if 0 <= section < len(cols) else ""
        if role == Qt.BackgroundRole:
            if self._main is not None and hasattr(self._main, "theme_manager"):
                return QColor(self._main.theme_manager.get_color("bg_table"))
            return None
        if role == Qt.ForegroundRole:
            if self._main is not None and hasattr(self._main, "theme_manager"):
                return QColor(self._main.theme_manager.get_color("text_primary"))
            return None
        if role in (Qt.ToolTipRole, Qt.StatusTipRole):
            if 0 <= section < len(self._header_tooltips):
                return self._header_tooltips[section]
            if 0 <= section < len(cols):
                return str(cols[section])
            return None
        return None


class ProductionRecordCellDelegate(QStyledItemDelegate):
    """Draws investigative cell/selection borders for additional production CSV table."""

    def __init__(self, table: QTableView, dialog: "AdditionalRecordsDialog", main_window: Any):
        super().__init__(table)
        self._table = table
        self._dialog = dialog
        self._main = main_window

    def paint(self, painter, option, index):
        # Explicitly paint model BackgroundRole so tag colors are visible
        # even when alternatingRowColors is enabled (palette overrides model otherwise)
        bg = index.data(Qt.BackgroundRole)
        if bg is not None:
            painter.save()
            if option.state & QStyle.State_Selected:
                painter.fillRect(option.rect, option.palette.highlight())
            else:
                painter.fillRect(option.rect, bg)
            painter.restore()
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        super().paint(painter, opt, index)
        if self._main is None:
            return
        row, col = index.row(), index.column()
        model = index.model()
        border_color = self._main.theme_manager.get_color("cell_border")
        border_color_obj = QColor(border_color)
        painter.save()
        pen = QPen(border_color_obj, 5)
        painter.setPen(pen)
        rect = option.rect

        ctx = self._dialog.active_section_context()
        if ctx:
            zp, inter, sec_i = ctx
            for tup in getattr(self._main, "additional_record_selection_borders", set()):
                if len(tup) != 7:
                    continue
                bzp, bint, bsec, min_r, max_r, min_c, max_c = tup
                if bzp != zp or bint != inter or int(bsec) != int(sec_i):
                    continue
                if min_r <= row <= max_r and min_c <= col <= max_c:
                    if row == min_r:
                        painter.drawLine(rect.left(), rect.top(), rect.right(), rect.top())
                    if row == max_r:
                        painter.drawLine(rect.left(), rect.bottom(), rect.right(), rect.bottom())
                    if col == min_c:
                        painter.drawLine(rect.left(), rect.top(), rect.left(), rect.bottom())
                    if col == max_c:
                        painter.drawLine(rect.right(), rect.top(), rect.right(), rect.bottom())
                    painter.restore()
                    return

        row_key = model.data(index, Qt.UserRole) if model else None
        if row_key and (row_key, col) in getattr(self._main, "additional_record_cell_borders", set()):
            painter.drawRect(rect.adjusted(1, 1, -1, -1))
        painter.restore()


class _ParseRecordThread(QThread):
    finished_ok = pyqtSignal(object)
    failed = pyqtSignal(str)

    def __init__(self, basename: str, internal_path: str, text: str):
        super().__init__()
        self._basename = basename
        self._internal_path = internal_path
        self._text = text

    def run(self):
        try:
            result = parse_production_record_text(self._basename, self._internal_path, self._text)
            self.finished_ok.emit(result)
        except Exception as e:
            self.failed.emit(str(e))


class AdditionalCsvResponsiveScanThread(QThread):
    """Classify each additional CSV as responsive (tabular production) or not."""

    progress = pyqtSignal(int, int)
    finished_scan = pyqtSignal(object)
    failed = pyqtSignal(str)

    def __init__(
        self,
        paths: List[Tuple[str, str]],
        read_zip_bytes: Callable[[str, str], Optional[bytes]],
    ):
        super().__init__()
        self._paths = paths
        self._read_zip_bytes = read_zip_bytes

    def run(self):
        result: Dict[Tuple[str, str], bool] = {}
        total = len(self._paths)
        try:
            for i, key in enumerate(self._paths):
                if self.isInterruptionRequested():
                    return
                zpath, internal = key
                raw = self._read_zip_bytes(zpath, internal)
                if not raw:
                    result[key] = False
                    self.progress.emit(i + 1, total)
                    continue
                text = decode_csv_bytes(raw)
                basename = os.path.basename(internal)
                parsed = parse_production_record_text(basename, internal, text)
                result[key] = parsed.responsive
                self.progress.emit(i + 1, total)
            self.finished_scan.emit(result)
        except Exception as e:
            self.failed.emit(str(e))


class AdditionalRecordsDialog(QDialog):
    """
    Inspect additional production CSVs. Lazy-parse on selection; heavy files parse in background.
    """

    LARGE_FILE_BYTES = 600_000
    _STACK_TABLE = 0
    _STACK_DETAIL = 1

    def __init__(
        self,
        zip_path: str,
        additional_csv_paths: List[Tuple[str, str]],
        read_zip_bytes: Callable[[str, str], Optional[bytes]],
        parent=None,
        dark_mode: bool = False,
        theme_stylesheet_hook: Optional[Callable[[], str]] = None,
        theme_manager=None,
        main_window: Any = None,
    ):
        super().__init__(parent)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setWindowTitle("Additional Records (production CSVs)")
        self._apply_initial_geometry()
        self._zip_path = zip_path
        self._additional = list(additional_csv_paths or [])
        self._read_zip_bytes = read_zip_bytes
        self._dark = dark_mode
        self._theme_hook = theme_stylesheet_hook
        self._theme_manager = theme_manager
        self._main_window = main_window

        self._ctx_zpath: Optional[str] = None
        self._ctx_internal: Optional[str] = None
        self._ctx_section_idx: int = 0

        self._responsive_map: Dict[Tuple[str, str], bool] = {}
        self._scan_thread: Optional[AdditionalCsvResponsiveScanThread] = None
        self._scan_complete = not bool(self._additional)
        self._parse_cache: Dict[Tuple[str, str], ParsedProductionRecord] = {}
        self._pending_file_item: Optional[QTreeWidgetItem] = None
        self._table_model = DataFrameTableModel(
            pd.DataFrame(), None, zip_path, None, None, 0, main_window
        )
        self._current_section_index: int = 0

        root = QVBoxLayout(self)

        split = QSplitter(Qt.Horizontal)

        left = QWidget()
        lv = QVBoxLayout(left)
        self._scan_label = QLabel("")
        self._scan_label.setWordWrap(True)
        lv.addWidget(self._scan_label)
        self._tree = QTreeWidget()
        self._tree.setHeaderLabels(["File"])
        self._tree.setMinimumWidth(300)
        lv.addWidget(self._tree, 1)
        split.addWidget(left)

        right = QWidget()
        rv = QVBoxLayout(right)
        top_row = QHBoxLayout()
        top_row.addStretch()
        self._export_btn = QPushButton("Export…")
        self._export_btn.setToolTip("Export the current section to a CSV file…")
        self._export_btn.setMinimumHeight(30)
        self._export_btn.setMinimumWidth(88)
        self._export_btn.setObjectName("additionalRecordsExportBtn")
        self._export_btn.clicked.connect(self._export_current_section)
        top_row.addWidget(self._export_btn)
        rv.addLayout(top_row)

        self._stack = QStackedWidget()
        self._table = QTableView()
        self._table.setModel(self._table_model)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self._table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self._table.setSortingEnabled(False)
        self._cell_delegate = ProductionRecordCellDelegate(self._table, self, main_window)
        self._table.setItemDelegate(self._cell_delegate)
        self._stack.addWidget(self._table)

        detail_wrap = QWidget()
        dv = QVBoxLayout(detail_wrap)
        self._detail_label = QLabel("")
        self._detail_label.setWordWrap(True)
        self._detail_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self._detail_label)
        dv.addWidget(scroll)
        self._stack.addWidget(detail_wrap)

        rv.addWidget(self._stack, 1)

        split.addWidget(right)
        split.setStretchFactor(1, 1)
        root.addWidget(split)

        box = QDialogButtonBox(QDialogButtonBox.Close)
        box.rejected.connect(self.reject)
        box.accepted.connect(self.accept)
        root.addWidget(box)

        self._tree.currentItemChanged.connect(self._on_tree_change)

        self._parse_thread: Optional[_ParseRecordThread] = None
        self._progress: Optional[QProgressDialog] = None
        self._last_parsed: Optional[ParsedProductionRecord] = None

        self._table.setContextMenuPolicy(Qt.CustomContextMenu)
        self._table.customContextMenuRequested.connect(self._on_table_context_menu)

        if callable(self._theme_hook):
            try:
                self.setStyleSheet(self._theme_hook())
            except Exception:
                pass

        self.refresh_table_theme()
        self._wire_selection_tracking()

        self._show_detail("Select a file on the left.")

        if not self._additional:
            self._show_detail(
                "No eligible CSV files were found in this archive (excluding conversations.csv "
                "and conversation_list.csv)."
            )
        else:
            self._start_responsive_scan()

    def _apply_initial_geometry(self) -> None:
        screen = QApplication.primaryScreen()
        if screen:
            avail = screen.availableGeometry()
            dw = max(40, int(avail.width() * 0.04))
            dh = max(40, int(avail.height() * 0.04))
            self.setGeometry(avail.adjusted(dw, dh, -dw, -dh))
        else:
            self.resize(1200, 720)

    def refresh_table_theme(self) -> None:
        if self._theme_manager is not None:
            apply_alternating_table_palette(self._table, self._theme_manager)
        m = self._table.model()
        if m is not None and m.rowCount() > 0:
            m.headerDataChanged.emit(Qt.Vertical, 0, m.rowCount() - 1)
        if m is not None and m.columnCount() > 0:
            m.headerDataChanged.emit(Qt.Horizontal, 0, m.columnCount() - 1)
        self._table.viewport().update()

    def active_section_context(self) -> Optional[Tuple[str, str, int]]:
        if self._stack.currentIndex() != self._STACK_TABLE:
            return None
        if not self._ctx_zpath or not self._ctx_internal:
            return None
        return (self._ctx_zpath, self._ctx_internal, int(self._ctx_section_idx))

    def _wire_selection_tracking(self) -> None:
        sm = self._table.selectionModel()
        if sm is None or self._main_window is None:
            return
        if hasattr(self._main_window, "_sync_additional_records_selection_keys"):
            try:
                sm.selectionChanged.disconnect(self._on_selection_changed)
            except TypeError:
                pass
            sm.selectionChanged.connect(self._on_selection_changed)

    def _on_selection_changed(self, _selected, _deselected) -> None:
        if self._main_window is not None and hasattr(self._main_window, "_sync_additional_records_selection_keys"):
            self._main_window._sync_additional_records_selection_keys(self)

    def _on_table_context_menu(self, pos) -> None:
        if self._main_window is not None and hasattr(self._main_window, "additional_records_table_ctx_menu"):
            self._main_window.additional_records_table_ctx_menu(self, pos)

    def _show_detail(self, text: str):
        self._detail_label.setText(text)
        self._stack.setCurrentIndex(self._STACK_DETAIL)

    def _show_table(self):
        self._stack.setCurrentIndex(self._STACK_TABLE)

    def _start_responsive_scan(self):
        self._scan_complete = False
        self._responsive_map.clear()
        self._parse_cache.clear()
        self._scan_label.setText("Scanning production files for responsive data…")
        self._tree.clear()
        self._scan_thread = AdditionalCsvResponsiveScanThread(self._additional, self._read_zip_bytes)
        self._scan_thread.progress.connect(self._on_scan_progress)
        self._scan_thread.finished_scan.connect(self._on_scan_finished)
        self._scan_thread.failed.connect(self._on_scan_failed)
        self._scan_thread.start()

    def _on_scan_progress(self, done: int, total: int):
        self._scan_label.setText(f"Scanning production files… ({done}/{total})")

    def _on_scan_finished(self, result: object):
        self._scan_thread = None
        self._scan_complete = True
        self._responsive_map = result if isinstance(result, dict) else {}
        self._scan_label.setText("")
        self._populate_tree()

    def _on_scan_failed(self, msg: str):
        self._scan_thread = None
        self._scan_complete = True
        self._scan_label.setText("Scan incomplete; no files listed until you re-open this dialog.")
        self._responsive_map = {}
        QMessageBox.warning(self, "Scan", msg)
        self._populate_tree()

    def _populate_tree(self):
        self._parse_cache.clear()
        self._last_parsed = None
        self._pending_file_item = None
        self._reset_table()
        fill_additional_file_tree_widget(
            self._tree, self._additional, self._responsive_map, checkable=False
        )

        if self._scan_complete and self._tree.topLevelItemCount() == 0 and self._additional:
            self._show_detail(
                "No production CSVs with responsive tabular data were found in this archive."
            )
        elif self._tree.topLevelItemCount() > 0:
            self._show_detail("Select a file on the left.")

    def _reset_table(self):
        self._ctx_zpath = None
        self._ctx_internal = None
        self._ctx_section_idx = 0
        self._table_model = DataFrameTableModel(
            pd.DataFrame(), None, self._zip_path, None, None, 0, self._main_window
        )
        self._table.setModel(self._table_model)
        self._cell_delegate = ProductionRecordCellDelegate(self._table, self, self._main_window)
        self._table.setItemDelegate(self._cell_delegate)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._wire_selection_tracking()

    def _abort_thread(self):
        if self._parse_thread and self._parse_thread.isRunning():
            self._parse_thread.requestInterruption()
            self._parse_thread.quit()
            self._parse_thread.wait(3000)

    def _abort_scan_thread(self):
        if self._scan_thread and self._scan_thread.isRunning():
            self._scan_thread.requestInterruption()
            self._scan_thread.quit()
            self._scan_thread.wait(3000)
        self._scan_thread = None

    def closeEvent(self, event):
        self._abort_thread()
        self._abort_scan_thread()
        super().closeEvent(event)

    def _on_tree_change(self, current: QTreeWidgetItem, _prev: QTreeWidgetItem):
        if not current:
            return
        data = current.data(0, Qt.UserRole)
        if not data or not isinstance(data, tuple) or len(data) < 3:
            return
        kind = data[0]
        if kind == _ROLE_FILE:
            _, zpath, internal = data
            self._on_file_item_selected(current, zpath, internal)
            return
        if kind == _ROLE_SECTION and len(data) >= 4:
            _, zpath, internal, sec_idx = data[0], data[1], data[2], int(data[3])
            self._on_section_item_selected(current, zpath, internal, sec_idx)

    def _on_file_item_selected(self, item: QTreeWidgetItem, zpath: str, internal: str):
        if item.childCount() > 0:
            last_orig = item.data(0, Qt.UserRole + 1)
            if last_orig is None:
                ch0 = item.child(0)
                d0 = ch0.data(0, Qt.UserRole) if ch0 else None
                if d0 and d0[0] == _ROLE_SECTION and len(d0) >= 4:
                    last_orig = int(d0[3])
                else:
                    last_orig = 0
            try:
                last_o = int(last_orig)
            except (TypeError, ValueError):
                last_o = 0
            ch: Optional[QTreeWidgetItem] = None
            for ci in range(item.childCount()):
                row = item.child(ci)
                d = row.data(0, Qt.UserRole)
                if d and d[0] == _ROLE_SECTION and len(d) >= 4 and int(d[3]) == last_o:
                    ch = row
                    break
            if ch is None:
                ch = item.child(0)
            d = ch.data(0, Qt.UserRole)
            if d and d[0] == _ROLE_SECTION and len(d) >= 4:
                sec_idx = int(d[3])
                self._tree.blockSignals(True)
                self._tree.setCurrentItem(ch)
                self._tree.blockSignals(False)
                self._on_section_item_selected(ch, zpath, internal, sec_idx)
            return
        self._load_file_into_tree_item(item, zpath, internal)

    def _on_section_item_selected(self, item: QTreeWidgetItem, zpath: str, internal: str, sec_idx: int):
        parent = item.parent()
        if parent:
            parent.setData(0, Qt.UserRole + 1, sec_idx)
        parsed = self._parse_cache.get((zpath, internal))
        if not parsed or sec_idx < 0 or sec_idx >= len(parsed.sections):
            return
        self._last_parsed = parsed
        self._current_section_index = sec_idx
        self._ctx_zpath = zpath
        self._ctx_internal = internal
        self._ctx_section_idx = sec_idx
        self._apply_section_to_table(parsed.sections[sec_idx])

    def _load_file_into_tree_item(self, file_item: QTreeWidgetItem, zpath: str, internal: str):
        key = (zpath, internal)
        if key in self._parse_cache:
            self._apply_parsed_to_file_item(file_item, self._parse_cache[key])
            return

        self._abort_thread()
        self._pending_file_item = file_item
        basename = os.path.basename(internal)
        raw = self._read_zip_bytes(zpath, internal)
        if not raw:
            self._pending_file_item = None
            self._last_parsed = None
            self._show_detail(f"Could not read bytes for:\n{internal}")
            return

        if len(raw) >= self.LARGE_FILE_BYTES:
            self._progress = QProgressDialog("Parsing large production file…", "Cancel", 0, 0, self)
            self._progress.setWindowModality(Qt.WindowModal)
            self._progress.setCancelButton(None)
            self._progress.show()
            QApplication.processEvents()

            text = decode_csv_bytes(raw)
            self._parse_thread = _ParseRecordThread(basename, internal, text)
            self._parse_thread.finished_ok.connect(self._on_parsed)
            self._parse_thread.failed.connect(self._on_parse_failed)
            self._parse_thread.start()
            return

        text = decode_csv_bytes(raw)
        parsed = parse_production_record_text(basename, internal, text)
        self._on_parsed_sync(parsed)

    def _on_parse_failed(self, msg: str):
        if self._progress:
            self._progress.hide()
            self._progress = None
        self._pending_file_item = None
        self._last_parsed = None
        QMessageBox.warning(self, "Parse error", msg)
        self._show_detail(f"Parse error:\n{msg}")

    def _on_parsed(self, parsed: ParsedProductionRecord):
        if self._progress:
            self._progress.hide()
            self._progress = None
        self._on_parsed_sync(parsed)

    def _on_parsed_sync(self, parsed: ParsedProductionRecord):
        file_item = self._pending_file_item
        self._pending_file_item = None
        if file_item is None:
            return
        self._apply_parsed_to_file_item(file_item, parsed)

    def _repopulate_section_children(self, file_item: QTreeWidgetItem, parsed: ParsedProductionRecord):
        """Rebuild subsection rows under a file from cache."""
        data = file_item.data(0, Qt.UserRole)
        if not data or data[0] != _ROLE_FILE:
            return
        _, zpath, internal = data
        key = (zpath, internal)
        self._parse_cache[key] = parsed

        while file_item.childCount():
            file_item.removeChild(file_item.child(0))

        if not parsed.responsive or not parsed.sections:
            return

        pairs = visible_section_tree_rows(parsed)

        for orig_idx, label in pairs:
            sec_item = QTreeWidgetItem([label])
            sec_item.setData(0, Qt.UserRole, (_ROLE_SECTION, zpath, internal, orig_idx))
            file_item.addChild(sec_item)

        file_item.setExpanded(True)
        first_orig = pairs[0][0]
        file_item.setData(0, Qt.UserRole + 1, first_orig)
        first_child = file_item.child(0)
        self._tree.blockSignals(True)
        self._tree.setCurrentItem(first_child)
        self._tree.blockSignals(False)
        d = first_child.data(0, Qt.UserRole)
        if d and d[0] == _ROLE_SECTION and len(d) >= 4:
            self._on_section_item_selected(first_child, zpath, internal, int(d[3]))

    def _apply_parsed_to_file_item(self, file_item: QTreeWidgetItem, parsed: ParsedProductionRecord):
        data = file_item.data(0, Qt.UserRole)
        if not data or data[0] != _ROLE_FILE:
            return
        _, zpath, internal = data
        key = (zpath, internal)
        self._parse_cache[key] = parsed

        while file_item.childCount():
            file_item.removeChild(file_item.child(0))

        if not parsed.responsive:
            parts = [
                "No responsive tabular data in this production.",
                "",
                (parsed.no_data_note or parsed.banner_text or "")[:2500],
            ]
            if parsed.raw_error:
                parts.extend(["", f"Notice: {parsed.raw_error}"])
            self._show_detail("\n".join(p for p in parts if p is not None))
            self._last_parsed = parsed
            self._reset_table()
            return

        if not parsed.sections:
            self._show_detail(
                (parsed.raw_error + "\n\n" if parsed.raw_error else "")
                + "No tabular sections were parsed from this file."
            )
            self._last_parsed = parsed
            self._reset_table()
            return

        self._repopulate_section_children(file_item, parsed)

    def _apply_section_to_table(self, sec: ParsedSection):
        tips = header_tooltips_for_section(sec)
        self._table_model = DataFrameTableModel(
            sec.dataframe,
            tips,
            self._zip_path,
            self._ctx_zpath,
            self._ctx_internal,
            self._ctx_section_idx,
            self._main_window,
        )
        self._table.setModel(self._table_model)
        self._cell_delegate = ProductionRecordCellDelegate(self._table, self, self._main_window)
        self._table.setItemDelegate(self._cell_delegate)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self._table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self._wire_selection_tracking()
        self._show_table()
        if self._main_window is not None and hasattr(self._main_window, "_sync_additional_records_selection_keys"):
            self._main_window._sync_additional_records_selection_keys(self)

    def _export_current_section(self):
        if not self._last_parsed or not self._last_parsed.responsive:
            QMessageBox.information(self, "Export", "No tabular section to export.")
            return
        idx = self._current_section_index
        if idx < 0 or idx >= len(self._last_parsed.sections):
            QMessageBox.information(self, "Export", "Select a data section in the tree first.")
            return
        sec = self._last_parsed.sections[idx]
        if sec.dataframe.empty and not sec.columns:
            QMessageBox.information(self, "Export", "No rows to export.")
            return
        stem = os.path.splitext(os.path.basename(self._last_parsed.basename))[0]
        default = f"{stem}_section{idx + 1}.csv"
        path, _ = QFileDialog.getSaveFileName(self, "Export CSV", default, "CSV (*.csv)")
        if not path:
            return
        try:
            sec.dataframe.to_csv(path, index=False, encoding="utf-8")
        except Exception as e:
            QMessageBox.warning(self, "Export failed", str(e))
            return
        QMessageBox.information(self, "Export", f"Saved:\n{path}")
