"""Shared xlsx-reading utilities (stdlib-only, no extra dependencies)."""
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

_NS = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'


def _col_idx(ref: str) -> int:
    """Convert Excel cell ref (e.g. 'B3', 'AA10') to 0-based column index."""
    letters = re.sub(r'\d', '', ref)
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch.upper()) - 64)
    return n - 1


def read_sheet(xlsx_path: str, sheet_number: int) -> list:
    """
    Read all rows from a sheet (1-based sheet_number) as a list of rows.
    Each row is a list indexed by 0-based column position; missing cells are ''.
    Handles sparse rows correctly via cell r-attribute column positioning.
    """
    with zipfile.ZipFile(xlsx_path) as z:
        strings = []
        try:
            sst = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in sst.findall(f'.//{_NS}si'):
                strings.append(''.join(t.text or '' for t in si.findall(f'.//{_NS}t')))
        except KeyError:
            pass

        def cv(c):
            t = c.get('t')
            v = c.find(f'{_NS}v')
            if v is None:
                return ''
            return strings[int(v.text)] if t == 's' else (v.text or '')

        xml = z.read(f"xl/worksheets/sheet{sheet_number}.xml")
        result = []
        for xml_row in ET.fromstring(xml).findall(f'.//{_NS}row'):
            cells = xml_row.findall(f'{_NS}c')
            if not cells:
                result.append([])
                continue
            max_col = max(_col_idx(c.get('r', 'A1')) for c in cells)
            row = [''] * (max_col + 1)
            for c in cells:
                row[_col_idx(c.get('r', 'A1'))] = cv(c)
            result.append(row)
        return result


def to_float(s: str) -> float:
    try:
        return float(s) if s else 0.0
    except (ValueError, TypeError):
        return 0.0


def slugify(name: str) -> str:
    s = name.lower()
    s = re.sub(r'[()]', '', s)
    s = re.sub(r'[^a-z0-9]+', '-', s)
    return s.strip('-')
