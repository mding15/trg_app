"""
Extract portfolio positions from H&B statement PDF.
Handles rotated (90-degree CCW) landscape pages using pdfplumber.
"""

import os
import re
import csv
import pdfplumber

PDF_PATH = os.path.join(os.path.dirname(__file__), "PDF", "H&B Statement May 2026.pdf")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "CSV")

# Column "top" coordinate ranges (rotated PDF coordinate system).
# In original landscape layout: LEFT side = high "top", RIGHT side = low "top".
YIELD_COST   = (10,  50)
YIELD_MKT    = (57,  90)
INCOME       = (105, 135)
PCT_ASSETS   = (165, 200)
MKT_VALUE    = (210, 250)
MKT_PRICE    = (280, 315)
TOTAL_COST   = (338, 370)
UNIT_COST    = (408, 435)
DUE_DATE     = (560, 600)   # bond maturity date string (contains '/')
COUPON       = (618, 645)   # bond coupon rate (starts with '%' before reversal)
SEC_NAME     = (490, 675)   # security name words (multi-word, sorted desc)
TICKER       = (675, 715)   # stock ticker symbol
QUANTITY     = (710, 740)   # share count or par value (numeric)

HEADER_X_MAX = 110   # x bands ≤ this are page-header rows → skip
FOOTER_X_MIN = 565   # x bands ≥ this are page-footer rows → skip

# Words to exclude from sector-name candidates
SECTOR_SKIP = {
    "TREASURY", "NOTES", "HEMENWAY", "BARNES", "TRUST", "COMPANY",
    "MONEY", "MARKET", "FUND", "CASH", "PRINCIPAL", "INCOME",
    "BLEND", "BOND", "BONDS", "STOCK", "COMMON",
    "USD", "UDS", "TOTAL", "PORTFOLIO",
}

ACCOUNTS = [
    {
        "name": "Patricia Finley 1999 Trust",
        "pages": [4, 5, 6, 7],    # 0-indexed (PDF pages 5-8)
        "output": "Patricia_Finley_1999_Trust.csv",
    },
    {
        "name": "Eric J Spindler Irrevocable Trust",
        "pages": [11, 12, 13, 14],  # 0-indexed (PDF pages 12-15)
        "output": "Eric_J_Spindler_Irrevocable_Trust.csv",
    },
]

CSV_FIELDS = [
    "Account", "Sector", "Ticker", "Security",
    "Quantity", "Unit_Cost", "Market_Price", "Total_Cost", "Market_Value",
    "Pct_Assets", "Annual_Income", "Yield_Market_Pct", "Yield_Cost_Pct",
    "Due_Date", "Coupon_Rate_Pct",
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def in_range(val, rng):
    return rng[0] <= val <= rng[1]


def words_in(band_words, rng):
    return [w for w in band_words if in_range(w["top"], rng)]


def get_text(band_words, rng, descending=False):
    """Return joined reversed text of words whose top falls in rng."""
    matches = words_in(band_words, rng)
    if not matches:
        return None
    matches.sort(key=lambda w: w["top"], reverse=descending)
    return " ".join(w["text"][::-1] for w in matches)


def to_float(s):
    """Convert a (possibly comma-formatted) string to float, or None."""
    if s is None:
        return None
    s = s.strip().lstrip("%").rstrip("%").replace(",", "").replace("-", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def has_numeric_qty(bw):
    """True if any word in the QUANTITY top range parses as a number."""
    for w in words_in(bw, QUANTITY):
        if to_float(w["text"][::-1]) is not None:
            return True
    return False


def classify(bw):
    """
    primary  – has a numeric value in the QUANTITY top range
    subtotal – has market-value data but no numeric quantity
    due_ext  – bond continuation band carrying due-date / coupon only
    label    – sector header, security-name-only, or other non-data band
    """
    if has_numeric_qty(bw):
        return "primary"
    if words_in(bw, MKT_VALUE):
        return "subtotal"
    # "Due" label in the bond detail line is stored as reversed "euD"
    if any(w["text"] == "euD" for w in bw):
        return "due_ext"
    return "label"


def is_security_word(text):
    """True if text looks like a word from a security name (not a date/coupon)."""
    if "/" in text:          # date component e.g. 7/15/2026
        return False
    if text.startswith("%") or text.endswith("%"):   # coupon e.g. 4.500%
        return False
    if text.lower() == "due":
        return False
    return True


def extract_name(bw):
    """
    Security name words sit at top [490-675], read right-to-left in the
    original landscape page (i.e. descending top = left-to-right order).
    Exclude date/coupon tokens that may overlap the same top range.
    """
    matches = []
    for w in words_in(bw, SEC_NAME):
        text = w["text"][::-1]
        if is_security_word(text):
            matches.append((w["top"], text))
    if not matches:
        return None
    matches.sort(key=lambda x: x[0], reverse=True)
    return " ".join(t for _, t in matches)


def extract_due_coupon(bw):
    """
    Return (due_date_str, coupon_float).
    Due date must contain '/' to distinguish it from security-name words
    that may share the same top range.
    """
    due = None
    for w in words_in(bw, DUE_DATE):
        text = w["text"][::-1]
        if "/" in text:
            due = text
            break

    raw_coupon = get_text(bw, COUPON)
    coupon = to_float(raw_coupon) if raw_coupon else None
    return due, coupon


def looks_like_sector(text):
    """
    Accept only strings that look like real sector names:
    all-uppercase letters, spaces, and '&'; at least 4 chars.
    Rejects strings with digits, hyphens, lowercase, or special chars.
    """
    return bool(re.match(r"^[A-Z& ]{4,}$", text))


# ---------------------------------------------------------------------------
# main extraction
# ---------------------------------------------------------------------------

def process_pages(pdf, page_indices):
    rows = []
    current_sector = None

    for pg_idx in page_indices:
        page = pdf.pages[pg_idx]
        words = [w for w in page.extract_words() if not w["upright"]]

        # Group words into x-bands (8-point grid)
        raw_bands = {}
        for w in words:
            band = round(w["x0"] / 8) * 8
            raw_bands.setdefault(band, []).append(w)

        band_list = [(x, raw_bands[x]) for x in sorted(raw_bands)]

        i = 0
        while i < len(band_list):
            x, bw = band_list[i]

            # Skip page header / footer bands
            if x <= HEADER_X_MAX or x >= FOOTER_X_MIN:
                i += 1
                continue

            kind = classify(bw)

            # ---- label band: update current sector if it qualifies ----
            if kind == "label":
                # Sector headers have at least one word at top ≥ 700 (original
                # "quantity column" area). Security/bond name labels live at
                # top 540-680 and don't qualify as sector updates.
                has_high_word = any(w["top"] >= 700 for w in bw)
                if has_high_word:
                    label_words = [w for w in bw if w["top"] > 600]
                    if label_words:
                        label_words.sort(key=lambda w: w["top"], reverse=True)
                        candidate = " ".join(w["text"][::-1] for w in label_words)
                        if (
                            looks_like_sector(candidate)
                            and not any(kw in candidate.upper() for kw in SECTOR_SKIP)
                        ):
                            current_sector = candidate
                i += 1
                continue

            if kind in ("subtotal", "due_ext"):
                i += 1
                continue

            # ---- primary security band ----

            # 1. Security name: current band first, then previous band
            sec_name = extract_name(bw)
            if not sec_name and i > 0:
                prev_x, prev_bw = band_list[i - 1]
                if x - prev_x <= 16 and classify(prev_bw) == "label":
                    sec_name = extract_name(prev_bw)

            # 2. Ticker
            ticker = get_text(bw, TICKER)

            # 3. Quantity (numeric, e.g. shares or par value)
            qty = to_float(get_text(bw, QUANTITY))

            # 4. Financial columns
            unit_cost  = to_float(get_text(bw, UNIT_COST))
            mkt_price  = to_float(get_text(bw, MKT_PRICE))
            total_cost = to_float(get_text(bw, TOTAL_COST))
            mkt_value  = to_float(get_text(bw, MKT_VALUE))
            pct_assets = to_float(get_text(bw, PCT_ASSETS))
            income     = to_float(get_text(bw, INCOME))
            yield_mkt  = to_float(get_text(bw, YIELD_MKT))
            yield_cost = to_float(get_text(bw, YIELD_COST))

            # 5. Bond due date / coupon: try this band, then adjacent next band
            due_date, coupon = extract_due_coupon(bw)
            if due_date is None and i + 1 < len(band_list):
                next_x, next_bw = band_list[i + 1]
                if next_x - x <= 16 and classify(next_bw) == "due_ext":
                    due_date, coupon = extract_due_coupon(next_bw)

            # Skip rows that have no useful data (section totals, stray labels)
            if qty is None and mkt_value is None:
                i += 1
                continue

            rows.append({
                "Sector":           current_sector,
                "Ticker":           ticker,
                "Security":         sec_name,
                "Quantity":         qty,
                "Unit_Cost":        unit_cost,
                "Market_Price":     mkt_price,
                "Total_Cost":       total_cost,
                "Market_Value":     mkt_value,
                "Pct_Assets":       pct_assets,
                "Annual_Income":    income,
                "Yield_Market_Pct": yield_mkt,
                "Yield_Cost_Pct":   yield_cost,
                "Due_Date":         due_date,
                "Coupon_Rate_Pct":  coupon,
            })
            i += 1

    return rows


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with pdfplumber.open(PDF_PATH) as pdf:
        for account in ACCOUNTS:
            rows = process_pages(pdf, account["pages"])

            out_path = os.path.join(OUTPUT_DIR, account["output"])
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writeheader()
                for row in rows:
                    row["Account"] = account["name"]
                    writer.writerow({k: row.get(k, "") or "" for k in CSV_FIELDS})

            print(f"Wrote {len(rows)} positions → {out_path}")


if __name__ == "__main__":
    main()
