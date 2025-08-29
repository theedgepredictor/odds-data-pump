import random
import time

import pandas as pd
import unicodedata


def polite_sleep_block(min_s=0.5, max_s=4.5):
    time.sleep(random.uniform(min_s, max_s))


def _to_ascii(x: str) -> str:
    return unicodedata.normalize("NFKD", x).encode("ascii", "ignore").decode("ascii")

def clean_player_names(
    s: pd.Series,
    *,
    lowercase: bool = False,
    convert_lastfirst: bool = True,
    convert_to_ascii: bool = True,
) -> pd.Series:
    """
    Clean player names (pandas version of the R function).

    Steps:
      1) collapse internal whitespace, trim ends
      2) if convert_lastfirst: 'Last, First' -> 'First Last'
      3) remove trailing suffixes (Jr., Sr., II, III, IV, V)
      4) remove apostrophes/periods/commas
      5) if convert_to_ascii: transliterate to latin-ascii
      6) if use_name_database: apply exact substitutions from name_database
      7) if lowercase: to lowercase
    """
    # ensure string dtype but preserve NA
    s = s.astype("string")

    # 1) whitespace
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()

    # 2) "Last, First" -> "First Last"
    if convert_lastfirst:
        s = s.str.replace(r"^(.+?),\s*(.+)$", r"\2 \1", regex=True)

    # 3) remove trailing suffixes
    s = s.str.replace(r"\s+(Jr|Sr)\.?$|\s+(II|III|IV|V)$", "", case=False, regex=True)

    # 4) remove apostrophes/periods/commas (incl. curly apostrophe)
    s = s.str.replace(r"[\'\u2019\.,]", "", regex=True)

    # 5) transliterate to ASCII
    if convert_to_ascii:
        s = s.apply(lambda x: _to_ascii(x) if pd.notna(x) else x)

    # 6) apply exact substitutions from name_database

    # 7) lowercase
    if lowercase:
        s = s.str.lower()

    return s