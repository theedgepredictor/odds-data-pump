# src/utils.py
from __future__ import annotations
from typing import Sequence, Optional, List
import pandas as pd
import time, random

# ------------- polite jitter (you already had this) -------------
def polite_sleep_block(min_s: float = 0.5, max_s: float = 1.5) -> None:
    time.sleep(random.uniform(min_s, max_s))


# ------------- generic utilities for OPEN synthesis + dedupe -------------
def _align_schemas(a: pd.DataFrame, b: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return copies of a and b with identical columns (union)."""
    if a is None or a.empty:
        a = pd.DataFrame(columns=b.columns) if b is not None else pd.DataFrame()
    if b is None or b.empty:
        b = pd.DataFrame(columns=a.columns) if a is not None else pd.DataFrame()

    a, b = a.copy(), b.copy()
    for col in set(a.columns) - set(b.columns):
        b[col] = pd.NA
    for col in set(b.columns) - set(a.columns):
        a[col] = pd.NA
    # reorder b to a's columns for stable concat elsewhere
    b = b[a.columns]
    return a, b


def keep_only_latest_per_book(
    df: pd.DataFrame,
    uniq_keys_with_book: Sequence[str],
    *,
    last_updated_col: str = "last_updated",
) -> pd.DataFrame:
    """Keep only the latest row per composite key including book_id."""
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    df = df.copy()
    if last_updated_col not in df.columns:
        df[last_updated_col] = pd.Timestamp("1970-01-01")
    return (
        df.sort_values(last_updated_col)
          .drop_duplicates(list(uniq_keys_with_book), keep="last")
          .reset_index(drop=True)
    )


def ensure_open_lines(
    df: pd.DataFrame,
    uniq_keys_no_book: Sequence[str],
    *,
    open_book_id: int = 30,
    fallback_priority: Optional[Sequence[int]] = None,
    last_updated_col: str = "last_updated",
    open_inferred_col: str = "open_inferred",
    open_source_book_id_col: str = "open_source_book_id",
) -> pd.DataFrame:
    """
    If a group (ignoring book) lacks an OPEN (book_id=open_book_id), duplicate from the
    first available fallback in `fallback_priority`, tagging open_inferred/source.
    """
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    if not fallback_priority:
        # no synthesis requested; just return as-is
        return df

    df = df.copy()

    # normalize book_id to Int64 for reliable comparisons
    if "book_id" in df.columns:
        df["book_id"] = pd.to_numeric(df["book_id"], errors="coerce").astype("Int64")

    # fast lookup by (group + book_id)
    idx_cols = list(uniq_keys_no_book) + ["book_id"]
    df_idx = df.set_index(idx_cols, drop=False)

    present_books_by_group = (
        df.groupby(list(uniq_keys_no_book), dropna=False)["book_id"]
          .apply(lambda s: set(pd.to_numeric(s, errors="coerce").dropna().astype(int)))
    )
    groups_missing_open = present_books_by_group[~present_books_by_group.apply(lambda s: open_book_id in s)]

    rows_to_add: List[dict] = []
    for group_key, _ in groups_missing_open.items():
        # group_key is a scalar or tuple depending on len(keys)
        if not isinstance(group_key, tuple):
            group_key = (group_key,)

        chosen = None
        for bid in fallback_priority:
            try:
                candidate = df_idx.loc[group_key + (bid,)]
            except KeyError:
                continue
            # if multiple rows exist for this (group+book), keep latest by last_updated
            if isinstance(candidate, pd.DataFrame):
                col = last_updated_col if last_updated_col in candidate.columns else None
                candidate = candidate.sort_values(col or candidate.columns[0]).iloc[-1]
            chosen = candidate
            break

        if chosen is not None:
            r = chosen.to_dict()
            r["book_id"] = open_book_id
            # ensure flags exist
            r[open_inferred_col] = True
            r[open_source_book_id_col] = int(chosen["book_id"]) if "book_id" in chosen else pd.NA
            rows_to_add.append(r)

    if rows_to_add:
        add_df = pd.DataFrame(rows_to_add)
        # align + concat
        for col in df.columns:
            if col not in add_df.columns:
                add_df[col] = pd.NA
        df = pd.concat([df, add_df[df.columns]], ignore_index=True)

    # guarantee flags on all rows
    if open_inferred_col not in df.columns:
        df[open_inferred_col] = False
    if open_source_book_id_col not in df.columns:
        df[open_source_book_id_col] = pd.NA
    df[open_inferred_col] = df[open_inferred_col].fillna(False)

    return df


def merge_with_existing_and_dedupe(
    current_df: pd.DataFrame,
    new_df: pd.DataFrame,
    *,
    uniq_keys_with_book: Sequence[str],
    open_book_id: int = 30,
    fallback_priority: Optional[Sequence[int]] = None,   # e.g., [15, 68, 69, 79]
    last_updated_col: str = "last_updated",
    open_inferred_col: str = "open_inferred",
    open_source_book_id_col: str = "open_source_book_id",
    protect_open: bool = True,          # don't overwrite existing OPEN (30)
    prefer_real_open: bool = False,     # if a "real" 30 shows up later, replace synthetic
) -> pd.DataFrame:
    """
    Combine existing weekly parquet + new pull.
      1) Merge first (NO open synthesis yet).
      2) For book_id != open_book_id: keep latest by last_updated.
         For book_id == open_book_id:
             - If protect_open: prefer existing rows over new rows.
             - If prefer_real_open: prefer non-inferred over inferred.
      3) After dedupe, synthesize any missing OPEN exactly once across the combined set.
      4) Final dedupe for non-OPEN rows by last_updated.
    """
    uniq_keys_with_book = list(uniq_keys_with_book)
    uniq_keys_no_book = [k for k in uniq_keys_with_book if k != "book_id"]

    # handle empty cases
    if new_df is None or new_df.empty:
        return current_df if current_df is not None else pd.DataFrame()
    if current_df is None or current_df.empty:
        combined = new_df.copy()
        # ensure fields used later exist
        for col in [last_updated_col, open_inferred_col, open_source_book_id_col]:
            if col not in combined.columns:
                combined[col] = pd.NA
        # synthesize OPEN now (no prior to protect)
        combined = ensure_open_lines(
            combined, uniq_keys_no_book,
            open_book_id=open_book_id,
            fallback_priority=fallback_priority,
            last_updated_col=last_updated_col,
            open_inferred_col=open_inferred_col,
            open_source_book_id_col=open_source_book_id_col,
        )
        # dedupe everything normally
        return keep_only_latest_per_book(combined, uniq_keys_with_book, last_updated_col=last_updated_col)

    # 1) schema align + concat, tag sources
    a, b = _align_schemas(current_df, new_df)
    a, b = a.copy(), b.copy()
    for col in [last_updated_col, open_inferred_col, open_source_book_id_col]:
        if col not in a.columns: a[col] = pd.NA
        if col not in b.columns: b[col] = pd.NA

    a["__src"] = "existing"
    b["__src"] = "new"
    combined_raw = pd.concat([a, b], ignore_index=True)

    # normalize book_id to Int64 for selection
    if "book_id" in combined_raw.columns:
        combined_raw["book_id"] = pd.to_numeric(combined_raw["book_id"], errors="coerce").astype("Int64")

    # 2) split OPEN vs non-OPEN
    is_open = (combined_raw["book_id"] == open_book_id) if "book_id" in combined_raw.columns else pd.Series(False, index=combined_raw.index)
    open_df = combined_raw[is_open].copy()
    non_open_df = combined_raw[~is_open].copy()

    # 2a) dedupe non-OPEN normally
    if not non_open_df.empty:
        if last_updated_col not in non_open_df.columns:
            non_open_df[last_updated_col] = pd.Timestamp("1970-01-01")
        non_open_df = (
            non_open_df.sort_values(last_updated_col)
                       .drop_duplicates(uniq_keys_with_book, keep="last")
                       .reset_index(drop=True)
        )

    # 2b) dedupe OPEN with protection logic
    if not open_df.empty:
        # lower ranks win
        if protect_open:
            src_rank = open_df["__src"].map({"existing": 0, "new": 1}).fillna(1)
        else:
            src_rank = 0  # ignore source

        # prefer "real" (non-inferred) over inferred if requested
        inferred = open_df[open_inferred_col].fillna(False) if open_inferred_col in open_df.columns else pd.Series(False, index=open_df.index)
        infer_rank = inferred.astype(int) if prefer_real_open else 0

        # ensure last_updated exists
        if last_updated_col not in open_df.columns:
            open_df[last_updated_col] = pd.Timestamp("1970-01-01")

        open_df = (
            open_df.assign(__rank_src=src_rank, __rank_infer=infer_rank)
                   .sort_values(["__rank_src", "__rank_infer", last_updated_col])
                   .drop_duplicates(uniq_keys_with_book, keep="first")
                   .reset_index(drop=True)
        )

    # 3) combine, then synthesize any missing OPEN across the combined set
    combined = pd.concat([non_open_df, open_df], ignore_index=True)
    combined = ensure_open_lines(
        combined, uniq_keys_no_book,
        open_book_id=open_book_id,
        fallback_priority=fallback_priority,
        last_updated_col=last_updated_col,
        open_inferred_col=open_inferred_col,
        open_source_book_id_col=open_source_book_id_col,
    )

    # 4) final: dedupe non-OPEN normally (OPEN rows are already resolved; ensure we don't disturb them)
    if "book_id" in combined.columns:
        is_open = (combined["book_id"] == open_book_id)
        open_final = combined[is_open].copy()
        non_open_final = keep_only_latest_per_book(
            combined[~is_open].copy(),
            uniq_keys_with_book,
            last_updated_col=last_updated_col,
        )
        out = pd.concat([non_open_final, open_final], ignore_index=True)
    else:
        out = keep_only_latest_per_book(
            combined,
            uniq_keys_with_book,
            last_updated_col=last_updated_col,
        )

    # cleanup helper cols
    out = out.drop(columns=["__src", "__rank_src", "__rank_infer"], errors="ignore")
    return out
