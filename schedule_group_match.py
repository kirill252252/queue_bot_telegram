import re


_GROUP_PREFIX_RE = re.compile(r"^(?:группа|гр)\.?\s*", re.IGNORECASE)
_GROUP_SEPARATORS_RE = re.compile(r"[^0-9a-zа-я]+", re.IGNORECASE)


def normalize_group_name(value: str) -> str:
    cleaned = (value or "").strip().lower().replace("ё", "е")
    cleaned = _GROUP_PREFIX_RE.sub("", cleaned)
    return _GROUP_SEPARATORS_RE.sub("", cleaned)


def build_group_lookup(groups: list[dict]) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for group in groups:
        key = normalize_group_name(group.get("group_name", ""))
        if key and key not in lookup:
            lookup[key] = group
    return lookup


def resolve_target_groups(
    change: dict,
    groups: list[dict],
    group_lookup: dict[str, dict],
) -> list[dict]:
    raw_group = (change.get("group") or "").strip()
    if not raw_group:
        return list(groups)

    match = group_lookup.get(normalize_group_name(raw_group))
    return [match] if match else []
