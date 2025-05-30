def normalize_name(name):
    """Normalize string by stripping and converting to lowercase."""
    return name.strip().lower()

def merge_similar_items(items):
    """Merge similar strings into a deduplicated list."""
    merged = []
    for item in items:
        item = normalize_name(item)
        if not any(item in m or m in item for m in merged):
            merged.append(item)
    return merged
