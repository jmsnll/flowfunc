import string
import uuid
from datetime import datetime


def sanitize_string(data: str) -> str:
    """Sanitizes a string to be safe for file names or identifiers."""
    allowed_chars = string.ascii_letters + string.digits + "-_"

    disallowed_map = str.maketrans("", "", allowed_chars)
    sanitized = data.translate(disallowed_map).replace(string.punctuation, "_")

    remove_duplicate_underscores = re.sub(r"_+", "_", sanitized)
    return remove_duplicate_underscores.strip("_")


def generate_unique_id(run_name: str | None = None) -> str:
    """Generates a unique ID for the run, adding a prefix is provided."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]
    prefix = sanitize_string(run_name) if run_name else "run"
    return f"{prefix}_{timestamp}_{unique_suffix}"
