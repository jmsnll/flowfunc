import string
import uuid
from datetime import datetime
from typing import Any


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


def is_jinja_template(value: any) -> bool:
    """Checks if a given value is a string that appears to be a Jinja2 template."""
    return isinstance(value, str) and "{{" in value and "}}" in value


def is_direct_jinja_reference(value: Any) -> bool:
    """
    Determines whether a string is a *direct* Jinja2 reference.

    A direct reference:
      - Is a string.
      - Contains exactly one `{{ ... }}` block.
      - Contains no other characters (except surrounding whitespace).
    """
    if not isinstance(value, str):
        return False

    stripped = value.strip()

    has_exactly_one_open = stripped.count("{{") == 1
    has_exactly_one_close = stripped.count("}}") == 1
    starts_with_open = stripped.startswith("{{")
    ends_with_close = stripped.endswith("}}")

    return all(
        [has_exactly_one_open, has_exactly_one_close, starts_with_open, ends_with_close]
    )
