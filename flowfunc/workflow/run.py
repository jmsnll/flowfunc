import uuid
from datetime import datetime


def generate_id(user_provided_name: str | None) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]
    if user_provided_name:
        safe_name = "".join(
            c if c.isalnum() or c in ["-", "_"] else "_" for c in user_provided_name
        ).strip("_")
        return (
            f"{ts}_{safe_name}_{unique_suffix}"
            if safe_name
            else f"run_{ts}_{unique_suffix}"
        )
    return f"run_{ts}_{unique_suffix}"
