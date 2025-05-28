import logging
import uuid
from datetime import datetime

from flowfunc.utils import helpers

logger = logging.getLogger(__name__)


def generate_unique_id(run_name: str | None = None) -> str:
    """
    Generates a unique ID for the run, adding a prefix is provided.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]
    prefix = helpers.sanitize_string(run_name) if run_name else "run"
    run_id = f"{prefix}_{timestamp}_{unique_suffix}"
    logger.debug(f"Generated run ID: {run_id}")
    return run_id
