# flowfunc/logging_config.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from structlog.typing import EventDict
from structlog.typing import Processor
from structlog.typing import WrappedLogger

if TYPE_CHECKING:

    from cleo.io.io import IO


# --- Structlog Processors (SHARED_PROCESSORS should be defined as before) ---
def flowfunc_event_renamer(
    _: WrappedLogger, __: str, event_dict: EventDict
) -> EventDict:
    """Ensures 'message' key exists, typically by renaming 'event'."""
    if "message" not in event_dict and "event" in event_dict:
        event_dict["message"] = event_dict.pop("event")
    elif (
        "event" not in event_dict and "message" not in event_dict
    ):  # Ensure a default if neither
        event_dict["message"] = "Log event"  # Or some other default placeholder
    return event_dict


SHARED_PROCESSORS: list[Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,  # Adds 'logger_name'
    structlog.stdlib.add_log_level,  # Adds 'log_level'
    structlog.processors.StackInfoRenderer(),
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.CallsiteParameterAdder(
        [
            structlog.processors.CallsiteParameter.PATHNAME,
            structlog.processors.CallsiteParameter.FILENAME,
            structlog.processors.CallsiteParameter.MODULE,
            structlog.processors.CallsiteParameter.FUNC_NAME,
            structlog.processors.CallsiteParameter.LINENO,
        ]
    ),
    structlog.processors.format_exc_info,
    structlog.processors.UnicodeDecoder(),
    # add_flowfunc_context, # Your custom processor if any
    flowfunc_event_renamer,  # Ensures 'message' key exists for stdlib formatters
]


def configure_structlog(
    log_level: int | str = logging.INFO,
    io_for_console: IO | None = None,  # Cleo IO for console handler
) -> None:
    """
    Configures structlog and the standard Python logging system.
    This MUST be called once at application startup.
    """
    # Main structlog pipeline configuration
    structlog.configure(
        processors=SHARED_PROCESSORS
        + [  # Processors applied to structlog events
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,  # Prepares LogRecord for stdlib
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,  # This is where default processors and context_class are used
        cache_logger_on_first_use=True,
    )

    # Standard library root logger setup
    stdlib_root_logger = logging.getLogger()
    for handler in list(stdlib_root_logger.handlers):  # Clear existing
        stdlib_root_logger.removeHandler(handler)

    if io_for_console:
        from flowfunc.console.logging.io_formatter import (
            IOFormatter,  # Your existing one
        )
        from flowfunc.console.logging.io_handler import IOHandler

        console_handler = IOHandler(io_for_console)
        console_formatter = IOFormatter()  # Your formatter for human-readable console
        console_handler.setFormatter(console_formatter)
        stdlib_root_logger.addHandler(console_handler)

    effective_log_level = (
        log_level
        if isinstance(log_level, int)
        else logging.getLevelName(log_level.upper())
    )
    stdlib_root_logger.setLevel(effective_log_level)
    structlog.get_logger("flowfunc.logging").debug(
        "Structlog globally configured.",
        root_level=logging.getLevelName(effective_log_level),
    )


_JSON_FILE_HANDLER_REFERENCE: logging.FileHandler | None = None


def add_json_file_handler(
    log_file_path: str | Path, log_level: int | str = logging.DEBUG
) -> logging.FileHandler | None:
    """
    Adds a FileHandler that logs in JSON format to the Python root logger.
    """
    global _JSON_FILE_HANDLER_REFERENCE
    log = structlog.get_logger(__name__)  # Use structlog logger

    try:
        log_file_p = Path(log_file_path).resolve()
        log_file_p.parent.mkdir(parents=True, exist_ok=True)

        remove_json_file_handler()  # Remove previous one if any

        file_handler = logging.FileHandler(str(log_file_p), encoding="utf-8")

        # Create a structlog.stdlib.ProcessorFormatter instance for the file handler.
        # This formatter will handle both logs originating from structlog and
        # logs originating directly from the standard library (foreign logs).
        json_formatter = structlog.stdlib.ProcessorFormatter(
            # This processor is applied to LogRecord.event_dict if the log came from structlog.
            # It renders the final JSON string.
            processor=structlog.processors.JSONRenderer(sort_keys=True),
            # This chain is applied to LogRecords that did NOT originate from structlog
            # (e.g., logs from pipefunc if it uses standard logging directly).
            # It processes them through SHARED_PROCESSORS to add structlog's context
            # (timestamp, level, logger_name etc.) before they are rendered by JSONRenderer.
            foreign_pre_chain=list(SHARED_PROCESSORS),  # Must be a list
        )

        file_handler.setFormatter(json_formatter)
        file_handler.setLevel(
            log_level
            if isinstance(log_level, int)
            else logging.getLevelName(log_level.upper())
        )

        logging.getLogger().addHandler(file_handler)  # Add to root logger
        _JSON_FILE_HANDLER_REFERENCE = file_handler

        log.info(
            "JSON file logging enabled.",
            path=str(log_file_p),
            handler_level=logging.getLevelName(file_handler.level),
        )
        return file_handler
    except Exception as e:
        log.error(
            "Failed to initialize JSON file logging.",
            path=str(log_file_path),
            error=str(e),
            exc_info=True,
        )
        return None


def remove_json_file_handler() -> None:
    """Removes the run-specific JSON file handler if it exists."""
    global _JSON_FILE_HANDLER_REFERENCE
    log = structlog.get_logger(__name__)  # Use structlog logger
    if _JSON_FILE_HANDLER_REFERENCE:
        log.debug(
            "Removing JSON file handler.",
            handler_name=str(_JSON_FILE_HANDLER_REFERENCE),
        )
        # It's important to close the handler before removing it
        _JSON_FILE_HANDLER_REFERENCE.close()
        logging.getLogger().removeHandler(_JSON_FILE_HANDLER_REFERENCE)
        _JSON_FILE_HANDLER_REFERENCE = None
