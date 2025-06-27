from typing import Any


def create_character_profile(
    name: str, archetype: str, power_level: int, design_year: int
) -> dict[str, Any]:
    """
    Combines data from four sources into a single, structured
    character profile dictionary.
    """
    profile = {
        "character_name": name,
        "class": archetype,
        "power_level": power_level,
        "year_created": design_year,
    }
    return profile
