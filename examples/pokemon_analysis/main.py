from typing import Dict, Any, List

import numpy as np
from pipefunc import pipefunc, Pipeline

import requests
import json


@pipefunc(output_name="pokemon_data", mapspec="pokemon_id[n] -> pokemon_data[n]")
def fetch_pokemon_data(pokemon_id: int) -> Dict[str, Any]:
    """
    Fetches data for a single Pokémon from the PokéAPI.
    """
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    print(f"Fetching data for Pokémon ID: {pokemon_id}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching Pokémon {pokemon_id}: {e}")
        return {}  # Return an empty dict on error

@pipefunc(output_name="all_stats", mapspec="pokemon_data[n] -> all_stats[n]")
def extract_basic_stats(pokemon_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts a few key pieces of information from the raw Pokémon data.
    """
    if not pokemon_data:
        return {}  # Skip if data is empty from a failed fetch

    name = pokemon_data.get("name", "Unknown")
    print(f"Extracting stats for: {name.capitalize()}")

    # Get the first type from the 'types' list
    primary_type = "none"
    if pokemon_data.get("types"):
        primary_type = pokemon_data["types"][0]["type"]["name"]

    return {
        "id": pokemon_data.get("id"),
        "name": name,
        "primary_type": primary_type,
        "height": pokemon_data.get("height"),  # In decimetres
        "weight": pokemon_data.get("weight"),  # In hectograms
    }

@pipefunc(output_name="final_summary")
def summarize_pokemon_stats(all_stats: np.ndarray) -> Dict[str, Any]:
    """
    Aggregates all the extracted stats into a final summary.
    """
    print("Aggregating all Pokémon stats...")

    # Filter out any empty results from failed API calls
    valid_stats = [s for s in all_stats if s]

    type_counts = {}
    for stats in valid_stats:
        p_type = stats["primary_type"]
        type_counts[p_type] = type_counts.get(p_type, 0) + 1

    total_pokemon_fetched = len(valid_stats)
    average_weight = sum(s['weight'] for s in valid_stats) / total_pokemon_fetched if total_pokemon_fetched > 0 else 0

    return {
        "total_pokemon_fetched": total_pokemon_fetched,
        "average_weight_hg": round(average_weight, 2),
        "pokemon_count_by_type": type_counts
    }


if __name__ == "__main__":
    pipeline = Pipeline([summarize_pokemon_stats, extract_basic_stats, fetch_pokemon_data], profile=True)
    results = pipeline.map({"pokemon_id": list(range(1,1000))})

    print(results)
