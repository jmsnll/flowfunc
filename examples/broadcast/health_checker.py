import random
from typing import Any


def check_service_health(environment: str, region: str) -> dict[str, Any]:
    """
    Simulates a health check for the 'auth-service' in a specific
    environment and region.
    """
    service = "auth-service"
    print(f"Checking {service} in {environment.upper()} ({region})...")

    # Simulate different results based on the target
    is_healthy = True
    latency = random.randint(20, 100)

    if environment == "playground":
        latency += 50  # Playground is less optimized
    if environment == "production" and random.random() < 0.05:
        is_healthy = False  # Simulate a rare production failure
        latency = 500

    return {
        "environment": environment,
        "region": region,
        "service": service,
        "status": "HEALTHY" if is_healthy else "UNHEALTHY",
        "latency_ms": latency,
    }
