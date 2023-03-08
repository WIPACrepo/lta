# lta_const.py
"""Central catalog of LTA constants and constant functions."""


def drain_semaphore_filename(component: str) -> str:
    """Obtain the canonical drain semaphore filename for the specified component name."""
    return f".lta-{component}-drain"


def pid_filename(component: str) -> str:
    """Obtain the canonical pid filename for the specified component name."""
    return f".lta-{component}-pid"
