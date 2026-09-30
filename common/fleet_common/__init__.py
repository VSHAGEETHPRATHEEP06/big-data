"""Shared helpers for every FleetPulse component.

The same package is installed in the simulator, Spark, Airflow and API images
so that configuration, logging format, the simulated clock and the business
rules are identical in the speed layer, the batch layer and the serving layer.
"""

__version__ = "1.0.0"
