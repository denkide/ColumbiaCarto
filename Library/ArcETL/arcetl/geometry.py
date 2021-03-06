"""Geometry-related objects."""
import logging
from math import pi, sqrt

from more_itertools import pairwise


LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""

RATIO = {
    "meter": {
        "foot": 0.3048,
        "feet": 0.3048,
        "ft": 0.3048,
        "yard": 0.9144,
        "yards": 0.9144,
        "yd": 0.9144,
        "mile": 1609.34,
        "miles": 1609.34,
        "mi": 1609.34,
        "meter": 1.0,
        "meters": 1.0,
        "m": 1.0,
        "kilometer": 1000.0,
        "kilometers": 1000.0,
        "km": 1000.0,
    }
}
"""dict: Two-level mapping of ratio between two types of measure.

Usage: `RATIO[to_measure][from_measure]`
"""


def compactness_ratio(geometry=None, **kwargs):
    """Return compactness ratio (4pi * area / perimeter ** 2) result.

    If geometry is None or one of the area & perimeter keyword arguments are undefined/
    None, will return None.

    Args:
        geometry (arcpy.Geometry): Geometry to evaluate.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        area (float): Area of geometry to evaluate. Only used if `geometry=None`.
        perimeter (float): Perimeter of geometry to evaluate. Only used if
            `geometry=None`.

    Returns:
        float: Ratio of compactness.
    """
    if geometry:
        dim = {"area": geometry.area, "perimeter": geometry.length}
    elif kwargs.get("area") and kwargs.get("perimeter"):
        dim = {"area": kwargs["area"], "perimeter": kwargs["perimeter"]}
    else:
        dim = {}
    return (4 * pi * dim["area"]) / (dim["perimeter"] ** 2) if dim else None


def coordinate_distance(*coordinates):
    """Return total distance between coordinates.

    Args:
        *coordinates: Collection of coordinates to compare. Coordinates can be `x,y` or
            `x,y,z`.

    Returns:
        float: Euclidian distance between coordinates.
    """
    distance = 0.0
    for coord1, coord2 in pairwise(coordinates):
        coord = {
            1: dict(zip(["x", "y", "z"], coord1)),
            2: dict(zip(["x", "y", "z"], coord2)),
        }
        coord[1].setdefault("z", 0)
        coord[2].setdefault("z", 0)
        distance += sqrt(
            sum(
                (coord[2]["x"] - coord[1]["x"]) ** 2,
                (coord[2]["y"] - coord[1]["y"]) ** 2,
                (coord[2]["z"] - coord[1]["z"]) ** 2,
            )
        )
    return distance


def sexagesimal_angle_to_decimal(degrees, minutes=0, seconds=0, thirds=0, fourths=0):
    """Convert sexagesimal-parsed angles to a decimal.

    Args:
        degrees (int): Angle degrees count.
        minutes (int): Angle minutes count.
        seconds (int): Angle seconds count.
        thirds (int): Angle thirds count.
        fourths (int): Angle fourths count.

    Returns:
        float: Angle in decimal degrees.
    """
    if degrees is None:
        return None

    # Degrees must be absolute or it will not sum right with subdivisions.
    absolute_decimal = abs(float(degrees))
    try:
        sign_multiplier = abs(float(degrees)) / float(degrees)
    except ZeroDivisionError:
        sign_multiplier = 1
    for count, divisor in [
        (minutes, 60),
        (seconds, 3600),
        (thirds, 216000),
        (fourths, 12960000),
    ]:
        if count:
            absolute_decimal += float(count) / divisor
    return absolute_decimal * sign_multiplier
