"""Execution code for spatial reference processing."""
import argparse
import logging
import math

import arcetl
import arcpy
from etlassist.pipeline import Job, execute_pipeline

from helper import dataset
from helper.misc import REAL_LOT_SQL


LOG = logging.getLogger(__name__)
"""logging.Logger: Script-level logger."""

BUFFER_FEET = {"county": 10 * 5280.0, "taxlot": 25.0}
SCALE_FACTOR = {"large": 8.0, "small": 2.0}


# Helpers.


class BoundingBox(object):
    """Container for bounding box coordinates.

    Attributes:
        xmin (float): Minimum x-coordinate.
        xmax (float): Maximum x-coordinate.
        ymin (float): Minimum y-coordinate.
        ymax (float): Maximum y-coordinate.
    """

    _coord_keys = ["xmin", "xmax", "ymin", "ymax"]

    @staticmethod
    def _coord_cmp(cmp_func, *coords):
        """Compare function for coordinates, ignoring NoneTypes.

        Args:
            cmp_func (types.FunctionType): Function to use for comparison.
            *coords (iter of floats): Coordinates to compare.

        Returns:
            float: Coordinate that satisifies the comparison.
        """
        try:
            return cmp_func(coord for coord in coords if coord is not None)

        except ValueError:
            return None

    def __init__(self, xmin=None, xmax=None, ymin=None, ymax=None):
        """Initialize instance.

        Args:
            xmin (float): Minimum x-coordinate.
            xmax (float): Maximum x-coordinate.
            ymin (float): Minimum y-coordinate.
            ymax (float): Maximum y-coordinate.
        """
        self.xmin = xmin
        self.xmax = xmax
        self.ymin = ymin
        self.ymax = ymax

    @property
    def as_dict(self):
        """Return bounding box coordinates as a dictionary.

        Returns:
            dict
        """
        return {key: getattr(self, key) for key in self._coord_keys}

    @property
    def delta_x(self):
        """Extent between x-bounds."""
        return abs(self.xmax - self.xmin)

    @property
    def delta_y(self):
        """Extent between y-bounds."""
        return abs(self.ymax - self.ymin)

    @property
    def delta_max(self):
        """Longest length between min & max on any axis."""
        return max(self.delta_x, self.delta_y)

    @property
    def esri_geometry(self):
        """Bounding box as Esri geometry object."""
        return arcpy.Polygon(
            arcpy.Array(arcpy.Point(*xy) for xy in self.xy_coordinates)
        )

    @property
    def xy_coordinates(self):
        """Bounding box as ordered XY coordinate tuples."""
        return [
            (self.xmin, self.ymin),
            (self.xmin, self.ymax),
            (self.xmax, self.ymax),
            (self.xmax, self.ymin),
        ]

    def buffer(self, distance):
        """Return new bounding box buffered given distance.

        Args:
            distance (float): Distance to buffer from bounding box.

        Returns:
            BoundingBox: Buffered bounding box.
        """
        return BoundingBox(
            xmin=self.xmin - distance,
            xmax=self.xmax + distance,
            ymin=self.ymin - distance,
            ymax=self.ymax + distance,
        )

    def extend_by_coordinates(self, xmin=None, xmax=None, ymin=None, ymax=None):
        """Return new box extended by given coordinates.

        If any of the coordinates do not extend the box, it will be ignored.

        Args:
            xmin (float): Minimum x-coordinate to extend bounding box with.
            xmax (float): Maximum x-coordinate to extend bounding box with.
            ymin (float): Minimum y-coordinate to extend bounding box with.
            ymax (float): Maximum y-coordinate to extend bounding box with.

        Returns:
            BoundingBox: Buffered bounding box.
        """
        return BoundingBox(
            xmin=self._coord_cmp(min, self.xmin, xmin),
            xmax=self._coord_cmp(max, self.xmax, xmax),
            ymin=self._coord_cmp(min, self.ymin, ymin),
            ymax=self._coord_cmp(max, self.ymax, ymax),
        )

    def extend_by_esri_geometry(self, geometry):
        """Return new box extended by given Esri geometry object extent.

        If the geometry does not extend the box, it will be ignored.

        Args:
            geometry (arcpy.Geometry): Geometry to extend bounding box with.

        Returns:
            BoundingBox: Buffered bounding box.
        """
        if geometry:
            new_box = self.extend_by_coordinates(
                xmin=geometry.extent.XMin,
                xmax=geometry.extent.XMax,
                ymin=geometry.extent.YMin,
                ymax=geometry.extent.YMax,
            )
        else:
            new_box = BoundingBox(**self.as_dict)
        return new_box


def bind_focus_box(focus_box, bounds_box, replace_all=False):
    """Return new focus box adjusted to fit in given bounds box.

    Args:
        focus_box (BoundingBox): Bounding box to bind.
        bounds_box (BoundingBox): Bounding box with which to bind.
        replace_all (bool): Replace all bounds with the bounds box if True, rather than
            just the overrun bound.

    Returns:
        BoundingBox
    """
    # No overruns means nothing adjusted.
    if not any(
        [
            focus_box.xmin < bounds_box.xmin,
            focus_box.xmax > bounds_box.xmax,
            focus_box.ymin < bounds_box.ymin,
            focus_box.ymax > bounds_box.ymax,
        ]
    ):
        new_box = BoundingBox(**focus_box.as_dict)
    elif replace_all:
        new_box = BoundingBox(**bounds_box.as_dict)
    else:
        new_box = BoundingBox(
            xmin=max(focus_box.xmin, bounds_box.xmin),
            xmax=min(focus_box.xmax, bounds_box.xmax),
            ymin=max(focus_box.ymin, bounds_box.ymin),
            ymax=min(focus_box.ymax, bounds_box.ymax),
        )
    return new_box


def expand_focus_box(focus_box, factor):
    """Return "focus" bounding box with expansion factor applied.

    Args:
        focus_box (BoundingBox): Box to expand.
        factor (float): Factor to expand bounding box by.

    Returns:
        BoundingBox
    """
    max_factor = focus_box.delta_max * factor / 2.0
    return BoundingBox(
        xmin=math.floor(focus_box.xmin + focus_box.delta_x / 2.0 - max_factor),
        ymin=math.floor(focus_box.ymin + focus_box.delta_y / 2.0 - max_factor),
        xmax=math.ceil(focus_box.xmax - focus_box.delta_x / 2.0 + max_factor),
        ymax=math.ceil(focus_box.ymax - focus_box.delta_y / 2.0 + max_factor),
    )


def taxlot_bounding_boxes():
    """Generate tuples of maptaxlot & bounding box objects.

    Yields:
        tuple
    """
    for maptaxlot, geom in arcetl.attributes.as_iters(
        dataset.TAXLOT.path("pub"),
        field_names=["maptaxlot", "shape@"],
        dataset_where_sql=REAL_LOT_SQL,
    ):
        yield (maptaxlot, BoundingBox().extend_by_esri_geometry(geom))


# ETLs.


def taxlot_focus_box_etl():
    """Run ETL for taxlot focus boxes."""
    county_box = BoundingBox(
        xmin=3956505, xmax=4588181, ymin=650607, ymax=972071
    ).buffer(BUFFER_FEET["county"])
    for scale in ["large", "small"]:
        taxlot_boxes = taxlot_bounding_boxes()
        buffered_boxes = (
            (maptaxlot, box.buffer(distance=BUFFER_FEET["taxlot"]))
            for maptaxlot, box in taxlot_boxes
        )
        # Expansion must come before binding.
        expanded_boxes = (
            (maptaxlot, expand_focus_box(box, SCALE_FACTOR[scale]))
            for maptaxlot, box in buffered_boxes
        )
        bound_boxes = (
            (maptaxlot, bind_focus_box(box, county_box, replace_all=True))
            for maptaxlot, box in expanded_boxes
        )
        features = ((maptaxlot, box.esri_geometry) for maptaxlot, box in bound_boxes)
        with arcetl.ArcETL("Taxlot Focus Boxes - {}".format(scale.title())) as etl:
            etl.init_schema(dataset.TAXLOT_FOCUS_BOX.path(scale))
            etl.transform(
                arcetl.features.insert_from_iters,
                insert_features=features,
                field_names=["maptaxlot", "shape@"],
            )
            etl.load(dataset.TAXLOT_FOCUS_BOX.path(scale))


# Jobs.


WEEKLY_JOB = Job("Spatial_Reference_Datasets_Weekly", etls=[taxlot_focus_box_etl])


# Execution.


def main():
    """Script execution code."""
    args = argparse.ArgumentParser()
    args.add_argument("pipelines", nargs="*", help="Pipeline(s) to run")
    available_names = {key for key in list(globals()) if not key.startswith("__")}
    pipeline_names = args.parse_args().pipelines
    if pipeline_names and available_names.issuperset(pipeline_names):
        pipelines = [globals()[arg] for arg in args.parse_args().pipelines]
        for pipeline in pipelines:
            execute_pipeline(pipeline)
    else:
        console = logging.StreamHandler()
        LOG.addHandler(console)
        if not pipeline_names:
            LOG.error("No pipeline arguments.")
        for arg in pipeline_names:
            if arg not in available_names:
                LOG.error("`%s` not available in exec.", arg)
        LOG.error(
            "Available objects in exec: %s",
            ", ".join("`{}`".format(name) for name in sorted(available_names)),
        )


if __name__ == "__main__":
    main()
