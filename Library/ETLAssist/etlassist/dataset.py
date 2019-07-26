"""Dataset objects."""
import itertools
import logging
import sys

# import arcetl  # Imported locally to avoid slow imports.

if sys.version_info.major >= 3:
    basestring = str


__all__ = (
    'Dataset',
    'last_change_date',
    )
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


##TODO: New classes, similar to SQLAlchemy style - Field, <type>, ...


class Dataset(object):
    """Representation of dataset information."""

    valid_geometry_types = ('point', 'multipoint', 'polygon', 'polyline')

    @staticmethod
    def init_tag_property(value):
        """Initialize a tag-style property."""
        if isinstance(value, dict):
            return value
        elif isinstance(value, basestring):
            return {None: value}
        else:
            raise TypeError("Invalid type for tag property.")

    def __init__(self, fields, geometry_type=None, path=None, **kwargs):
        self._geometry_type = None
        self.geometry_type = geometry_type
        self.fields = list(fields)
        self._path = self.init_tag_property(path)
        self._odbc = self.init_tag_property(kwargs.get('odbc', {None: None}))
        self._sql_name = self.init_tag_property(kwargs.get('sql_name', {None: None}))

    @property
    def field_names(self):
        """Field names property."""
        return [field['name'] for field in self.fields]

    @property
    def geometry_type(self):
        """Geometry type property."""
        return self._geometry_type

    @geometry_type.setter
    def geometry_type(self, value):
        """Geometry type property setter.

        NoneType means dataset has no geometry (a table).
        """
        if value is None:
            self._geometry_type = value
        elif value.lower() in Dataset.valid_geometry_types:
            self._geometry_type = value.lower()

    @property
    def id_field_names(self):
        """Identifier field names property."""
        return [field['name'] for field in self.fields if field.get('is_id')]

    @property
    def id_fields(self):
        """Identifier fields property."""
        return [field for field in self.fields if field['is_id']]

    def add_path(self, tag, path):
        """Add a path for the given tag."""
        self._path[tag] = path

    def create(self, path, field_tag=None, spatial_reference_item=None):
        """Create dataset from instance properties."""
        import arcetl
        # Check for path in path-tags; otherwise assume path is literal.
        dataset_path = self._path.get(path, path)
        if field_tag:
            field_metadata_list = (field for field in self.fields
                                   if field_tag in field['tags'])
        else:
            field_metadata_list = self.fields
        arcetl.dataset.create(dataset_path, field_metadata_list,
                              geometry_type=self.geometry_type,
                              spatial_reference_item=spatial_reference_item)
        return dataset_path

    def path(self, tag=None):
        """Return path string associated with the given tag."""
        return self._path[tag]

    def odbc(self, tag=None):
        """Return ODBC string associated with the given tag."""
        return self._odbc[tag]

    def sql_name(self, tag=None):
        """Return ODBC string associated with the given tag."""
        return self._sql_name[tag]


def last_change_date(dataset_path, init_date_field_name='init_date',
                     mod_date_field_name='mod_date'):
    """Return date of the last change on dataset with tracking fields."""
    import arcetl
    field_names = (init_date_field_name, mod_date_field_name)
    date_iters = arcetl.attributes.as_iters(dataset_path, field_names)
    dates = set(itertools.chain.from_iterable(date_iters))
    # datetimes can't compare to NoneTypes.
    if None in dates:
        dates.remove(None)
    return max(dates) if dates else None
