"""ETL transformation objects.

Mirror objects between transform and update submodules when it makes sense.
Mirror differences:
    * Transforms have ArcETL instance passed as argument; updates have
        dataset_path instead.
    * Transforms do not need use_edit_session=True for feature/attribute
        changes; updates do.
"""
import functools
import logging
import os

# import arcetl  # Imported locally to avoid slow imports.

from . import value  # pylint: disable=relative-beyond-top-level


__all__ = (
    'add_missing_fields',
    'clean_whitespace',
    'clean_whitespace_without_clear',
    'clear_all_values',
    'clear_non_numeric_text',
    'clear_nonpositive',
    'etl_dataset',
    'force_lowercase',
    'force_title_case',
    'force_uppercase',
    'force_yn',
    'init_transform',
    'insert_features_from_dicts',
    'insert_features_from_iters',
    'insert_features_from_paths',
    'rename_fields',
    'update_attributes_by_domains',
    'update_attributes_by_functions',
    'update_attributes_by_joined_values',
    'update_attributes_by_mappings',
    'update_attributes_by_unique_ids',
    'update_attributes_by_values',
    )
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def add_missing_fields(etl, dataset_instance, tags=None):
    """Add missing fields from the dataset instance.

    If tags is NoneType, all fields will be added. Otherwise will add only
    fields with tags in the sequence.
    """
    import arcetl
    tags = set(arcetl.contain(tags))
    if not tags:
        fields_meta = dataset_instance.fields
    else:
        fields_meta = (field for field in dataset_instance.fields
                       if set(field.get('tags')) & tags)
    func = functools.partial(
        etl.transform, transformation=arcetl.dataset.add_field_from_metadata,
        exist_ok=True,
        )
    tuple(func(add_metadata=meta) for meta in fields_meta)


def clean_whitespace(etl, field_names, **kwargs):
    """Clean whitespace on values of fields."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_function,
        function=value.clean_whitespace, **kwargs
        )
    tuple(func(field_name=name) for name in field_names)


def clean_whitespace_without_clear(etl, field_names, **kwargs):
    """Clean whitespace on values of fields."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_function,
        function=value.clean_whitespace_without_clear, **kwargs
        )
    tuple(func(field_name=name) for name in field_names)


def clear_all_values(etl, field_names, **kwargs):
    """Clear all values, changing them to NoneTypes."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_value,
        **kwargs
        )
    tuple(func(field_name=name, value=None) for name in field_names)


def clear_non_numeric_text(etl, field_names, **kwargs):
    """Clear non-numeric text values of fields."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_function,
        function=(lambda x: x if value.is_numeric(x) else None), **kwargs
        )
    tuple(func(field_name=name) for name in field_names)


def clear_nonpositive(etl, field_names, **kwargs):
    """Clear nonpositive values of fields."""
    import arcetl
    def return_value(original_value):
        """Return value if a positive number representation, else NoneType."""
        try:
            result = original_value if float(original_value) > 0 else None
        except (ValueError, TypeError):
            result = None
        return result
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_function,
        function=return_value, **kwargs
        )
    tuple(func(field_name=name) for name in field_names)


##TODO: Replace usage with init_transform.
##NOTE # Loading shapefiles destroys spatial indexes: restore after load. # if kwargs.get('adjust_for_shapefile'): arcetl.dataset.add_index(output_path, field_names=('shape',), fail_on_lock_ok=True)
##NOTE: Remove use_edit_session kwarg.
##NOTE: feature_count = etl.load(output_path, use_edit_session=kwargs['use_edit_session'])
##NOTE: Or feature_count = etl.update(...)
def etl_dataset(output_path, source_path=None, **kwargs):
    """Run basic ETL for dataset.

    Args:
        output_path (str): Path of the dataset to load/update.
        source_path (str): Path of the dataset to extract. If None, will
            initialize transform dataset with the output path's schema.

    Keyword Args:
        adjust_for_shapefile (bool): Flag to indicate running ArcETL's
            shapefile attribute adjuster function.
        clean_whitespace_field_names (iter): Collection of field names to
            clean their values of excess whitespace.
        dissolve_field_names (iter): Collection of field names to dissolve
            the features on.
        etl_name (str): Name to give the ETL operation.
        extract_where_sql (str): SQL where-clause for extract subselection.
        field_name_change_map (dict): Mapping of names to their replacement
            name.
        insert_dataset_paths (iter of str): Collection of dataset paths to
            insert features from.
        insert_dicts_kwargs (iter of dict): Keyword arguments for inserting
            features from dictionaries.
        insert_iters_kwargs (iter of dict): Keyword arguments for inserting
            features from iterables.
        unique_id_field_names (iter): Collection of field names to update
            unique IDs in.
        xy_tolerance (float, str): Representation of a distance for
            operations that can interpret a tolerance.

    Returns:
        collections.Counter: Counts for each update type.

    """
    kwargs.setdefault('adjust_for_shapefile', False)
    kwargs.setdefault('clean_whitespace_field_names', ())
    kwargs.setdefault('etl_name', os.path.basename(output_path))
    kwargs.setdefault('dissolve_field_names')
    kwargs.setdefault('extract_where_sql')
    kwargs.setdefault('field_name_change_map', {})
    kwargs.setdefault('insert_dataset_paths', ())
    kwargs.setdefault('insert_dicts_kwargs', ())
    kwargs.setdefault('insert_iters_kwargs', ())
    kwargs.setdefault('unique_id_field_names', ())
    kwargs.setdefault('use_edit_session', False)
    kwargs.setdefault('xy_tolerance')
    import arcetl
    with arcetl.ArcETL(kwargs['etl_name']) as etl:
        # Init.
        if source_path:
            etl.extract(source_path,
                        extract_where_sql=kwargs['extract_where_sql'])
        else:
            etl.init_schema(output_path)
        rename_fields(etl, kwargs['field_name_change_map'])
        # Insert features.
        for func, key in (
                (insert_features_from_paths, 'insert_dataset_paths'),
                (insert_features_from_dicts, 'insert_dicts_kwargs'),
                (insert_features_from_iters, 'insert_iters_kwargs'),
            ):
            func(etl, kwargs[key])
        # Alter attributes.
        clean_whitespace(etl, kwargs['clean_whitespace_field_names'])
        # Combine features.
        if kwargs['dissolve_field_names']:
            etl.transform(arcetl.features.dissolve,
                          dissolve_field_names=kwargs['dissolve_field_names'],
                          tolerance=kwargs['xy_tolerance'])
        # Finalize attributes.
        update_attributes_by_unique_ids(etl, kwargs['unique_id_field_names'])
        if kwargs['adjust_for_shapefile']:
            etl.transform(arcetl.combo.adjust_for_shapefile)
        feature_count = etl.load(output_path,
                                 use_edit_session=kwargs['use_edit_session'])
        # Loading shapefiles destroys spatial indexes: restore after load.
        if kwargs.get('adjust_for_shapefile'):
            arcetl.dataset.add_index(output_path, field_names=('shape',),
                                     fail_on_lock_ok=True)
    return feature_count


def force_lowercase(etl, field_names, **kwargs):
    """Force lowercase on values of fields."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_function,
        function=value.force_lowercase, **kwargs
        )
    tuple(func(field_name=name) for name in field_names)


def force_title_case(etl, field_names, **kwargs):
    """Force title case on values of fields."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_function,
        function=value.force_title_case, **kwargs
        )
    tuple(func(field_name=name) for name in field_names)


def force_uppercase(etl, field_names, **kwargs):
    """Force uppercase on values of fields."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_function,
        function=value.force_uppercase, **kwargs
        )
    tuple(func(field_name=name) for name in field_names)


def force_yn(etl, field_names, default=None, **kwargs):
    """Ensure only 'Y' or 'N' values on fields."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_function,
        function=functools.partial(value.force_yn, default=default), **kwargs
        )
    tuple(func(field_name=name) for name in field_names)


def init_transform(source_path=None, template_path=None, **kwargs):
    """Run basic init & transform for an ETL procedure.

    Since this returns the ArcETL instance, it does not manage the context.
    It is recommended that you call the `close` method when done.

    Args:
        source_path (str): Path of the dataset to extract. If None, will
            initialize transform dataset with the output path's schema.
        template_path (str): Path of the dataset to use as schema template.
            Only applicable if source_path is None.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        adjust_for_shapefile (bool): Flag to indicate running ArcETL's
            shapefile attribute adjuster function.
        clean_whitespace_field_names (iter): Collection of field names to
            clean their values of excess whitespace.
        dissolve_field_names (iter): Collection of field names to dissolve
            the features on.
        extract_where_sql (str): SQL where-clause for extract subselection.
        field_name_change_map (dict): Mapping of names to their replacement
            name.
        insert_dataset_paths (iter of str): Collection of dataset paths to
            insert features from.
        insert_dicts_kwargs (iter of dict): Keyword arguments for inserting
            features from dictionaries.
        insert_iters_kwargs (iter of dict): Keyword arguments for inserting
            features from iterables.
        unique_id_field_names (iter): Collection of field names to update
            unique IDs in.
        xy_tolerance (float, str): Representation of a distance for
            operations that can interpret a tolerance.

    Returns:
        arcetl.ArcETL: ETL manager object.

    """
    kwargs.setdefault('adjust_for_shapefile', False)
    kwargs.setdefault('clean_whitespace_field_names', ())
    kwargs.setdefault('dissolve_field_names')
    kwargs.setdefault('extract_where_sql')
    kwargs.setdefault('field_name_change_map', {})
    kwargs.setdefault('insert_dataset_paths', ())
    kwargs.setdefault('insert_dicts_kwargs', ())
    kwargs.setdefault('insert_iters_kwargs', ())
    kwargs.setdefault('unique_id_field_names', ())
    kwargs.setdefault('xy_tolerance')
    import arcetl
    # Init.
    try:
        if source_path:
            etl = arcetl.ArcETL('Extract from ' + os.path.basename(source_path))
            etl.extract(source_path, extract_where_sql=kwargs['extract_where_sql'])
        else:
            etl = arcetl.ArcETL('Init from ' + os.path.basename(template_path))
            etl.init_schema(template_path)
        rename_fields(etl, kwargs['field_name_change_map'])
        # Insert features.
        for func, arg in ((insert_features_from_paths, 'insert_dataset_paths'),
                          (insert_features_from_dicts, 'insert_dicts_kwargs'),
                          (insert_features_from_iters, 'insert_iters_kwargs')):
            func(etl, kwargs[arg])
        # Alter attributes.
        clean_whitespace(etl, kwargs['clean_whitespace_field_names'])
        # Combine features.
        if kwargs['dissolve_field_names'] is not None:
            etl.transform(arcetl.features.dissolve,
                          dissolve_field_names=kwargs['dissolve_field_names'],
                          tolerance=kwargs['xy_tolerance'])
        # Finalize attributes.
        update_attributes_by_unique_ids(etl, kwargs['unique_id_field_names'])
        if kwargs['adjust_for_shapefile']:
            etl.transform(arcetl.combo.adjust_for_shapefile)
    except:
        etl.close()
        raise
    return etl


def insert_features_from_dicts(etl, insert_dicts_kwargs):
    """Insert features into the dataset from iterables of dictionaries.

    Note: This runs inserts on a sequence of iterables containing dictionaries.

    Args:
        etl (arcetl.ArcETL): Instance of the ArcETL ETL object.
        insert_dicts_kwargs (iter): Sequence of keyword argument
            dictionaries for inserting dictionaries. Each insert_kwargs
            contains the following keys:
                insert_features (iter): Iterable containing dictionaries
                    representing features.
                field_names (iter): Collection of field names to insert.
    """
    import arcetl
    func = functools.partial(etl.transform,
                             transformation=arcetl.features.insert_from_dicts)
    tuple(func(**insert_kwargs) for insert_kwargs in insert_dicts_kwargs)


def insert_features_from_iters(etl, insert_iters_kwargs):
    """Insert features into the dataset from iterables of iterable items.

    Note: This runs inserts on a sequence of iterables containing iterables.

    Args:
        etl (arcetl.ArcETL): Instance of the ArcETL ETL object.
        insert_iters_kwargs (iter): Sequence of keyword argument
            dictionaries for inserting iterables. Each insert_kwargs contains
            the following keys:
                insert_features (iter): Iterable containing iterable items
                    representing features.
                field_names (iter): Collection of field names to insert.
                    These must match the order of their attributes in the
                    insert_features items.
    """
    import arcetl
    func = functools.partial(etl.transform,
                             transformation=arcetl.features.insert_from_iters)
    tuple(func(**insert_kwargs) for insert_kwargs in insert_iters_kwargs)


def insert_features_from_paths(etl, insert_dataset_paths):
    """Insert features into the dataset from given dataset path."""
    import arcetl
    func = functools.partial(etl.transform,
                             transformation=arcetl.features.insert_from_path)
    tuple(func(insert_dataset_path=path) for path in insert_dataset_paths)


def rename_fields(etl, field_name_change_map):
    """Rename fields."""
    import arcetl
    func = functools.partial(etl.transform,
                             transformation=arcetl.dataset.rename_field)
    tuple(func(field_name=old, new_field_name=new)
          for old, new in field_name_change_map.items())


def update_attributes_by_domains(etl, update_kwargs):
    """Update attributes by domain codes."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_domain_code,
        )
    tuple(func(**kwargs) for kwargs in update_kwargs)


def update_attributes_by_functions(etl, update_kwargs):
    """Update attributes by functions."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_function,
        )
    tuple(func(**kwargs) for kwargs in update_kwargs)


def update_attributes_by_joined_values(etl, update_kwargs):  # pylint: disable=invalid-name
    """Update attributes by joined values."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_joined_value,
        )
    tuple(func(**kwargs) for kwargs in update_kwargs)


def update_attributes_by_mappings(etl, update_kwargs):
    """Update attributes by mapping functions."""
    import arcetl
    func = functools.partial(
        etl.transform,
        transformation=arcetl.attributes.update_by_mapping,
        )
    tuple(func(**kwargs) for kwargs in update_kwargs)


def update_attributes_by_unique_ids(etl, field_names):
    """Update attributes by unique ID values."""
    import arcetl
    func = functools.partial(
        etl.transform, transformation=arcetl.attributes.update_by_unique_id,
        )
    tuple(func(field_name=name) for name in field_names)


def update_attributes_by_values(etl, update_kwargs):
    """Update attributes by values."""
    import arcetl
    func = functools.partial(etl.transform,
                             transformation=arcetl.attributes.update_by_value)
    tuple(func(**kwargs) for kwargs in update_kwargs)
