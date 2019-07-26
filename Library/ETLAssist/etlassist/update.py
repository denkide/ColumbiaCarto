"""Data update objects.

Mirror objects between transform and update submodules when it makes sense.
Mirror differences:
    * Transforms have ArcETL instance passed as argument; updates have
        dataset_path instead.
    * Transforms do not need use_edit_session=True for feature/attribute
        changes; updates do.
"""
import functools
import logging

# import arcetl  # Imported locally to avoid slow imports.

from . import value


__all__ = (
    'clean_whitespace',
    'clean_whitespace_without_clear',
    'clear_all_values',
    'clear_non_numeric_text',
    'clear_nonpositive',
    'insert_features_from_dicts',
    'insert_features_from_iters',
    'insert_features_from_paths',
    'force_lowercase',
    'force_title_case',
    'force_uppercase',
    'force_yn',
    'update_attributes_by_domains',
    'update_attributes_by_functions',
    'update_attributes_by_joined_values',
    'update_attributes_by_mappings',
    'update_attributes_by_unique_ids',
    'update_attributes_by_values',
    'update_dataset',
    )
LOG = logging.getLogger(__name__)
"""logging.Logger: Module-level logger."""


def clean_whitespace(dataset_path, field_names, **kwargs):
    """Clean whitespace on values of fields."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_function,
                             dataset_path=dataset_path,
                             function=value.clean_whitespace,
                             **kwargs)
    tuple(func(field_name=name) for name in field_names)


def clean_whitespace_without_clear(dataset_path, field_names, **kwargs):
    """Clean whitespace on values of fields."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_function,
                             dataset_path=dataset_path,
                             function=value.clean_whitespace_without_clear,
                             **kwargs)
    tuple(func(field_name=name) for name in field_names)


def clear_all_values(dataset_path, field_names, **kwargs):
    """Clear all values, changing them to NoneTypes."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_value,
                             dataset_path=dataset_path,
                             **kwargs)
    tuple(func(field_name=name, value=None) for name in field_names)


def clear_non_numeric_text(dataset_path, field_names, **kwargs):
    """Clear non-numeric text values of fields."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_function,
                             dataset_path=dataset_path,
                             function=(lambda x: x if value.is_numeric(x)
                                       else None),
                             **kwargs)
    tuple(func(field_name=name) for name in field_names)


def clear_nonpositive(dataset_path, field_names, **kwargs):
    """Clear nonpositive values of fields."""
    import arcetl
    def return_value(original_value):
        """Return value if a positive number representation, else NoneType."""
        try:
            result = original_value if float(original_value) > 0 else None
        except (ValueError, TypeError):
            result = None
        return result
    func = functools.partial(arcetl.attributes.update_by_function,
                             dataset_path=dataset_path,
                             function=return_value,
                             **kwargs)
    tuple(func(field_name=name) for name in field_names)


def force_lowercase(dataset_path, field_names, **kwargs):
    """Force lowercase on values of fields."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_function,
                             dataset_path=dataset_path,
                             function=value.force_lowercase,
                             **kwargs)
    tuple(func(field_name=name) for name in field_names)


def force_title_case(dataset_path, field_names, **kwargs):
    """Force title case on values of fields."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_function,
                             dataset_path=dataset_path,
                             function=value.force_title_case,
                             **kwargs)
    tuple(func(field_name=name) for name in field_names)


def force_uppercase(dataset_path, field_names, **kwargs):
    """Force uppercase on values of fields."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_function,
                             dataset_path=dataset_path,
                             function=value.force_uppercase,
                             **kwargs)
    tuple(func(field_name=name) for name in field_names)


def force_yn(dataset_path, field_names, default=None, **kwargs):
    """Ensure only 'Y' or 'N' values on fields."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_function,
                             dataset_path=dataset_path,
                             function=functools.partial(value.force_yn,
                                                        default=default),
                             **kwargs)
    tuple(func(field_name=name) for name in field_names)


def insert_features_from_dicts(dataset_path, insert_dicts_kwargs, **kwargs):
    """Insert features into the dataset from iterables of dictionaries.

    Note: This runs inserts on a sequence of iterables containing dictionaries.

    Args:
        dataset_path (str): Path for the update dataset.
        insert_dicts_kwargs (iter): Sequence of keyword argument
            dictionaries for inserting dictionaries. Each insert_kwargs
            contains the following keys:
                insert_features (iter): Iterable containing dictionaries
                    representing features.
                field_names (iter): Collection of field names to insert.
    """
    import arcetl
    func = functools.partial(arcetl.features.insert_from_dicts,
                             dataset_path=dataset_path, **kwargs)
    tuple(func(**insert_kwargs) for insert_kwargs in insert_dicts_kwargs)


def insert_features_from_iters(dataset_path, insert_iters_kwargs, **kwargs):
    """Insert features into the dataset from iterables of iterable items.

    Note: This runs inserts on a sequence of iterables containing iterables.

    Args:
        dataset_path (str): Path for the update dataset.
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
    func = functools.partial(arcetl.features.insert_from_iters,
                             dataset_path=dataset_path, **kwargs)
    tuple(func(**insert_kwargs) for insert_kwargs in insert_iters_kwargs)


def insert_features_from_paths(dataset_path, insert_dataset_paths, **kwargs):
    """Insert features into the dataset from given dataset paths."""
    import arcetl
    func = functools.partial(arcetl.features.insert_from_path,
                             dataset_path=dataset_path, **kwargs)
    tuple(func(insert_dataset_path=path) for path in insert_dataset_paths)


def update_attributes_by_domains(dataset_path, update_kwargs, **kwargs):
    """Update attributes by domain codes."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_domain_code,
                             dataset_path=dataset_path, **kwargs)
    tuple(func(**kwargs) for kwargs in update_kwargs)


def update_attributes_by_functions(dataset_path, update_kwargs, **kwargs):
    """Update attributes by functions."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_function,
                             dataset_path=dataset_path, **kwargs)
    tuple(func(**kwargs) for kwargs in update_kwargs)


def update_attributes_by_joined_values(dataset_path, update_kwargs, **kwargs):  # pylint: disable=invalid-name
    """Update attributes by joined values."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_joined_value,
                             dataset_path=dataset_path, **kwargs)
    tuple(func(**kwargs) for kwargs in update_kwargs)


def update_attributes_by_mappings(dataset_path, update_kwargs, **kwargs):
    """Update attributes by mapping functions."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_mapping,
                             dataset_path=dataset_path, **kwargs)
    tuple(func(**kwargs) for kwargs in update_kwargs)


def update_attributes_by_unique_ids(dataset_path, field_names, **kwargs):
    """Update attributes by unique ID values."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_unique_id,
                             dataset_path, **kwargs)
    tuple(func(field_name) for field_name in field_names)


def update_attributes_by_values(dataset_path, update_kwargs):
    """Update attributes by values."""
    import arcetl
    func = functools.partial(arcetl.attributes.update_by_value,
                             dataset_path=dataset_path, **kwargs)
    tuple(func(**kwargs) for kwargs in update_kwargs)


def update_dataset(dataset_path, **kwargs):
    """Run basic update for dataset."""
    # Insert features.
    insert_features_from_paths(dataset_path,
                               kwargs.get('insert_dataset_paths', ()))
    insert_features_from_dicts(dataset_path,
                               kwargs.get('insert_dicts_kwargs', ()))
    insert_features_from_iters(dataset_path,
                               kwargs.get('insert_iters_kwargs', ()))
    # Alter attributes.
    clean_whitespace(dataset_path,
                     kwargs.get('clean_whitespace_field_names', ()))
    force_lowercase(dataset_path,
                    kwargs.get('force_lowercase_field_names', ()))
    force_title_case(dataset_path,
                     kwargs.get('force_title_case_field_names', ()))
    force_uppercase(dataset_path,
                    kwargs.get('force_uppercase_field_names', ()))
    # Finalize attributes.
    update_attributes_by_unique_ids(dataset_path,
                                    kwargs.get('unique_id_field_names', ()))
