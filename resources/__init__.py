from databricks.bundles.core import (
    Bundle,
    Resources,
    load_resources_from_current_package_module,
)

from resources.unity_catalog import load_unity_catalog


def load_resources(bundle: Bundle) -> Resources:
    """
    'load_resources' function is referenced in databricks.yml and is responsible for loading
    bundle resources defined in Python code. This function is called by Databricks CLI during
    bundle deployment. After deployment, this function is not used.
    """

    # the default implementation loads all Python files in 'resources' directory
    resources = load_resources_from_current_package_module()
    # Unity Catalog resources are prod-only, which needs bundle.target that module-level resources cannot see
    resources.add_resources(load_unity_catalog(bundle))
    return resources
