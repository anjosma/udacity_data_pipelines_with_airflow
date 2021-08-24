import yaml

def load_yml_config(file_path: str) -> dict:
    """Transfor a YML configuration file into a Python Dictionary

    Args:
        file_path (str): YML configuration file path

    Returns:
        dict: Dictionary containing YML file content
    """
    with open(file_path, 'r') as file:
        return yaml.load(file, Loader=yaml.FullLoader)