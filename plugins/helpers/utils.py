import yaml

def load_yml_config(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        return yaml.load(file, Loader=yaml.FullLoader)