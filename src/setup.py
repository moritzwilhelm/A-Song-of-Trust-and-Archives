from pathlib import Path

from setuptools import setup, find_packages


def update_utils_file() -> None:
    """Dynamically update PROJECT_SOURCE constant."""
    utils_file_path = Path(__file__).parent.resolve().joinpath('configs', 'utils.py')

    with open(utils_file_path) as file:
        utils_file_content = file.read()

    with open(utils_file_path, 'w') as file:
        file.write(utils_file_content.replace('<AUTOMATICALLY-REPLACED-DURING-INSTALL>',
                                              str(Path(__file__).parents[1].resolve())))


update_utils_file()

setup(
    name='master-thesis',
    version='1.0',
    packages=find_packages(),
    package_data={'configs': ['tranco_W9JG9.csv', 'tranco_random_sample_1337.csv']},
    include_package_data=True
)
