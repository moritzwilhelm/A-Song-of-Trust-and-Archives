import json
from itertools import chain

from configs.utils import PROJECT_ROOT


def create_trackers_json() -> None:
    """Parse Disconnect list and create tracking.json containing the set of tracking domains."""
    with open(PROJECT_ROOT.joinpath('src', 'configs', 'disconnect_entities.json')) as file:
        entities = json.load(file)

    trackers = set()
    for entity_name, entity_info in entities['entities'].items():
        for domain in chain(*entity_info.values()):
            trackers.add(domain)

    with open(PROJECT_ROOT.joinpath('src', 'configs', 'files', 'trackers.json'), 'w') as file:
        json.dump(sorted(trackers), file, indent=2)


def main():
    create_trackers_json()


if __name__ == '__main__':
    main()
