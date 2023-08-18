import random

from configs.utils import get_absolute_tranco_file_path

SEED = 1337

RANDOM_SAMPLING_TABLE_NAME = 'RANDOM_SAMPLING_HISTORICAL_DATA'


def sample(buckets: int = 10, domains_per_bucket: int = 2000) -> None:
    """Apply stratified random sampling on the tranco file."""
    with open(get_absolute_tranco_file_path()) as file:
        lines = file.readlines()

    sampled_lines = []
    bucket_size = len(lines) // buckets
    for i in range(buckets):
        sampled_lines += random.sample(lines[bucket_size * i:bucket_size * (i + 1)], domains_per_bucket)

    with open(get_absolute_tranco_file_path().parent.joinpath(f"tranco_random_sample_{SEED}.csv"), 'w') as file:
        file.writelines(sorted(sampled_lines, key=lambda data: int(data.split(',')[0])))


def main():
    random.seed(SEED)
    sample()


if __name__ == '__main__':
    main()
