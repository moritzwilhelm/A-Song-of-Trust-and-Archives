import random

from configs.utils import get_absolute_tranco_file_path

SEED = 1337
random.seed(SEED)


def sample(buckets=10, domains_per_bucket=2000):
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
    sample()


if __name__ == '__main__':
    main()