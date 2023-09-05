import json
from collections import Counter
from pathlib import Path

from pandas import DataFrame, concat, Series
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeClassifier
from tqdm import tqdm

from analysis.header_utils import HeadersDecoder, parse_origin, get_headers_security, get_headers_security_categories
from configs.crawling import TIMESTAMPS
from configs.utils import join_with_json_path, get_tranco_data

CATEGORIES = get_headers_security_categories()


def encode_non_numeric_features(df: DataFrame, features: list[str]) -> DataFrame:
    """Encode non-numeric `features` as new binary sub-features and replace the original `features` in `df`."""
    for feature in features:
        reshaped_feature = df[feature].to_numpy().reshape(-1, 1)
        encoder = OneHotEncoder()
        encoder.fit(reshaped_feature)
        encoded_feature = encoder.transform(reshaped_feature)

        columns = [f"{feature}::{n}" for n in encoder.get_feature_names_out()]
        encoded_feature = DataFrame(encoded_feature.toarray(), index=df.index, columns=columns)

        df = concat([df, encoded_feature], axis=1)
        df.pop(feature)

    return df


def compute_information_gain(training_data: DataFrame, target_values: Series) -> dict[str, float]:
    """Compute the information gain of splitting the training data per feature."""
    information_gain = {}
    for feature in training_data:
        classifier = DecisionTreeClassifier(criterion='gini', max_depth=1)
        classifier.fit(training_data[[feature]], target_values)
        if classifier.get_depth() > 0:
            impurity = classifier.tree_.impurity
            n_node_samples = classifier.tree_.n_node_samples
            weighted_gini = (n_node_samples[1] / n_node_samples[0]) * impurity[1] + \
                            (n_node_samples[2] / n_node_samples[0]) * impurity[2]

            if weighted_gini != impurity[0]:
                information_gain[feature] = impurity[0] - weighted_gini
    return information_gain


def attribute_differences(urls: list[tuple[int, str, str]], proximity_sets_path: Path) -> None:
    """Compute the consistency of header values within each proximity set."""
    with open(proximity_sets_path) as file:
        proximity_sets = json.load(file, cls=HeadersDecoder)

    result = {category: Counter() for category in CATEGORIES}
    for tid, _, _ in tqdm(urls):
        for timestamp in TIMESTAMPS:
            proximity_set = proximity_sets[str(tid)][str(timestamp)]
            if len(proximity_set) < 2:
                continue

            df = DataFrame(proximity_set,
                           columns=['archived_timestamp', 'headers', 'end_url', 'status_code', 'contributor',
                                    'relevant_sources', 'hosts', 'sites'])
            df['origin'] = df['end_url'].apply(parse_origin)
            df['headers_security'] = df.apply(lambda row: get_headers_security(row['headers'], row['origin']), axis=1)
            df['origin'] = df['origin'].apply(str)

            training_data = df[['contributor', 'origin', 'status_code']].copy()
            for archived_timestamp in df['archived_timestamp'].sort_values().iloc[1:-2]:
                training_data[f"archival time::<= {archived_timestamp}"] = df['archived_timestamp'].apply(
                    lambda row: row <= archived_timestamp
                )

            training_data['status_code_ok'] = training_data['status_code'].apply(lambda row: 200 <= row < 300)
            training_data['status_code_error'] = training_data['status_code'].apply(lambda row: row >= 400)

            training_data = encode_non_numeric_features(training_data, ['contributor', 'origin'])

            for category in CATEGORIES:
                target_values = df['headers_security'].apply(lambda row: row[category])

                if target_values.nunique() == 1:
                    continue

                assert target_values.nunique() == 2, target_values.nunique()

                if information_gain := compute_information_gain(training_data, target_values):
                    max_information_gain = max(information_gain.values())
                    assert max_information_gain > 0.0, information_gain

                    best_features = {f.split('::')[0] for f, v in information_gain.items() if v == max_information_gain}
                    for feature in best_features:
                        result[category][feature] += 1
                else:
                    result[category]['None'] += 1
                result[category]['total'] += 1

    with open(join_with_json_path(f"FEATURES-{proximity_sets_path.name}"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def main():
    attribute_differences(get_tranco_data(), join_with_json_path(f"PROXIMITY-SETS-{10}.json"))


if __name__ == '__main__':
    main()
