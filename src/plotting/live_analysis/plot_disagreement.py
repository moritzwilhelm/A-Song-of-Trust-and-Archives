import json
from collections import defaultdict
from pathlib import Path

from analysis.live.analyze_disagreement import ARCHIVE_TABLE_NAME
from configs.analysis import SECURITY_MECHANISM_HEADERS
from configs.utils import join_with_json_path
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME


def build_headers_table(input_path: Path) -> None:
    with open(input_path) as file:
        data = defaultdict(int, json.load(file))

    header_lines = []
    for mechanism, header in SECURITY_MECHANISM_HEADERS.items():
        usage = data[f"USES_{header}"]
        syn_diff = data[f"SYNTAX_DIFFERENCE_{mechanism}"]
        sem_diff = data[f"SEMANTICS_DIFFERENCE_{mechanism}"]
        header_lines.append(
            f"\t\t{mechanism} & {usage:,} & "
            fr"{syn_diff:,} ({syn_diff / usage * 100:.2f}\%) & "
            fr"{sem_diff:,} ({sem_diff / usage * 100:.2f}\%) \\"
        )
        for reason in 'ORIGIN_MISMATCH', 'USER_AGENT', 'NO_INFORMATION':
            syn_diff_reason = data[f"SYNTAX_DIFFERENCE_{mechanism}::{reason}"]
            sem_diff_reason = data[f"SEMANTICS_DIFFERENCE_{mechanism}::{reason}"]
            header_lines.append(
                f"\t\t\quad\emph{{– {reason}}} &  & "
                fr"{syn_diff_reason:,} ({syn_diff_reason / syn_diff * 100:.2f}\%) & "
                fr"{sem_diff_reason:,} ({sem_diff_reason / sem_diff * 100:.2f}\%) \\"
            )
    header_lines = '\n'.join(header_lines)

    usage = data['USES_ANY']
    syn_diff = data['SYNTAX_DIFFERENCE']
    sem_diff = data['SEMANTICS_DIFFERENCE']
    any_header_lines = [
        f"\t\t\\textit{{Any header}} & {usage:,} & "
        fr"{syn_diff:,} ({syn_diff / usage * 100:.2f}\%) & "
        fr"{sem_diff:,} ({sem_diff / usage * 100:.2f}\%) \\"
    ]

    for reason in 'ORIGIN_MISMATCH', 'USER_AGENT', 'NO_INFORMATION':
        syn_diff_reason = data[f"SYNTAX_DIFFERENCE::{reason}"]
        sem_diff_reason = data[f"SEMANTICS_DIFFERENCE::{reason}"]
        any_header_lines.append(
            f"\t\t\quad\emph{{– {reason}}} &  & "
            fr"{syn_diff_reason:,} ({syn_diff_reason / syn_diff * 100:.2f}\%) & "
            fr"{sem_diff_reason:,} ({sem_diff_reason / sem_diff * 100:.2f}\%) \\"
        )
    any_header_lines = '\n'.join(any_header_lines)

    print(fr"""
\begin{{table}}
    \centering
    \begin{{tabular}}{{l|rrr}}
        & \multicolumn{{3}}{{c}}{{Total ({data['SUCCESS']:,} sites)}} \\
        \midrule
        & \textbf{{usage}} & \textbf{{syn. diff.}} & \textbf{{sem. diff.}} \\
{header_lines}
        \midrule
{any_header_lines}
    \end{{tabular}}
    \caption{{Differences between live data and archival data}}
    \label{{tab:live-archive::headers}}
\end{{table}}""")


def build_js_table(input_path: Path) -> None:
    with open(input_path) as file:
        data = defaultdict(int, json.load(file))

    lines = []
    inclusions = data["INCLUDES_SCRIPTS"]
    lines.append('\t\t' + fr"Sites including scripts & {inclusions:,} & \\")
    for granularity in 'URLs', 'hosts', 'sites':
        diff = data[f"DIFFERENT_{granularity.upper()}"]
        line = fr"\quad\emph{{– different {granularity}}} &  & {diff:,} ({diff / inclusions * 100:.2f}\%) \\"
        lines.append(f"\t\t{line}")
    lines = '\n'.join(lines)

    usage_trackers = data['INCLUDES_TRACKERS']
    diff = data['DIFFERENT_TRACKERS']
    trackers = fr"Sites including trackers & {usage_trackers:,} & {diff:,} ({diff / usage_trackers * 100:.2f}\%) \\"

    print(fr"""
\begin{{table}}
    \centering
    \begin{{tabular}}{{l|cc}}
        & \multicolumn{{2}}{{c}}{{Total ({data['SUCCESS']:,} sites)}} \\
        \midrule
        & \textbf{{Count}} & \textbf{{Disagreement}} \\
{lines}
        \midrule
        {trackers}
    \end{{tabular}}
    \caption{{Differences between live and archival data (security header configurations)}}
    \label{{tab:live-archive::js}}
\end{{table}}""")


def main():
    build_headers_table(join_with_json_path(f"DISAGREEMENT-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"))
    build_js_table(join_with_json_path(f"DISAGREEMENT-JS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"))


if __name__ == '__main__':
    main()
