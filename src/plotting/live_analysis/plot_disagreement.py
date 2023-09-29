import json
from collections import defaultdict
from pathlib import Path

from analysis.live.analyze_disagreement import ARCHIVE_TABLE_NAME
from configs.analysis import SECURITY_MECHANISM_HEADERS
from configs.utils import join_with_json_path
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME


def build_headers_table(input_path: Path) -> None:
    """Build a LaTeX table listing information about the security header configuration disagreement."""
    with open(input_path) as file:
        data = defaultdict(int, json.load(file))

    header_lines = []
    for mechanism, header in SECURITY_MECHANISM_HEADERS.items():
        usage = data[f"USES_{header}"]
        syn_diff = data[f"SYNTAX_DIFFERENCE_{mechanism}"]
        sem_diff = data[f"SEMANTICS_DIFFERENCE_{mechanism}"]
        header_lines.append(
            f"\t\t{mechanism} & {usage:,} & "
            fr"{syn_diff:,} ({syn_diff / usage:.2%}) & "
            fr"{sem_diff:,} ({sem_diff / usage:.2%}) \\"
        )
        for reason in 'ORIGIN_MISMATCH', 'USER_AGENT', 'TITLE_DIFFERENCE', 'NO_INFORMATION':
            syn_diff_reason = data[f"SYNTAX_DIFFERENCE_{mechanism}::{reason}"]
            sem_diff_reason = data[f"SEMANTICS_DIFFERENCE_{mechanism}::{reason}"]
            header_lines.append(
                f"\t\t\quad\emph{{– {reason}}} &  & "
                fr"{syn_diff_reason:,} ({syn_diff_reason / syn_diff:.2%}) & "
                fr"{sem_diff_reason:,} ({sem_diff_reason / sem_diff:.2%}) \\"
            )
    header_lines = '\n'.join(header_lines)

    usage = data['USES_ANY']
    syn_diff = data['SYNTAX_DIFFERENCE']
    sem_diff = data['SEMANTICS_DIFFERENCE']
    any_header_lines = [
        f"\t\t\\textit{{Any header}} & {usage:,} & "
        fr"{syn_diff:,} ({syn_diff / usage:.2%}) & "
        fr"{sem_diff:,} ({sem_diff / usage:.2%}) \\"
    ]

    for reason in 'ORIGIN_MISMATCH', 'USER_AGENT', 'TITLE_DIFFERENCE', 'NO_INFORMATION':
        syn_diff_reason = data[f"SYNTAX_DIFFERENCE::{reason}"]
        sem_diff_reason = data[f"SEMANTICS_DIFFERENCE::{reason}"]
        any_header_lines.append(
            f"\t\t\quad\emph{{– {reason}}} &  & "
            fr"{syn_diff_reason:,} ({syn_diff_reason / syn_diff:.2%}) & "
            fr"{sem_diff_reason:,} ({sem_diff_reason / sem_diff:.2%}) \\"
        )
    any_header_lines = '\n'.join(any_header_lines)

    print(fr"""
\begin{{table}}
    \centering
    \begin{{tabular}}{{l|rrr}}
        & \multicolumn{{3}}{{c}}{{Total ({data['SUCCESS']:,} domains)}} \\
        \midrule
        & \textbf{{deploys}} & \textbf{{syn. diff.}} & \textbf{{sem. diff.}} \\
{header_lines}
        \midrule
{any_header_lines}
    \end{{tabular}}
    \caption{{Differences between live data and archival data}}
    \label{{tab:live-archive::headers}}
\end{{table}}""".replace('%', '\%'))


def build_js_table(input_path: Path) -> None:
    """Build a LaTeX table listing information about the JavaScript dependency disagreement."""
    with open(input_path) as file:
        data = defaultdict(int, json.load(file))

    lines = []
    inclusions = data["INCLUDES_SCRIPTS"]
    lines.append(fr"Domains including third-party JavaScript & {inclusions:,} & \\")
    for granularity in 'scripts', 'hosts', 'sites':
        diff = data[f"DIFFERENT_{granularity.upper()}"]
        lines.append(fr"\quad\emph{{– different {granularity}}} &  & {diff:,} ({diff / inclusions:.2%}) \\")
    lines = '\n\t\t'.join(lines)

    usage_trackers = data['INCLUDES_TRACKERS']
    diff = data['DIFFERENT_TRACKERS']
    title_difference = data['DIFFERENT_TRACKERS::TITLE_DIFFERENCE']
    either_missing = data['DIFFERENT_TRACKERS::EITHER_MISSING']
    trackers = '\n\t\t'.join(
        (fr"Domains including web trackers & {usage_trackers:,} & {diff:,} ({diff / usage_trackers:.2%}) \\",
         fr"\quad\emph{{– attributable to title difference}} &  & {title_difference} ({title_difference / diff:.2%}) \\",
         fr"\quad\emph{{– missing entirely in either collection}} &  & {either_missing} ({either_missing / diff:.2%}) \\",
         ))

    print(fr"""
\begin{{table}}
    \centering
    \begin{{tabular}}{{l|rr}}
        & \multicolumn{{2}}{{c}}{{Total ({data['SUCCESS']:,} domains)}} \\
        \midrule
        & \textbf{{Count}} & \textbf{{Disagreement}} \\
        {lines}
        \midrule
        {trackers}
    \end{{tabular}}
    \caption{{Differences between live and archival data (security header configurations)}}
    \label{{tab:live-archive::js}}
\end{{table}}""".replace('%', '\%'))


def main():
    build_headers_table(join_with_json_path(f"DISAGREEMENT-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"))
    build_js_table(join_with_json_path(f"DISAGREEMENT-JS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"))


if __name__ == '__main__':
    main()
