import argparse

"""
python -m <module> -h
python -m <module> regression -h
python -m <module> invariance -h
"""


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run tests against SDF.")
    subparsers = parser.add_subparsers(
        required=True, dest="test_type", title="test-type"
    )

    result_destination_args = {
        "default": ":memory:",
        "metavar": "RESULT_DESTINATION",
        "help": "Path to save the results. If not specified, results will be saved in memory.",
    }

    invariance_parser = subparsers.add_parser("invariance")
    invariance_parser.add_argument("--result-destination", **result_destination_args)

    regression_parser = subparsers.add_parser("regression")
    regression_parser.add_argument("--result-destination", **result_destination_args)
    group = regression_parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--regression-reference",
        metavar="REGRESSION_REFERENCE",
        help="Path to reference results. The current run will be compared against those results.",
    )
    group.add_argument(  # boolean flag, defaults to False
        "--compute-reference",
        action="store_true",
        help="Compute reference results against which subsequent runs will be compared.",
    )

    return parser.parse_args()
