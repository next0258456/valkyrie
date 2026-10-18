"""Valkyrie Command Line Interface."""

import argparse
import sys
from valkyrie.engine import execute, run, to_sql
from valkyrie.exceptions import ContractBreachException


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="valkyrie",
        description="Valkyrie Data Contract Enforcement Engine",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # Command: run
    run_parser = subparsers.add_parser("run", help="Run contract validation on an SQL query")
    run_parser.add_argument("--sql", required=True, help="Input SQL query to validate")
    run_parser.add_argument("--contract", required=True, help="Path to contract YAML file")
    run_parser.add_argument("--write-clean", required=False, help="Target Parquet path for clean data")
    run_parser.add_argument("--write-dlq", required=False, help="Target Parquet path for DLQ data")

    # Command: to-sql
    sql_parser = subparsers.add_parser("to-sql", help="Compile contract to a DuckDB SQL query string")
    sql_parser.add_argument("--contract", required=True, help="Path to contract YAML file")
    sql_parser.add_argument("--sql", required=True, help="Input SQL query")

    return parser


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "run":
        try:
            if args.write_clean or args.write_dlq:
                result = execute(
                    sql=args.sql,
                    contract=args.contract,
                    write_clean=args.write_clean,
                    write_dlq=args.write_dlq,
                )
            else:
                result = run(sql=args.sql, contract=args.contract, raise_on_breach=True)

            metrics = result.metrics
            clean_pct = (metrics["clean_rows"] / metrics["total_rows"]) if metrics["total_rows"] > 0 else 1.0
            dlq_pct = (metrics["error_rows"] / metrics["total_rows"]) if metrics["total_rows"] > 0 else 0.0

            report = (
                f"\n{'=' * 80}\n"
                f"🛡️ VALKYRIE VALIDATION REPORT\n"
                f"Dataset      : {metrics['dataset']}\n"
                f"Owner        : {metrics['owner']}\n"
                f"Status       : PASSED ✅\n"
                f"Total Rows   : {metrics['total_rows']:,}\n"
                f"Clean Rows   : {metrics['clean_rows']:,} ({clean_pct:.1%})\n"
                f"DLQ Rows     : {metrics['error_rows']:,} ({dlq_pct:.1%})\n"
                f"Error Rate   : {metrics['error_rate']:.2%} (Threshold: {metrics['max_error_rate']:.2%})\n"
                f"{'=' * 80}\n"
            )
            print(report)
            sys.exit(0)

        except ContractBreachException as e:
            print(e, file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error executing contract: {e}", file=sys.stderr)
            sys.exit(2)

    elif args.command == "to-sql":
        try:
            sql_out = to_sql(contract=args.contract, source_sql=args.sql)
            print(sql_out)
            sys.exit(0)
        except Exception as e:
            print(f"Error compiling SQL: {e}", file=sys.stderr)
            sys.exit(2)


if __name__ == "__main__":
    main()
