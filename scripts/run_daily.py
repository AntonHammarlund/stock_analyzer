from stock_analyzer.pipeline import run_daily


def main() -> None:
    report = run_daily()
    print("Daily run completed.")
    print(f"Generated at: {report.get('generated_at')}")
    print(f"Top picks: {len(report.get('top_picks', []))}")


if __name__ == "__main__":
    main()
