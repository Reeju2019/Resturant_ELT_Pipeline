"""Verify the project setup is correct."""

import os
import sys
from dotenv import load_dotenv


def check_file(path, description):
    """Check if a file exists."""
    exists = os.path.exists(path)
    status = "✅" if exists else "❌"
    print(f"{status} {description}: {path}")
    return exists


def check_csv_files():
    """Check if all CSV files exist."""
    csv_dir = "data/csv"
    required_files = [
        "raw_customers.csv",
        "raw_orders.csv",
        "raw_items.csv",
        "raw_products.csv",
        "raw_stores.csv",
        "raw_supplies.csv",
    ]

    all_exist = True
    for filename in required_files:
        path = os.path.join(csv_dir, filename)
        exists = check_file(path, "CSV file")
        all_exist = all_exist and exists

    return all_exist


def check_env():
    """Check if .env file exists and has required variables."""
    if not os.path.exists(".env"):
        print("❌ .env file not found")
        return False

    print("✅ .env file exists")

    load_dotenv()

    required_vars = ["CONTAINER_SAS_URL", "DUCKDB_PATH", "CSV_DATA_DIR"]
    all_set = True

    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"  ✅ {var} is set")
        else:
            print(f"  ❌ {var} is not set")
            all_set = False

    return all_set


def main():
    """Run all verification checks."""
    print("🔍 Verifying Restaurant ELT Pipeline Setup")
    print("=" * 60)

    checks = []

    # Check project structure
    print("\n📁 Project Structure:")
    checks.append(check_file("src/repository.py", "Repository"))
    checks.append(check_file("src/resources/warehouse.py", "Warehouse resource"))
    checks.append(check_file("src/resources/azure.py", "Azure resource"))
    checks.append(check_file("requirements.txt", "Requirements"))
    checks.append(check_file("dagster.yaml", "Dagster config"))

    # Check SQL files
    print("\n📝 SQL Files:")
    sql_files = [
        "sql/silver/customers.sql",
        "sql/silver/orders.sql",
        "sql/gold/fact_orders.sql",
        "sql/gold/metrics.sql",
    ]
    for sql_file in sql_files:
        checks.append(check_file(sql_file, "SQL"))

    # Check CSV files
    print("\n📊 CSV Data Files:")
    checks.append(check_csv_files())

    # Check environment
    print("\n🔧 Environment Configuration:")
    checks.append(check_env())

    # Summary
    print("\n" + "=" * 60)
    if all(checks):
        print("✅ All checks passed! Setup is complete.")
        print("\nNext steps:")
        print("  1. Run: python run_pipeline.py")
        print("  2. Or: dagster dev -f src/repository.py")
        return 0

    print("❌ Some checks failed. Please review the output above.")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"\n❌ Error during verification: {error}")
        sys.exit(1)
