#!/usr/bin/env bash

set -euo pipefail

# ------------------------------------------------------------
# E-Commerce Data Engineering Platform
# Initial OLTP Data Load
# ------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
SQL_FILE="$PROJECT_ROOT/01-oltp-database/sql/003_load_initial_data.sql"

CONTAINER_NAME="ecommerce-mysql"

EXPECTED_CATEGORIES=5
EXPECTED_COUNTRIES=56
EXPECTED_ORDERS=300000

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: .env file not found."
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

CURRENT_COUNTS="$(
    docker exec \
        -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" \
        "$CONTAINER_NAME" \
        mysql \
        -u root \
        -N \
        -B \
        -e "
SELECT COUNT(*) FROM sales.categories;
SELECT COUNT(*) FROM sales.countries;
SELECT COUNT(*) FROM sales.orders;
"
)"

mapfile -t COUNTS <<< "$CURRENT_COUNTS"

CURRENT_CATEGORIES="${COUNTS[0]}"
CURRENT_COUNTRIES="${COUNTS[1]}"
CURRENT_ORDERS="${COUNTS[2]}"

echo "Current rows:"
echo "  categories: $CURRENT_CATEGORIES"
echo "  countries:  $CURRENT_COUNTRIES"
echo "  orders:     $CURRENT_ORDERS"

if [[ "$CURRENT_CATEGORIES" -eq 0 \
   && "$CURRENT_COUNTRIES" -eq 0 \
   && "$CURRENT_ORDERS" -eq 0 ]]; then

    echo "Loading initial OLTP dataset..."

    docker exec \
        -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" \
        -i "$CONTAINER_NAME" \
        mysql \
        --local-infile=1 \
        -u root \
        < "$SQL_FILE"

    FINAL_COUNTS="$(
        docker exec \
            -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" \
            "$CONTAINER_NAME" \
            mysql \
            -u root \
            -N \
            -B \
            -e "
SELECT COUNT(*) FROM sales.categories;
SELECT COUNT(*) FROM sales.countries;
SELECT COUNT(*) FROM sales.orders;
"
    )"

    mapfile -t FINAL <<< "$FINAL_COUNTS"

    FINAL_CATEGORIES="${FINAL[0]}"
    FINAL_COUNTRIES="${FINAL[1]}"
    FINAL_ORDERS="${FINAL[2]}"

    echo "Rows after initial load:"
    echo "  categories: $FINAL_CATEGORIES"
    echo "  countries:  $FINAL_COUNTRIES"
    echo "  orders:     $FINAL_ORDERS"

    if [[ "$FINAL_CATEGORIES" -ne "$EXPECTED_CATEGORIES" \
       || "$FINAL_COUNTRIES" -ne "$EXPECTED_COUNTRIES" \
       || "$FINAL_ORDERS" -ne "$EXPECTED_ORDERS" ]]; then

        echo "ERROR: Initial OLTP data load validation failed."
        exit 1
    fi

    echo "Initial OLTP data load validated successfully."

elif [[ "$CURRENT_CATEGORIES" -gt 0 \
     && "$CURRENT_COUNTRIES" -gt 0 \
     && "$CURRENT_ORDERS" -gt 0 ]]; then

    echo "Initial data load skipped: OLTP tables already contain data."

else
    echo "ERROR: Partial OLTP data state detected."
    echo "The initial load will not continue automatically."
    exit 1
fi
