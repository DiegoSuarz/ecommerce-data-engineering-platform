#!/bin/sh

# ============================================================
# E-Commerce Data Engineering Platform
# CDC MySQL User Bootstrap
# ============================================================

cdc_fail() {
    echo "CDC bootstrap error: $1" >&2
    exit 1
}

[ -n "${MYSQL_ROOT_PASSWORD:-}" ] \
    || cdc_fail "MYSQL_ROOT_PASSWORD is required."

[ -n "${MYSQL_DATABASE:-}" ] \
    || cdc_fail "MYSQL_DATABASE is required."

[ -n "${MYSQL_CDC_USER:-}" ] \
    || cdc_fail "MYSQL_CDC_USER is required."

[ -n "${MYSQL_CDC_PASSWORD:-}" ] \
    || cdc_fail "MYSQL_CDC_PASSWORD is required."

case "$MYSQL_CDC_USER" in
    *[!A-Za-z0-9_]*)
        cdc_fail "MYSQL_CDC_USER contains unsupported characters."
        ;;
esac

case "$MYSQL_DATABASE" in
    *[!A-Za-z0-9_]*)
        cdc_fail "MYSQL_DATABASE contains unsupported characters."
        ;;
esac

cdc_password_escaped=$(
    printf '%s' "$MYSQL_CDC_PASSWORD" \
        | sed -e 's/\\/\\\\/g' -e "s/'/''/g"
)

if MYSQL_PWD="$MYSQL_ROOT_PASSWORD" \
    mysql \
        --protocol=socket \
        --user=root <<SQL
CREATE USER IF NOT EXISTS
    '${MYSQL_CDC_USER}'@'%'
    IDENTIFIED BY '${cdc_password_escaped}';

ALTER USER
    '${MYSQL_CDC_USER}'@'%'
    IDENTIFIED BY '${cdc_password_escaped}';

GRANT SELECT
    ON \`${MYSQL_DATABASE}\`.*
    TO '${MYSQL_CDC_USER}'@'%';

GRANT REPLICATION SLAVE, REPLICATION CLIENT
    ON *.*
    TO '${MYSQL_CDC_USER}'@'%';
SQL
then
    echo "CDC MySQL user configured successfully."
else
    cdc_fail "MySQL privilege configuration failed."
fi