docker compose exec -T db sh -lc '
  PGPASSWORD="$POSTGRES_PASSWORD" \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -p "${PGPORT:-5432}" \
  -v ON_ERROR_STOP=1 -f /sql/main.sql
'
