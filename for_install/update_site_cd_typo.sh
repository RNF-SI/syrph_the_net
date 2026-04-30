#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <db_name> [db_user] [cd_typo]"
  exit 1
fi

DB_NAME="$1"
DB_USER="${2:-postgres}"
CD_TYPO_INPUT="${3:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SITE_TEMPLATE_JSON="${SCRIPT_DIR}/../site.json.template"
SITE_JSON="${SCRIPT_DIR}/../site.json"

if [[ ! -f "${SITE_TEMPLATE_JSON}" ]]; then
  echo "Erreur: fichier template introuvable: ${SITE_TEMPLATE_JSON}"
  exit 1
fi

if [[ -n "${CD_TYPO_INPUT}" ]]; then
  CD_TYPO="${CD_TYPO_INPUT}"
else
  CD_TYPO="$(psql -X -q -t -A -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT cd_typo FROM ref_habitats.typoref WHERE cd_table = 'TYPO_STN' LIMIT 1;")"
  CD_TYPO="$(echo "${CD_TYPO}" | tr -d '[:space:]')"
fi

if [[ -z "${CD_TYPO}" ]]; then
  echo "Erreur: aucun cd_typo trouve pour cd_table='TYPO_STN'"
  exit 1
fi

python3 - "${SITE_TEMPLATE_JSON}" "${SITE_JSON}" "${CD_TYPO}" <<'PY'
import json
import sys

site_template_path = sys.argv[1]
site_path = sys.argv[2]
cd_typo = int(sys.argv[3])

with open(site_template_path, "r", encoding="utf-8") as f:
    data = json.load(f)

specific = data.get("specific", {})
for field in specific.values():
    if not isinstance(field, dict):
        continue
    if field.get("type_util") != "habitat":
        continue
    params = field.setdefault("params", {})
    params["cd_typo"] = cd_typo
    field["api"] = "habref/habitats/autocomplete"

with open(site_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
    f.write("\n")
PY

echo "site.json mis a jour avec cd_typo=${CD_TYPO}"
