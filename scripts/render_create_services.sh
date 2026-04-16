#!/usr/bin/env bash
set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to run this script." >&2
  exit 1
fi

API_ROOT="https://api.render.com/v1"
RENDER_API_KEY="${RENDER_API_KEY:?Set RENDER_API_KEY before running this script.}"
RENDER_OWNER_ID="${RENDER_OWNER_ID:-}"
RENDER_REPO="${RENDER_REPO:-https://github.com/Giacomoni-Contabilidade/repo-automacao}"
RENDER_BRANCH="${RENDER_BRANCH:-main}"
RENDER_REGION="${RENDER_REGION:-virginia}"
RENDER_PLAN="${RENDER_PLAN:-free}"
RENDER_AUTO_DEPLOY="${RENDER_AUTO_DEPLOY:-yes}"
RENDER_HEALTHCHECK_PATH="${RENDER_HEALTHCHECK_PATH:-/openapi.json}"
RENDER_BUILD_COMMAND="${RENDER_BUILD_COMMAND:-pip install -r requirements.txt}"
RENDER_START_COMMAND="${RENDER_START_COMMAND:-uvicorn main:app --host 0.0.0.0 --port \$PORT}"

if [ -z "$RENDER_OWNER_ID" ]; then
  RENDER_OWNER_ID="$(
    curl -sS \
      -H "Authorization: Bearer $RENDER_API_KEY" \
      "$API_ROOT/owners" | jq -r '.[0].owner.id // empty'
  )"
fi

if [ -z "$RENDER_OWNER_ID" ]; then
  echo "Could not determine a Render owner ID." >&2
  exit 1
fi

SERVICE_SPECS=(
  "bot-rpa|bot-rpa"
  "bot-contrib-parlamentares-dir-nacional|botContribParlamentaresDirNacional"
  "bot-contrib-parlamentares-sp|botContribParlamentaresSP"
  "bot-doacoes-dir-nacional|botDoacoesDirNacional"
  "bot-faturas-nix|botFaturasNIX"
  "bot-fundo-partidario-dir-nacional|botFundoPartidarioDirNacional"
  "bot-proprietarios-aluga-165|botProprietariosAluga165"
  "bot-rpa-dominio-spca-sp|botRpaDominioSpcaSP"
  "bot-rpa-dominio-vlr-bruto|botRpaDominioVlrBruto"
  "bot-rpa-vlr-liq-dir-nacional|botRpaVlrLiqDirNacional"
)

list_services() {
  curl -sS \
    -H "Authorization: Bearer $RENDER_API_KEY" \
    "$API_ROOT/services"
}

existing_services_json="$(list_services)"

for spec in "${SERVICE_SPECS[@]}"; do
  IFS="|" read -r name root_dir <<<"$spec"

  existing_service_json="$(
    jq -c --arg name "$name" '.[] | select(.service.name == $name)' <<<"$existing_services_json" | head -n 1
  )"

  if [ -n "$existing_service_json" ]; then
    printf 'skip    %s  %s\n' \
      "$name" \
      "$(jq -r '.service.serviceDetails.url // .service.dashboardUrl // "already exists"' <<<"$existing_service_json")"
    continue
  fi

  payload="$(
    jq -n \
      --arg name "$name" \
      --arg owner_id "$RENDER_OWNER_ID" \
      --arg repo "$RENDER_REPO" \
      --arg branch "$RENDER_BRANCH" \
      --arg root_dir "$root_dir" \
      --arg auto_deploy "$RENDER_AUTO_DEPLOY" \
      --arg plan "$RENDER_PLAN" \
      --arg region "$RENDER_REGION" \
      --arg healthcheck_path "$RENDER_HEALTHCHECK_PATH" \
      --arg build_command "$RENDER_BUILD_COMMAND" \
      --arg start_command "$RENDER_START_COMMAND" \
      '{
        type: "web_service",
        name: $name,
        ownerId: $owner_id,
        repo: $repo,
        branch: $branch,
        rootDir: $root_dir,
        autoDeploy: $auto_deploy,
        serviceDetails: {
          runtime: "python",
          plan: $plan,
          region: $region,
          healthCheckPath: $healthcheck_path,
          envSpecificDetails: {
            buildCommand: $build_command,
            startCommand: $start_command
          }
        }
      }'
  )"

  response="$(
    curl -sS -X POST \
      -H "Authorization: Bearer $RENDER_API_KEY" \
      -H "Content-Type: application/json" \
      -d "$payload" \
      "$API_ROOT/services"
  )"

  if [ -z "$(jq -r '.service.id // empty' <<<"$response")" ]; then
    echo "$response" >&2
    exit 1
  fi

  printf 'create  %s  %s  %s\n' \
    "$(jq -r '.service.name' <<<"$response")" \
    "$(jq -r '.service.serviceDetails.url' <<<"$response")" \
    "$(jq -r '.deployId' <<<"$response")"

  existing_services_json="$(list_services)"
done
