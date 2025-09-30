import requests
import csv
from typing import List, Dict

def safe_get(item: dict, keys: List[str]):
    """Retorna o primeiro valor não-nulo entre as chaves listadas."""
    for k in keys:
        if k in item and item[k] not in (None, "", []):
            return item[k]
    return None

def normalize_symbol(sym: str, exchange: str = None, force_sa=True):
    """
    Normaliza symbol: trim, uppercase.
    Se for BR (exchange indica B3/BVMF) e não contém '.', opcionalmente adiciona '.SA'.
    """
    if not sym:
        return None
    s = str(sym).strip().upper()
    # já tem sufixo (ex.: .SA, .SAO, .NS etc) -> não altera
    if "." in s:
        return s
    if not force_sa:
        return s
    # heurística simples para adicionar .SA: quando exchange sinaliza B3/BVMF/BM
    if exchange:
        e = str(exchange).upper()
        if any(x in e for x in ["BVMF", "B3", "BM&F", "BMFBOVESPA", "BOVESPA", "BMF"]):
            return f"{s}.SA"
    # Se não souber a exchange, podemos ainda adicionar .SA para tentar (opcional)
    return f"{s}.SA"

def listar_ativos_b3_robusto_brapi(token: str = None, save_csv: str = "ativos_b3_completos.csv"):
    """
    Consulta BRAPI (ou outro endpoint similar) e salva um CSV com símbolos normalizados.
    Tenta encontrar campos alternativos quando 'symbol' estiver ausente.
    """
    url = "https://brapi.dev/api/quote/list"
    params = {"page": 1, "limit": 500}
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    todos = []
    skipped = []
    while True:
        resp = requests.get(url, params=params, headers=headers, timeout=20)
        if resp.status_code != 200:
            raise RuntimeError(f"Erro {resp.status_code}: {resp.text}")
        data = resp.json()

        # O payload da BRAPI pode ter a chave 'stocks' ou similar
        batch = data.get("stocks") or data.get("results") or data.get("data") or data.get("items") or []
        if not isinstance(batch, list):
            break

        for item in batch:
            # tenta extrair symbol a partir de várias chaves possíveis
            raw_symbol = safe_get(item, ["symbol", "stock", "ticker", "code", "asset", "id"])
            # tenta nomes alternativos
            name = safe_get(item, ["name", "shortName", "longName", "companyName", "assetName"])
            at_type = safe_get(item, ["type", "assetType", "instrumentType", "kind"])
            exchange = safe_get(item, ["exchange", "stockExchange", "market", "exchangeName"])

            norm_sym = normalize_symbol(raw_symbol, exchange, force_sa=True)
            if not raw_symbol and not norm_sym:
                # nenhum símbolo identificável: guarda para inspeção
                skipped.append({"item": item})
                continue

            todos.append({
                "symbol_raw": raw_symbol,
                "symbol": norm_sym,
                "name": name,
                "type": at_type,
                "exchange": exchange
            })

        # paginação: BRAPI devolve currentPage / totalPages em alguns schemas
        current = data.get("currentPage") or data.get("page") or params["page"]
        total_pages = data.get("totalPages") or data.get("total_pages") or None
        # parar quando batch menor que limit ou quando não houver total_pages
        if total_pages:
            if current >= total_pages:
                break
            params["page"] = current + 1
        else:
            # fallback: se retornou menos que o limite, provavelmente é a última página
            if len(batch) < params["limit"]:
                break
            params["page"] += 1

    # remove duplicados pelo symbol
    seen = set()
    unique = []
    for r in todos:
        s = r["symbol"]
        if s and s not in seen:
            seen.add(s)
            unique.append(r)

    # salvar CSV principal
    fieldnames = ["symbol", "symbol_raw", "name", "type", "exchange"]
    with open(save_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in unique:
            writer.writerow({k: (r.get(k) or "") for k in fieldnames})

    # opcional: salvar itens pulados para auditoria
    if skipped:
        import json
        with open("ativos_skipped_debug.json", "w", encoding="utf-8") as f:
            json.dump(skipped, f, ensure_ascii=False, indent=2)

    print(f"Total lidos: {len(todos)} | Únicos com símbolo: {len(unique)} | Pulsados (sem símbolo): {len(skipped)}")
    print(f"CSV salvo em: {save_csv}")
    if skipped:
        print("Alguns itens sem símbolo foram salvos em ativos_skipped_debug.json para inspeção.")

    return unique

# exemplo de uso:
if __name__ == "__main__":
    # token opcional; brapi pode permitir sem token para endpoints públicos
    lista = listar_ativos_b3_robusto_brapi(token=None, save_csv="ativos_b3.csv")