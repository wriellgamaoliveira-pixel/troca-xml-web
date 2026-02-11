from flask import Flask, render_template, request, jsonify, send_file, session, url_for
import os
import uuid
import json
from datetime import datetime
import zipfile
import io
import pandas as pd

from lxml import etree

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-troca-xml-web")

# Aumente se seus ZIPs forem muito grandes (Render pode impor limites próprios)
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024 * 1024  # 1GB

TEMP_DIR = "temp"
UPLOADS_DIR = "uploads"
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

# =========================================================
# Redis (fallback em memória)
# =========================================================
class FakeRedis:
    def __init__(self):
        self.data = {}
        self.exp = {}

    def setex(self, key, ttl, value):
        self.data[key] = value
        self.exp[key] = datetime.now().timestamp() + ttl

    def get(self, key):
        if key in self.data and datetime.now().timestamp() < self.exp.get(key, 0):
            return self.data[key]
        if key in self.data:
            self.data.pop(key, None)
            self.exp.pop(key, None)
        return None

    def ttl(self, key):
        if key not in self.exp:
            return 0
        return max(0, int(self.exp[key] - datetime.now().timestamp()))


def get_redis():
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        return FakeRedis()
    try:
        import redis
        return redis.Redis.from_url(redis_url)
    except Exception:
        return FakeRedis()


redis_store = get_redis()

def redis_setex(key: str, ttl: int, value: bytes):
    return redis_store.setex(key, ttl, value)

def redis_get(key: str):
    return redis_store.get(key)

def redis_ttl(key: str):
    try:
        return redis_store.ttl(key)
    except Exception:
        return 0


# =========================================================
# Helpers formatação BR
# =========================================================
def br_money(v) -> str:
    try:
        return "R$ {:,.2f}".format(float(v or 0)).replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

def br_pct(v) -> str:
    try:
        return "{:,.2f}%".format(float(v or 0)).replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0,00%"

def br_date(iso: str) -> str:
    if not iso:
        return ""
    try:
        s = iso.strip()
        if "T" in s:
            s = s.split("T")[0]
        y, m, d = s.split("-")
        return f"{d}/{m}/{y}"
    except Exception:
        return iso

def safe_float(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return 0.0


# =========================================================
# XML helpers (lxml)
# =========================================================
def parse_xml_bytes(xml_bytes: bytes):
    try:
        return etree.fromstring(xml_bytes)
    except Exception:
        return None

def xfirst(node, xp):
    res = node.xpath(xp)
    return res[0] if res else None

def xtext(node, xp):
    n = xfirst(node, xp)
    if n is None:
        return None
    if isinstance(n, etree._Element):
        return (n.text or "").strip()
    return str(n).strip()

def localname(el):
    try:
        return etree.QName(el).localname
    except Exception:
        return ""


# =========================================================
# Parser NFCom (Nota Única)
# =========================================================
def parse_nfcom_nota(root: etree._Element):
    # suporta nfcomProc e NFCom direto
    inf = xfirst(root, '//*[local-name()="infNFCom"]')
    if inf is None:
        return {"error": "NFCom: tag infNFCom não encontrada."}

    ide = xfirst(inf, './*[local-name()="ide"]')
    emit = xfirst(inf, './*[local-name()="emit"]')
    dest = xfirst(inf, './*[local-name()="dest"]')
    total = xfirst(inf, './*[local-name()="total"]')

    nNF = xtext(ide, './*[local-name()="nNF"]') if ide is not None else None
    serie = xtext(ide, './*[local-name()="serie"]') if ide is not None else None
    cNF = xtext(ide, './*[local-name()="cNF"]') if ide is not None else None
    dhEmi = xtext(ide, './*[local-name()="dhEmi"]') if ide is not None else None

    # emitente
    emit_nome = xtext(emit, './*[local-name()="xNome"]') if emit is not None else None
    emit_cnpj = xtext(emit, './*[local-name()="CNPJ"]') if emit is not None else None
    emit_cpf = xtext(emit, './*[local-name()="CPF"]') if emit is not None else None
    emit_ie = xtext(emit, './*[local-name()="IE"]') if emit is not None else None
    emit_fant = xtext(emit, './*[local-name()="xFant"]') if emit is not None else None

    end_emit = xfirst(emit, './*[local-name()="enderEmit"]') if emit is not None else None
    emit_end = " ".join([p for p in [
        xtext(end_emit, './*[local-name()="xLgr"]') if end_emit is not None else None,
        xtext(end_emit, './*[local-name()="nro"]') if end_emit is not None else None,
        xtext(end_emit, './*[local-name()="xCpl"]') if end_emit is not None else None,
        xtext(end_emit, './*[local-name()="xBairro"]') if end_emit is not None else None,
        xtext(end_emit, './*[local-name()="xMun"]') if end_emit is not None else None,
        xtext(end_emit, './*[local-name()="UF"]') if end_emit is not None else None,
        ("CEP " + xtext(end_emit, './*[local-name()="CEP"]')) if end_emit is not None and xtext(end_emit, './*[local-name()="CEP"]') else None,
    ] if p])

    # destinatário
    dest_nome = xtext(dest, './*[local-name()="xNome"]') if dest is not None else None
    dest_cnpj = xtext(dest, './*[local-name()="CNPJ"]') if dest is not None else None
    dest_cpf = xtext(dest, './*[local-name()="CPF"]') if dest is not None else None
    dest_doc = dest_cnpj or dest_cpf

    end_dest = xfirst(dest, './*[local-name()="enderDest"]') if dest is not None else None
    dest_end = " ".join([p for p in [
        xtext(end_dest, './*[local-name()="xLgr"]') if end_dest is not None else None,
        xtext(end_dest, './*[local-name()="nro"]') if end_dest is not None else None,
        xtext(end_dest, './*[local-name()="xCpl"]') if end_dest is not None else None,
        xtext(end_dest, './*[local-name()="xBairro"]') if end_dest is not None else None,
        xtext(end_dest, './*[local-name()="xMun"]') if end_dest is not None else None,
        xtext(end_dest, './*[local-name()="UF"]') if end_dest is not None else None,
        ("CEP " + xtext(end_dest, './*[local-name()="CEP"]')) if end_dest is not None and xtext(end_dest, './*[local-name()="CEP"]') else None,
    ] if p])

    # Itens
    itens = []
    for det in inf.xpath('./*[local-name()="det"]'):
        prod = xfirst(det, './*[local-name()="prod"]')
        if prod is None:
            continue
        cClass = xtext(prod, './*[local-name()="cClass"]')
        cfop = xtext(prod, './*[local-name()="CFOP"]')
        desc = xtext(prod, './*[local-name()="xProd"]')
        un = xtext(prod, './*[local-name()="uMed"]')
        qtd = xtext(prod, './*[local-name()="qFaturada"]')
        v_unit = xtext(prod, './*[local-name()="vItem"]')
        v_total = xtext(prod, './*[local-name()="vProd"]') or xtext(prod, './*[local-name()="vItem"]')

        item = {
            "cClass": cClass,
            "CFOP": cfop,
            "desc": desc,
            "un": un,
            "qtd": qtd,
            "v_unit": br_money(safe_float(v_unit)),
            "v_total": br_money(safe_float(v_total)),
        }

        imposto = xfirst(det, './*[local-name()="imposto"]')
        # ICMS
        icms_vbc = None
        icms_picms = None
        icms_vicms = None
        if imposto is not None:
            icms = xfirst(imposto, './*[starts-with(local-name(),"ICMS")]')
            if icms is not None:
                icms_vbc = xtext(icms, './/*[local-name()="vBC"]')
                icms_picms = xtext(icms, './/*[local-name()="pICMS"]')
                icms_vicms = xtext(icms, './/*[local-name()="vICMS"]')

            pis = xfirst(imposto, './*[local-name()="PIS"]')
            cofins = xfirst(imposto, './*[local-name()="COFINS"]')
            item["pis_cofins"] = " / ".join([p for p in [
                (xtext(pis, './/*[local-name()="vPIS"]') and br_money(safe_float(xtext(pis, './/*[local-name()="vPIS"]')))) if pis is not None else None,
                (xtext(cofins, './/*[local-name()="vCOFINS"]') and br_money(safe_float(xtext(cofins, './/*[local-name()="vCOFINS"]')))) if cofins is not None else None,
            ] if p]) or "-"

        item["bc_icms"] = br_money(safe_float(icms_vbc)) if icms_vbc else "-"
        item["aliq_icms"] = (str(icms_picms).replace(".", ",") + "%") if icms_picms else "-"
        item["icms"] = br_money(safe_float(icms_vicms)) if icms_vicms else "-"

        itens.append(item)

    # Totais
    totals = {}
    if total is not None:
        totals["vBC"] = br_money(safe_float(xtext(total, './/*[local-name()="ICMSTot"]/*[local-name()="vBC"]')))
        totals["vICMS"] = br_money(safe_float(xtext(total, './/*[local-name()="ICMSTot"]/*[local-name()="vICMS"]')))
        totals["vPIS"] = br_money(safe_float(xtext(total, './*[local-name()="vPIS"]')))
        totals["vCOFINS"] = br_money(safe_float(xtext(total, './*[local-name()="vCOFINS"]')))
        totals["vFUST"] = br_money(safe_float(xtext(total, './*[local-name()="vFUST"]')))
        totals["vFUNTTEL"] = br_money(safe_float(xtext(total, './*[local-name()="vFUNTTEL"]')))
        totals["vNF"] = br_money(safe_float(xtext(total, './*[local-name()="vNF"]')))
        # Retenções (vRetTribTot)
        vret_node = xfirst(total, './*[local-name()="vRetTribTot"]')
        if vret_node is not None:
            vret_pis = safe_float(xtext(vret_node, './*[local-name()="vRetPIS"]'))
            vret_cof = safe_float(xtext(vret_node, './*[local-name()="vRetCofins"]'))
            vret_csll = safe_float(xtext(vret_node, './*[local-name()="vRetCSLL"]'))
            vret_irrf = safe_float(xtext(vret_node, './*[local-name()="vIRRF"]'))
        else:
            vret_pis = vret_cof = vret_csll = vret_irrf = 0.0
        totals["vRetTribTot"] = br_money(vret_pis + vret_cof + vret_csll + vret_irrf)

    return {
        "tipo": "NFCom",
        "nNF": nNF,
        "serie": serie,
        "cNF": cNF,
        "dhEmi": dhEmi,
        "dhEmi_fmt": br_date(dhEmi),
        "emitente": {
            "xNome": emit_nome,
            "xFant": emit_fant,
            "doc": emit_cnpj or emit_cpf,
            "IE": emit_ie,
            "ender": emit_end,
        },
        "destinatario": {
            "xNome": dest_nome,
            "doc": dest_doc,
            "ender": dest_end,
        },
        "itens": itens,
        "totais": totals,
        "retencoes": {
            "pis": br_money(vret_pis),
            "cofins": br_money(vret_cof),
            "csll": br_money(vret_csll),
            "irrf": br_money(vret_irrf),
        }
    }

# =========================================================
# Parser NFe (mínimo para Nota Única + Resumo)
# =========================================================
def parse_nfe_min(root: etree._Element):
    # suporta nfeProc e NFe direto
    inf = xfirst(root, '//*[local-name()="infNFe"]')
    if inf is None:
        return {"error": "NFe: tag infNFe não encontrada."}
    ide = xfirst(inf, './*[local-name()="ide"]')
    emit = xfirst(inf, './*[local-name()="emit"]')
    dest = xfirst(inf, './*[local-name()="dest"]')
    total = xfirst(inf, './*[local-name()="total"]')

    nNF = xtext(ide, './*[local-name()="nNF"]') if ide is not None else None
    serie = xtext(ide, './*[local-name()="serie"]') if ide is not None else None
    cNF = xtext(ide, './*[local-name()="cNF"]') if ide is not None else None
    dhEmi = xtext(ide, './*[local-name()="dhEmi"]') or xtext(ide, './*[local-name()="dEmi"]')

    emit_nome = xtext(emit, './*[local-name()="xNome"]') if emit is not None else None
    emit_doc = xtext(emit, './*[local-name()="CNPJ"]') or xtext(emit, './*[local-name()="CPF"]')

    dest_nome = xtext(dest, './*[local-name()="xNome"]') if dest is not None else None
    dest_doc = xtext(dest, './*[local-name()="CNPJ"]') or xtext(dest, './*[local-name()="CPF"]')

    vNF = safe_float(xtext(total, './/*[local-name()="ICMSTot"]/*[local-name()="vNF"]')) if total is not None else 0.0

    itens = []
    for det in inf.xpath('./*[local-name()="det"]'):
        prod = xfirst(det, './*[local-name()="prod"]')
        if prod is None:
            continue
        itens.append({
            "cProd": xtext(prod, './*[local-name()="cProd"]'),
            "xProd": xtext(prod, './*[local-name()="xProd"]'),
            "CFOP": xtext(prod, './*[local-name()="CFOP"]'),
            "vProd": safe_float(xtext(prod, './*[local-name()="vProd"]')),
        })

    # retenções (quando existirem)
    retTrib = xfirst(inf, './/*[local-name()="retTrib"]')
    ret = {
        "pis": br_money(safe_float(xtext(retTrib, './*[local-name()="vRetPIS"]'))) if retTrib is not None else "R$ 0,00",
        "cofins": br_money(safe_float(xtext(retTrib, './*[local-name()="vRetCOFINS"]'))) if retTrib is not None else "R$ 0,00",
        "csll": br_money(safe_float(xtext(retTrib, './*[local-name()="vRetCSLL"]'))) if retTrib is not None else "R$ 0,00",
        "irrf": br_money(safe_float(xtext(retTrib, './*[local-name()="vIRRF"]'))) if retTrib is not None else "R$ 0,00",
    }

    return {
        "tipo": "NFe",
        "nNF": nNF,
        "serie": serie,
        "cNF": cNF,
        "dhEmi": dhEmi,
        "dhEmi_fmt": br_date(dhEmi),
        "emitente": {"xNome": emit_nome, "doc": emit_doc},
        "destinatario": {"xNome": dest_nome, "doc": dest_doc},
        "itens": itens,
        "totais": {"vNF": br_money(vNF)},
        "retencoes": ret,
    }

def parse_any(xml_bytes: bytes):
    root = parse_xml_bytes(xml_bytes)
    if root is None:
        return {"error": "XML inválido."}
    # detect nfcom
    if xfirst(root, '//*[local-name()="infNFCom"]') is not None:
        return parse_nfcom_nota(root)
    if xfirst(root, '//*[local-name()="infNFe"]') is not None:
        return parse_nfe_min(root)
    return {"error": "Tipo XML não suportado (apenas NFCom/NFe)."}


# =========================================================
# Resumo (ZIP grande) – agregação com limites de detalhe
# =========================================================
MAX_NOTAS_DETALHE_POR_GRUPO = 80  # evita JSON gigante
MAX_NOTAS_DETALHE_IMPOSTO = 200

LARGE_ZIP_THRESHOLD = 500

def norm_cclass(s):
    if s is None:
        return ""
    s = str(s).strip()
    # mantém zeros, mas normaliza também versão sem zeros para comparação
    return s

def cclass_match_key(s):
    s = norm_cclass(s)
    return s.lstrip("0") or "0"

def process_zip_resumo(zip_path: str):
    total_xml = 0
    total_ok = 0
    total_falhas = 0
    primeiro_erro = None

    emitente_nome = None
    emitente_doc = None
    total_geral = 0.0

    # agrupadores
    cclass_map = {}  # cClass -> {desc, qtd_itens, v_total, cfops{cfop:{v_total, notas[]}}}
    item_map = {}    # (cProd,xProd,cClass) -> { ... notas[] }
    impostos_map = {}  # tipo -> {v_total, qtd_notas, notas[]}

    with zipfile.ZipFile(zip_path, "r") as z:
        names = [n for n in z.namelist() if n.lower().endswith(".xml")]
        total_xml = len(names)

        # Para ZIPs grandes, evitamos salvar listas de notas no Redis (fica pesado e pode falhar).
        store_notes = total_xml <= 500
        # Também reduzimos o limite de detalhes por grupo automaticamente
        # (os totais continuam completos).

        for name in names:
            try:
                xml_bytes = z.read(name)
                d = parse_any(xml_bytes)
                if "error" in d:
                    total_falhas += 1
                    if not primeiro_erro:
                        primeiro_erro = f"{name}: {d['error']}"
                    continue

                total_ok += 1

                if not emitente_nome:
                    emitente_nome = (d.get("emitente") or {}).get("xNome")
                    emitente_doc = (d.get("emitente") or {}).get("doc") or (d.get("emitente") or {}).get("CNPJ")

                # total nota
                if d.get("tipo") == "NFCom":
                    vNF = safe_float((d.get("totais") or {}).get("vNF", "").replace("R$", "").replace(".", "").replace(",", "."))
                else:
                    vNF = safe_float((d.get("totais") or {}).get("vNF", "").replace("R$", "").replace(".", "").replace(",", "."))
                total_geral += vNF

                # info base nota
                nota_info = {
                    "nNF": d.get("nNF"),
                    "cNF": d.get("cNF"),
                    "xNome": (d.get("emitente") or {}).get("xNome"),
                    "xContato": (d.get("destinatario") or {}).get("xNome"),
                    "dhEmi_fmt": d.get("dhEmi_fmt"),
                }

                # itens
                for it in d.get("itens") or []:
                    # NFCom
                    cClass = it.get("cClass") or it.get("cClass".lower()) or it.get("cClass".upper())
                    cfop = it.get("CFOP") or it.get("CFOP".lower())
                    desc = it.get("desc") or it.get("xProd") or it.get("xProd".lower())
                    cProd = it.get("cProd")
                    xProd = it.get("xProd") or desc

                    # valores
                    v_total = 0.0
                    if d.get("tipo") == "NFCom":
                        # já vem formatado no parse_nfcom_nota, então tenta reparse
                        v_total = safe_float(str(it.get("v_total") or "").replace("R$", "").replace(".", "").replace(",", "."))
                        if v_total == 0:
                            # fallback
                            v_total = safe_float(it.get("vProd") or 0)
                    else:
                        v_total = safe_float(it.get("vProd") or 0)

                    if not cClass:
                        continue

                    # cClass table
                    g = cclass_map.get(cClass)
                    if not g:
                        g = {"cClass": cClass, "desc": (desc or ""), "qtd_itens": 0, "v_total": 0.0, "cfops": {}}
                        cclass_map[cClass] = g
                    g["qtd_itens"] += 1
                    g["v_total"] += v_total

                    if cfop:
                        cf = g["cfops"].get(cfop)
                        if not cf:
                            cf = {"cfop": cfop, "v_total": 0.0, "notas": []}
                            g["cfops"][cfop] = cf
                        cf["v_total"] += v_total
                        if store_notes and len(cf["notas"]) < MAX_NOTAS_DETALHE_POR_GRUPO:
                            cf["notas"].append({**nota_info, "vProd_br": br_money(v_total)})

                    # itens table (agrega)
                    if cProd or xProd:
                        k = (str(cProd or ""), str(xProd or ""), str(cClass))
                        ig = item_map.get(k)
                        if not ig:
                            ig = {"item": str(cProd or ""), "desc": str(xProd or ""), "cClass": str(cClass), "qtd_itens": 0, "v_total": 0.0, "notas": []}
                            item_map[k] = ig
                        ig["qtd_itens"] += 1
                        ig["v_total"] += v_total
                        if store_notes and len(ig["notas"]) < MAX_NOTAS_DETALHE_POR_GRUPO:
                            ig["notas"].append({**nota_info, "vProd_br": br_money(v_total)})

                # retenções/impostos (NFe)
                ret = d.get("retencoes") or {}
                # soma por tipo somente se existir valor > 0
                for tipo_key, tipo_label in [("pis", "PIS Retido"), ("cofins", "COFINS Ret."), ("csll", "CSLL Ret."), ("irrf", "IRRF Ret.")]:
                    val_br = ret.get(tipo_key)
                    val = safe_float(str(val_br or "").replace("R$", "").replace(".", "").replace(",", "."))
                    if val <= 0:
                        continue
                    m = impostos_map.get(tipo_label)
                    if not m:
                        m = {"tipo": tipo_label, "qtd_notas": 0, "v_total": 0.0, "notas": []}
                        impostos_map[tipo_label] = m
                    m["qtd_notas"] += 1
                    m["v_total"] += val
                    if store_notes and len(m["notas"]) < MAX_NOTAS_DETALHE_IMPOSTO:
                        m["notas"].append({
                            "nNF": nota_info["nNF"],
                            "cNF": nota_info["cNF"],
                            "emitente": nota_info["xNome"],
                            "destinatario": nota_info["xContato"],
                            "emissao": nota_info["dhEmi_fmt"],
                            "pis_ret": ret.get("pis", "R$ 0,00"),
                            "cofins_ret": ret.get("cofins", "R$ 0,00"),
                            "csll_ret": ret.get("csll", "R$ 0,00"),
                            "irrf_ret": ret.get("irrf", "R$ 0,00"),
                            "total_retido": br_money(
                                safe_float(ret.get("pis","0").replace("R$","").replace(".","").replace(",",".")) +
                                safe_float(ret.get("cofins","0").replace("R$","").replace(".","").replace(",",".")) +
                                safe_float(ret.get("csll","0").replace("R$","").replace(".","").replace(",",".")) +
                                safe_float(ret.get("irrf","0").replace("R$","").replace(".","").replace(",",".")) 
                            )
                        })

            except Exception as e:
                total_falhas += 1
                if not primeiro_erro:
                    primeiro_erro = f"{name}: {str(e)}"

    # monta listas + percentuais
    linhas = []
    for cClass, g in cclass_map.items():
        linhas.append({
            "cClass": cClass,
            "desc": g.get("desc",""),
            "qtd_itens": g["qtd_itens"],
            "v_total": round(g["v_total"], 2),
            "v_total_br": br_money(g["v_total"]),
            "pct": 0.0,  # depois
            "pct_br": "",
            "cfops": [
                {
                    "cfop": cfop,
                    "v_total": round(v["v_total"],2),
                    "v_total_br": br_money(v["v_total"]),
                    "notas": v["notas"]
                }
                for cfop, v in g["cfops"].items()
            ]
        })

    # Limites de linhas para evitar JSON gigantes em ZIPs muito grandes
    MAX_CCLASS_ROWS = 5000
    MAX_ITEM_ROWS = 5000
    # Se houver mais linhas que o limite, mantém as maiores (por valor)
    if len(linhas) > MAX_CCLASS_ROWS:
        linhas = sorted(linhas, key=lambda x: x["v_total"], reverse=True)[:MAX_CCLASS_ROWS]

    # ordena cfops internamente
    for l in linhas:
        l["cfops"].sort(key=lambda x: x["v_total"], reverse=True)

    total_geral_itens = sum(l["v_total"] for l in linhas) or 1.0
    for l in linhas:
        pct = (l["v_total"] / total_geral_itens) * 100.0
        l["pct"] = round(pct, 2)
        l["pct_br"] = br_pct(pct)

    # labels (top 12)
    top = sorted(linhas, key=lambda x: x["v_total"], reverse=True)[:12]
    labels = [x["cClass"] for x in top]
    valores = [x["v_total"] for x in top]

    itens_linhas = []
    for k, ig in item_map.items():
        itens_linhas.append({
            "item": ig["item"],
            "desc": ig["desc"],
            "cClass": ig["cClass"],
            "qtd_itens": ig["qtd_itens"],
            "v_total": round(ig["v_total"], 2),
            "v_total_br": br_money(ig["v_total"]),
            "pct": 0.0,
            "pct_br": "",
            "notas": ig["notas"],
        })
    total_itens = sum(i["v_total"] for i in itens_linhas) or 1.0
    for i in itens_linhas:
        pct = (i["v_total"] / total_itens) * 100.0
        i["pct"] = round(pct, 2)
        i["pct_br"] = br_pct(pct)

    # Limita tabela de itens para evitar payload muito grande
    if len(itens_linhas) > MAX_ITEM_ROWS:
        itens_linhas = sorted(itens_linhas, key=lambda x: x["v_total"], reverse=True)[:MAX_ITEM_ROWS]

    impostos_linhas = []
    total_impostos = 0.0
    for tipo, m in impostos_map.items():
        total_impostos += m["v_total"]
    for tipo, m in impostos_map.items():
        pct = (m["v_total"] / (total_impostos or 1.0)) * 100.0
        impostos_linhas.append({
            "tipo": m["tipo"],
            "qtd_notas": m["qtd_notas"],
            "v_total": round(m["v_total"],2),
            "v_total_br": br_money(m["v_total"]),
            "pct": round(pct,2),
            "pct_br": br_pct(pct),
            "notas": m["notas"],
        })
    impostos_linhas.sort(key=lambda x: x["v_total"], reverse=True)

    # Choices para aba Lote (cClass e CFOP encontrados no resumo)
    cclass_set = set()
    cfop_set = set()
    pairs = []
    for l in linhas:
        c = l.get("cClass")
        if c: cclass_set.add(str(c))
        for cf in (l.get("cfops") or []):
            f = cf.get("cfop")
            if f: cfop_set.add(str(f))
            if c and f:
                pairs.append({"cClass": str(c), "CFOP": str(f)})
    # ordena e remove duplicados de pares
    seen=set()
    uniq_pairs=[]
    for p in pairs:
        k=(p["cClass"], p["CFOP"])
        if k in seen: continue
        seen.add(k)
        uniq_pairs.append(p)
    cclass_list = sorted(cclass_set)
    cfop_list = sorted(cfop_set)

    return {
        "emitente_nome": emitente_nome or "",
        "emitente_cnpj": emitente_doc or "",
        "total_arquivos": total_ok,
        "total_geral": round(total_geral, 2),
        "total_geral_br": br_money(total_geral),
        "total_impostos": round(total_impostos,2),
        "total_impostos_br": br_money(total_impostos),
        "labels": labels,
        "valores": valores,
        "linhas": linhas,
        "itens_linhas": itens_linhas,
        "impostos_linhas": impostos_linhas,
        "debug": {"total_xml": total_xml, "total_ok": total_ok, "total_falhas": total_falhas, "primeiro_erro": primeiro_erro},
        "choices": {"cclass": cclass_list, "cfop": cfop_list, "pairs": uniq_pairs},
        "limits": {
            "max_notas_por_grupo": MAX_NOTAS_DETALHE_POR_GRUPO,
            "max_notas_imposto": MAX_NOTAS_DETALHE_IMPOSTO
        }
    }


# =========================================================
# Páginas
# =========================================================
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/sessao")
def sessao_page():
    return render_template("sessao.html")

@app.route("/nota")
def nota_page():
    return render_template("nota.html")

@app.route("/lote")
def lote_page():
    return render_template("lote.html")

@app.route("/resumo")
def resumo_page():
    return render_template("resumo.html")

@app.route("/resumo/resultado")
def resumo_resultado_page():
    session_id = session.get("resumo_session_id") or request.args.get("session_id")
    data = None
    if session_id:
        raw = redis_get(f"resumo:{session_id}")
        if raw:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            try:
                data = json.loads(raw)
            except Exception:
                data = None

    if not data:
        data = {
            "emitente_nome": "",
            "emitente_cnpj": "",
            "total_arquivos": 0,
            "total_geral_br": "R$ 0,00",
            "labels": [],
            "valores": [],
            "linhas": [],
            "itens_linhas": [],
            "impostos_linhas": [],
            "debug": {"total_xml": 0, "total_ok": 0, "total_falhas": 0, "primeiro_erro": "Nenhum relatório encontrado. Faça o upload novamente."},
        }
    return render_template("resumo_resultado.html", data=data)

@app.route("/csv")
def csv_page():
    return render_template("csv.html")


# =========================================================
# API - Resumo
# =========================================================
@app.route("/api/resumo/upload", methods=["POST"])
def api_resumo_upload():
    try:
        if "file" not in request.files:
            return jsonify({"success": False, "error": "Nenhum arquivo enviado"}), 400

        f = request.files["file"]
        if not f.filename:
            return jsonify({"success": False, "error": "Nome de arquivo vazio"}), 400
        if not f.filename.lower().endswith(".zip"):
            return jsonify({"success": False, "error": "Envie um arquivo .zip"}), 400

        session_id = str(uuid.uuid4())
        zip_path = os.path.join(UPLOADS_DIR, f"{session_id}.zip")
        f.save(zip_path)

        # PROCESSA DE VERDADE
        data = process_zip_resumo(zip_path)

        # IMPORTANTÍSSIMO: NÃO GUARDA JSON GRANDE NO COOKIE (session do Flask é client-side)
        session["resumo_session_id"] = session_id
        redis_setex(f"resumo:{session_id}", 6 * 3600, json.dumps(data, ensure_ascii=False).encode("utf-8"))

        return jsonify({"success": True, "session_id": session_id, "redirect": url_for("resumo_resultado_page")})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================================================
# API - Resumo (choices p/ Lote)
# =========================================================
@app.route('/api/resumo/choices')
def api_resumo_choices():
    try:
        session_id = request.args.get('session_id') or session.get('resumo_session_id')
        if not session_id:
            return jsonify({'success': False, 'error': 'session_id não encontrado'}), 400
        raw = redis_get(f'resumo:{session_id}')
        if not raw:
            return jsonify({'success': False, 'error': 'Resumo não encontrado (faça o upload novamente)'}), 404
        if isinstance(raw, bytes):
            raw = raw.decode('utf-8', errors='replace')
        data = __import__('json').loads(raw)
        choices = (data or {}).get('choices') or {'cclass': [], 'cfop': [], 'pairs': []}
        return jsonify({'success': True, 'session_id': session_id, 'choices': choices})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =========================================================
# API - Nota única (retorna JSON para o front montar layout)
# =========================================================
@app.route("/api/nota/visualizar", methods=["POST"])
def api_nota_visualizar():
    try:
        if "xml_nota" not in request.files:
            return jsonify({"success": False, "error": "Envie o arquivo no campo xml_nota"}), 400
        f = request.files["xml_nota"]
        xml_bytes = f.read()
        d = parse_any(xml_bytes)
        if "error" in d:
            return jsonify({"success": False, "error": d["error"]}), 400
        return jsonify({"success": True, "data": d})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================================================
# API - Sessão (simples)
# =========================================================
@app.route("/api/sessao/criar", methods=["POST"])
def api_sessao_criar():
    session_id = str(uuid.uuid4())
    session["current_session"] = session_id

    sessao_data = {
        "id": session_id,
        "criado_em": datetime.now().isoformat(),
        "status": "ativa",
        "chunks_recebidos": 0,
    }

    redis_setex(f"session:{session_id}", 14400, json.dumps(sessao_data).encode("utf-8"))
    return jsonify({"success": True, "session_id": session_id, "ttl": 14400})

@app.route("/api/sessao/status")
def api_sessao_status():
    session_id = request.args.get("session_id")
    if not session_id:
        return jsonify({"success": False, "error": "session_id é obrigatório"}), 400

    raw = redis_get(f"session:{session_id}")
    if not raw:
        return jsonify({"success": True, "status": "nao_encontrada", "ttl_restante": 0})

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    data = json.loads(raw)
    ttl = redis_ttl(f"session:{session_id}")

    return jsonify(
        {
            "success": True,
            "status": data.get("status", "ativa"),
            "chunks_recebidos": data.get("chunks_recebidos", 0),
            "ttl_restante": ttl,
            "criado_em": data.get("criado_em"),
        }
    )


# =========================================================
# API - Lote (troca/inclusão CFOP por cClass)
# =========================================================
def parse_rules(text: str):
    rules = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(";")]
        if len(parts) >= 2:
            cclass, cfop = parts[0], parts[1]
            if cclass and cfop:
                rules[cclass_match_key(cclass)] = cfop
    return rules

def apply_cfop_rules_nfcom(xml_bytes: bytes, rules: dict):
    root = parse_xml_bytes(xml_bytes)
    if root is None:
        return None

    inf = xfirst(root, '//*[local-name()="infNFCom"]')
    if inf is None:
        return None

    changed = False
    for prod in inf.xpath('.//*[local-name()="det"]/*[local-name()="prod"]'):
        cClass = xtext(prod, './*[local-name()="cClass"]')
        if not cClass:
            continue
        key = cclass_match_key(cClass)
        new_cfop = rules.get(key)
        if not new_cfop:
            continue

        cfop_el = xfirst(prod, './*[local-name()="CFOP"]')
        if cfop_el is not None:
            if (cfop_el.text or "").strip() != new_cfop:
                cfop_el.text = new_cfop
                changed = True
        else:
            # insere CFOP logo após cClass
            cclass_el = xfirst(prod, './*[local-name()="cClass"]')
            if cclass_el is not None:
                new_el = etree.Element(cclass_el.tag.replace("cClass", "CFOP"))
                # OBS: manter namespace do pai
                # se tag tiver namespace, usa mesmo namespace:
                try:
                    q = etree.QName(cclass_el)
                    ns = q.namespace
                    if ns:
                        new_el = etree.Element(f"{{{ns}}}CFOP")
                except Exception:
                    pass
                new_el.text = new_cfop
                # inserir após cClass
                idx = list(prod).index(cclass_el)
                prod.insert(idx + 1, new_el)
                changed = True

    if not changed:
        return xml_bytes

    return etree.tostring(root, xml_declaration=True, encoding="utf-8", pretty_print=False)

@app.route("/api/lote/processar", methods=["POST"])
def api_lote_processar():
    try:
        if "zip_xmls" not in request.files:
            return jsonify({"success": False, "error": "Envie o ZIP no campo zip_xmls"}), 400

        zf = request.files["zip_xmls"]
        if not zf.filename.lower().endswith(".zip"):
            return jsonify({"success": False, "error": "Envie um arquivo .zip"}), 400

        session_id = request.form.get("session_id") or str(uuid.uuid4())
        regras_texto = request.form.get("regras_cclass_cfop", "") or ""
        rules = parse_rules(regras_texto)

        zip_bytes = zf.read()
        inbuf = io.BytesIO(zip_bytes)
        outbuf = io.BytesIO()

        total_xml = 0
        alterados = 0

        with zipfile.ZipFile(inbuf, "r") as zin, zipfile.ZipFile(outbuf, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for name in zin.namelist():
                data = zin.read(name)
                if not name.lower().endswith(".xml"):
                    # copia arquivos não-XML
                    zout.writestr(name, data)
                    continue

                total_xml += 1
                # aplica regra apenas em NFCom (pode estender para NFe se precisar)
                updated = apply_cfop_rules_nfcom(data, rules)
                if updated is None:
                    zout.writestr(name, data)
                else:
                    if updated != data:
                        alterados += 1
                    zout.writestr(name, updated)

            zout.writestr("relatorio_lote.txt", f"Total XML: {total_xml}\nAlterados: {alterados}\nRegras: {len(rules)}\n")

        outbuf.seek(0)
        zip_path = os.path.join(TEMP_DIR, f"{session_id}_processado.zip")
        with open(zip_path, "wb") as f:
            f.write(outbuf.getvalue())

        return jsonify({"success": True, "session_id": session_id, "download_url": f"/download/{session_id}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================================================
# API - CSV
# =========================================================
@app.route("/api/csv/gerar", methods=["POST"])
def api_csv_gerar():
    try:
        if "zip_xmls" not in request.files:
            return jsonify({"success": False, "error": "Envie o ZIP no campo zip_xmls"}), 400

        zf = request.files["zip_xmls"]
        if not zf.filename.lower().endswith(".zip"):
            return jsonify({"success": False, "error": "Envie um arquivo .zip"}), 400

        campos = (request.form.get("campos", "") or "").strip()
        requested = [c.strip() for c in campos.split(";") if c.strip()] if campos else []

        rows = []
        with zipfile.ZipFile(io.BytesIO(zf.read()), "r") as zip_in:
            for name in zip_in.namelist():
                if not name.lower().endswith(".xml"):
                    continue
                d = parse_any(zip_in.read(name))
                if "error" in d:
                    continue

                row = {
                    "arquivo": name,
                    "tipo": d.get("tipo"),
                    "nNF": d.get("nNF"),
                    "serie": d.get("serie"),
                    "cNF": d.get("cNF"),
                    "dhEmi": d.get("dhEmi"),
                    "emitente_nome": (d.get("emitente") or {}).get("xNome"),
                    "emitente_doc": (d.get("emitente") or {}).get("doc") or (d.get("emitente") or {}).get("CNPJ"),
                    "dest_nome": (d.get("destinatario") or {}).get("xNome"),
                    "dest_doc": (d.get("destinatario") or {}).get("doc"),
                }
                if requested:
                    row = {k: row.get(k) for k in requested if k in row}
                    if "arquivo" not in row:
                        row["arquivo"] = name
                rows.append(row)

        if not rows:
            return jsonify({"success": False, "error": "Nenhum XML válido encontrado no ZIP"}), 400

        df = pd.DataFrame(rows)
        csv_bytes = df.to_csv(index=False).encode("utf-8")

        return send_file(io.BytesIO(csv_bytes), as_attachment=True, download_name="export.csv", mimetype="text/csv")
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================================================
# Download ZIP lote
# =========================================================
@app.route("/download/<session_id>")
def download_file(session_id):
    zip_path = os.path.join(TEMP_DIR, f"{session_id}_processado.zip")
    if os.path.exists(zip_path):
        return send_file(zip_path, as_attachment=True, download_name=f"processado_{session_id}.zip")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as z:
        z.writestr("exemplo.txt", "Arquivo processado (demo)")
    zip_buffer.seek(0)
    return send_file(zip_buffer, as_attachment=True, download_name=f"processado_{session_id}.zip", mimetype="application/zip")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
