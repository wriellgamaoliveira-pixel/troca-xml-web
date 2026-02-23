from flask import Flask, render_template, request, jsonify, send_file, session, url_for, Response
import os
import uuid
import json
import csv
import unicodedata
from collections import defaultdict
from datetime import datetime
import zipfile
import io
import threading
import time

from lxml import etree

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-troca-xml-web")

app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024 * 1024  # 1GB (Render pode limitar antes)

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
        self.exp[key] = time.time() + ttl

    def get(self, key):
        exp = self.exp.get(key)
        if exp is not None and time.time() > exp:
            self.data.pop(key, None)
            self.exp.pop(key, None)
            return None
        return self.data.get(key)

    def ttl(self, key):
        exp = self.exp.get(key)
        if exp is None:
            return 0
        return max(0, int(exp - time.time()))

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

def r_setex(key: str, ttl: int, obj):
    b = obj if isinstance(obj, (bytes, bytearray)) else json.dumps(obj, ensure_ascii=False).encode("utf-8")
    redis_store.setex(key, ttl, b)

def r_get_json(key: str):
    raw = redis_store.get(key)
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(raw)
    except Exception:
        return None

def r_get_bytes(key: str):
    return redis_store.get(key)

# =========================================================
# Helpers BR
# =========================================================
def br_money(v: float) -> str:
    try:
        return "R$ {:,.2f}".format(float(v)).replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

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
        return float(str(x).strip())
    except Exception:
        return 0.0

def validar_integridade(dados):
    total_processadas = int(((dados or {}).get("debug") or {}).get("total_notas_processadas") or 0)
    total_resumo = sum(
        len(cfop.get("notas") or [])
        for linha in (dados or {}).get("linhas") or []
        for cfop in linha.get("cfops") or []
    )

    if total_processadas != total_resumo:
        print("⚠ ERRO: Divergência de notas!")
    else:
        print("✔ Integridade OK")

    return total_processadas, total_resumo

# =========================================================
# XML robust (NFe / NFCom) via local-name
# =========================================================
def xp(node, path):
    return node.xpath(path)

def x1(node, path):
    arr = node.xpath(path)
    if not arr:
        return None
    v = arr[0]
    if isinstance(v, etree._Element):
        return (v.text or "").strip()
    return str(v).strip()

def get_text(root, path, ns):
    if root is None:
        return "0.00"
    try:
        el = root.find(path, ns)
    except Exception:
        el = None
    if el is not None and el.text is not None:
        return el.text

    # fallback sem namespace para XMLs antigos/fora do padrão
    try:
        plain_path = path.replace("nfe:", "")
        el_plain = root.find(plain_path)
        if el_plain is not None and el_plain.text is not None:
            return el_plain.text
    except Exception:
        pass
    return "0.00"


def _icms_vals(imposto, ns):
    if imposto is None:
        return "0", "0", "0"

    icms_group = imposto.find('.//nfe:ICMS', ns)
    if icms_group is None:
        icms_group = imposto.find('.//ICMS')
    if icms_group is None:
        return "0", "0", "0"

    icms_node = None
    for child in icms_group:
        if isinstance(child.tag, str):
            icms_node = child
            break
    if icms_node is None:
        return "0", "0", "0"

    vbc = get_text(icms_node, './/nfe:vBC', ns)
    picms = get_text(icms_node, './/nfe:pICMS', ns)
    vicms = get_text(icms_node, './/nfe:vICMS', ns)
    return vbc, picms, vicms


def parse_nfcom_xml(xml_bytes: bytes):
    root = etree.fromstring(xml_bytes)
    ns_uri = root.tag.split('}')[0].strip('{') if '}' in root.tag else ''
    ns = {'nfe': ns_uri} if ns_uri else {}

    inf = root.find('.//nfe:infNFCom', ns) if ns else root.find('.//infNFCom')
    if inf is None:
        inf = root

    emit = inf.find('.//nfe:emit', ns) if ns else inf.find('.//emit')
    dest = inf.find('.//nfe:dest', ns) if ns else inf.find('.//dest')

    nNF = get_text(inf, './/nfe:nNF', ns)
    serie = get_text(inf, './/nfe:serie', ns)
    cNF = get_text(inf, './/nfe:cNF', ns)
    dhEmi = get_text(inf, './/nfe:dhEmi', ns)
    if dhEmi == '0.00':
        dhEmi = get_text(inf, './/nfe:dEmi', ns)

    emit_nome = get_text(emit, './/nfe:xNome', ns) if emit is not None else None
    emit_doc = get_text(emit, './/nfe:CNPJ', ns) if emit is not None else None
    if emit_doc == '0.00':
        emit_doc = get_text(emit, './/nfe:CPF', ns) if emit is not None else None

    dest_nome = get_text(dest, './/nfe:xNome', ns) if dest is not None else None
    dest_doc = get_text(dest, './/nfe:CNPJ', ns) if dest is not None else None
    if dest_doc in (None, '0.00'):
        dest_doc = get_text(dest, './/nfe:CPF', ns) if dest is not None else None

    valor_total = safe_float(get_text(inf, './/nfe:vNF', ns))
    vProd = safe_float(get_text(inf, './/nfe:vProd', ns))
    vICMS_total = safe_float(get_text(root, './/nfe:ICMSTot/nfe:vICMS', ns))
    vPIS_total = safe_float(get_text(root, './/nfe:total/nfe:vPIS', ns))
    vCOFINS_total = safe_float(get_text(root, './/nfe:total/nfe:vCOFINS', ns))
    vFUST_total = safe_float(get_text(root, './/nfe:total/nfe:vFUST', ns))
    vFUNTTEL_total = safe_float(get_text(root, './/nfe:total/nfe:vFUNTTEL', ns))
    vDesc_total = safe_float(get_text(root, './/nfe:total/nfe:vDesc', ns))
    vOutro_total = safe_float(get_text(root, './/nfe:total/nfe:vOutro', ns))
    vIBS_total = safe_float(get_text(root, './/nfe:IBSCBSTot/nfe:vIBS', ns))
    vCBS_total = safe_float(get_text(root, './/nfe:IBSCBSTot/nfe:vCBS', ns))

    ret = inf.find('.//nfe:vRetTribTot', ns) if ns else inf.find('.//vRetTribTot')
    ret_pis = safe_float(get_text(ret, './/nfe:vRetPIS', ns)) if ret is not None else 0.0
    ret_cofins = safe_float(get_text(ret, './/nfe:vRetCofins', ns)) if ret is not None else 0.0
    ret_csll = safe_float(get_text(ret, './/nfe:vRetCSLL', ns)) if ret is not None else 0.0
    ret_irrf = safe_float(get_text(ret, './/nfe:vIRRF', ns)) if ret is not None else 0.0

    itens = []
    dets = inf.findall('.//nfe:det', ns) if ns else inf.findall('.//det')
    for det in dets:
        prod = det.find('.//nfe:prod', ns) if ns else det.find('.//prod')
        imposto = det.find('.//nfe:imposto', ns) if ns else det.find('.//imposto')
        pis = imposto.find('.//nfe:PIS', ns) if imposto is not None and ns else (imposto.find('.//PIS') if imposto is not None else None)
        cofins = imposto.find('.//nfe:COFINS', ns) if imposto is not None and ns else (imposto.find('.//COFINS') if imposto is not None else None)

        cClass = get_text(prod, './/nfe:cClass', ns) if prod is not None else ''
        cfop = get_text(prod, './/nfe:CFOP', ns) if prod is not None else ''
        cProd = get_text(prod, './/nfe:cProd', ns) if prod is not None else ''
        xProd = get_text(prod, './/nfe:xProd', ns) if prod is not None else ''
        uMed = get_text(prod, './/nfe:uMed', ns) if prod is not None else '0'
        qFaturada = get_text(prod, './/nfe:qFaturada', ns) if prod is not None else '0'
        if qFaturada == '0.00':
            qFaturada = get_text(prod, './/nfe:qCom', ns) if prod is not None else '0'

        v_un = safe_float(get_text(prod, './/nfe:vUnCom', ns)) if prod is not None else 0.0
        v_prod = safe_float(get_text(prod, './/nfe:vProd', ns)) if prod is not None else 0.0
        v_desc = safe_float(get_text(prod, './/nfe:vDesc', ns)) if prod is not None else 0.0
        v_outro = safe_float(get_text(prod, './/nfe:vOutro', ns)) if prod is not None else 0.0
        v_fust = safe_float(get_text(imposto, './/nfe:FUST/nfe:vFUST', ns)) if imposto is not None else 0.0
        v_funttel = safe_float(get_text(imposto, './/nfe:FUNTTEL/nfe:vFUNTTEL', ns)) if imposto is not None else 0.0
        ibscbs = prod.find('.//nfe:IBSCBS', ns) if prod is not None and ns else (prod.find('.//IBSCBS') if prod is not None else None)
        v_ibs = safe_float(get_text(ibscbs, './/nfe:vIBS', ns)) if ibscbs is not None else 0.0
        v_cbs = safe_float(get_text(ibscbs, './/nfe:vCBS', ns)) if ibscbs is not None else 0.0
        vpis = safe_float(get_text(pis, './/nfe:vPIS', ns)) if pis is not None else 0.0
        vcofins = safe_float(get_text(cofins, './/nfe:vCOFINS', ns)) if cofins is not None else 0.0
        vbc, picms, vicms = _icms_vals(imposto, ns)

        itens.append({
            'cClass': '' if cClass == '0.00' else cClass,
            'CFOP': '' if cfop == '0.00' else cfop,
            'cProd': '' if cProd == '0.00' else cProd,
            'xProd': '' if xProd == '0.00' else xProd,
            'desc': '' if xProd == '0.00' else xProd,
            'uMed': '' if uMed == '0.00' else uMed,
            'un': '' if uMed == '0.00' else uMed,
            'qFaturada': qFaturada,
            'qCom': safe_float(qFaturada),
            'qtd': qFaturada,
            'vProd': v_prod,
            'vProd_br': br_money(v_prod),
            'v_total': br_money(v_prod),
            'v_unit': br_money(v_un),
            'vPIS': br_money(vpis),
            'vCOFINS': br_money(vcofins),
            'vBC': vbc,
            'pICMS': picms,
            'vICMS': vicms,
            'pis_cofins': f"{br_money(vpis)}/{br_money(vcofins)}",
            'vDesc': br_money(v_desc),
            'vOutro': br_money(v_outro),
            'vFUST': br_money(v_fust),
            'vFUNTTEL': br_money(v_funttel),
            'vIBS': br_money(v_ibs),
            'vCBS': br_money(v_cbs),
            'ibs_cbs': f"{br_money(v_ibs)}/{br_money(v_cbs)}",
            'icms': vicms,
        })

    print('IRRF:', ret_irrf)
    print('Valor Total (vNF):', valor_total)
    print('Itens extraídos:', len(itens))

    return {
        'tipo': 'NFCom',
        'nNF': nNF if nNF != '0.00' else None,
        'serie': serie if serie != '0.00' else None,
        'cNF': cNF if cNF != '0.00' else None,
        'dhEmi': dhEmi if dhEmi != '0.00' else None,
        'dhEmi_fmt': br_date(dhEmi if dhEmi != '0.00' else ''),
        'emitente': {'xNome': None if emit_nome == '0.00' else emit_nome, 'CNPJ': None if emit_doc == '0.00' else emit_doc},
        'destinatario': {'xNome': None if dest_nome == '0.00' else dest_nome, 'doc': None if dest_doc == '0.00' else dest_doc},
        'itens': itens,
        'valor_total': valor_total,
        'totais': {
            'vNF_num': valor_total,
            'vNF': br_money(valor_total),
            'vProd': vProd,
            'vProd_br': br_money(vProd),
            'vPagar': valor_total,
            'vPagar_br': br_money(valor_total),
            'vICMS_total': br_money(vICMS_total),
            'vPIS_total': br_money(vPIS_total),
            'vCOFINS_total': br_money(vCOFINS_total),
            'vFUST_total': br_money(vFUST_total),
            'vFUNTTEL_total': br_money(vFUNTTEL_total),
            'vDesc_total': br_money(vDesc_total),
            'vOutro_total': br_money(vOutro_total),
            'vIBS_total': br_money(vIBS_total),
            'vCBS_total': br_money(vCBS_total),
            'vIBSCBS_total': f"{br_money(vIBS_total)}/{br_money(vCBS_total)}",
        },
        'ret_pis': ret_pis,
        'ret_cofins': ret_cofins,
        'ret_csll': ret_csll,
        'ret_irrf': ret_irrf,
        'retencoes': {
            'pis': br_money(ret_pis),
            'cofins': br_money(ret_cofins),
            'csll': br_money(ret_csll),
            'irrf': br_money(ret_irrf),
            'vRetPIS': ret_pis,
            'vRetPIS_br': br_money(ret_pis),
            'vRetCofins': ret_cofins,
            'vRetCofins_br': br_money(ret_cofins),
            'vRetCSLL': ret_csll,
            'vRetCSLL_br': br_money(ret_csll),
            'vIRRF': ret_irrf,
            'vIRRF_br': br_money(ret_irrf),
            'total': (ret_pis + ret_cofins + ret_csll + ret_irrf),
            'total_br': br_money(ret_pis + ret_cofins + ret_csll + ret_irrf),
        }
    }

def parse_nfe_xml(xml_bytes: bytes):
    root = etree.fromstring(xml_bytes)
    ns_uri = root.tag.split('}')[0].strip('{') if '}' in root.tag else ''
    ns = {'nfe': ns_uri} if ns_uri else {}

    inf = root.find('.//nfe:infNFe', ns) if ns else root.find('.//infNFe')
    if inf is None:
        inf = root

    emit = inf.find('.//nfe:emit', ns) if ns else inf.find('.//emit')
    dest = inf.find('.//nfe:dest', ns) if ns else inf.find('.//dest')

    nNF = get_text(inf, './/nfe:nNF', ns)
    serie = get_text(inf, './/nfe:serie', ns)
    cNF = get_text(inf, './/nfe:cNF', ns)
    dhEmi = get_text(inf, './/nfe:dhEmi', ns)
    if dhEmi == '0.00':
        dhEmi = get_text(inf, './/nfe:dEmi', ns)

    emit_nome = get_text(emit, './/nfe:xNome', ns) if emit is not None else None
    emit_doc = get_text(emit, './/nfe:CNPJ', ns) if emit is not None else None
    if emit_doc == '0.00':
        emit_doc = get_text(emit, './/nfe:CPF', ns) if emit is not None else None

    dest_nome = get_text(dest, './/nfe:xNome', ns) if dest is not None else None
    dest_doc = get_text(dest, './/nfe:CNPJ', ns) if dest is not None else None
    if dest_doc in (None, '0.00'):
        dest_doc = get_text(dest, './/nfe:CPF', ns) if dest is not None else None

    valor_total = safe_float(get_text(inf, './/nfe:vNF', ns))
    vICMS_total = safe_float(get_text(root, './/nfe:ICMSTot/nfe:vICMS', ns))
    vPIS_total = safe_float(get_text(root, './/nfe:total/nfe:vPIS', ns))
    vCOFINS_total = safe_float(get_text(root, './/nfe:total/nfe:vCOFINS', ns))
    vFUST_total = safe_float(get_text(root, './/nfe:total/nfe:vFUST', ns))
    vFUNTTEL_total = safe_float(get_text(root, './/nfe:total/nfe:vFUNTTEL', ns))
    vDesc_total = safe_float(get_text(root, './/nfe:total/nfe:vDesc', ns))
    vOutro_total = safe_float(get_text(root, './/nfe:total/nfe:vOutro', ns))
    vIBS_total = safe_float(get_text(root, './/nfe:IBSCBSTot/nfe:vIBS', ns))
    vCBS_total = safe_float(get_text(root, './/nfe:IBSCBSTot/nfe:vCBS', ns))

    ret = inf.find('.//nfe:vRetTribTot', ns) if ns else inf.find('.//vRetTribTot')
    ret_pis = safe_float(get_text(ret, './/nfe:vRetPIS', ns)) if ret is not None else 0.0
    ret_cofins = safe_float(get_text(ret, './/nfe:vRetCofins', ns)) if ret is not None else 0.0
    ret_csll = safe_float(get_text(ret, './/nfe:vRetCSLL', ns)) if ret is not None else 0.0
    ret_irrf = safe_float(get_text(ret, './/nfe:vIRRF', ns)) if ret is not None else 0.0

    itens = []
    total_vprod = 0.0
    dets = inf.findall('.//nfe:det', ns) if ns else inf.findall('.//det')
    for det in dets:
        prod = det.find('.//nfe:prod', ns) if ns else det.find('.//prod')
        imposto = det.find('.//nfe:imposto', ns) if ns else det.find('.//imposto')
        pis = imposto.find('.//nfe:PIS', ns) if imposto is not None and ns else (imposto.find('.//PIS') if imposto is not None else None)
        cofins = imposto.find('.//nfe:COFINS', ns) if imposto is not None and ns else (imposto.find('.//COFINS') if imposto is not None else None)

        cClass = get_text(prod, './/nfe:cClass', ns) if prod is not None else ''
        cfop = get_text(prod, './/nfe:CFOP', ns) if prod is not None else ''
        cProd = get_text(prod, './/nfe:cProd', ns) if prod is not None else ''
        xProd = get_text(prod, './/nfe:xProd', ns) if prod is not None else ''
        uMed = get_text(prod, './/nfe:uMed', ns) if prod is not None else '0'
        qFaturada = get_text(prod, './/nfe:qFaturada', ns) if prod is not None else '0'
        if qFaturada == '0.00':
            qFaturada = get_text(prod, './/nfe:qCom', ns) if prod is not None else '0'

        v_un = safe_float(get_text(prod, './/nfe:vUnCom', ns)) if prod is not None else 0.0
        v_prod = safe_float(get_text(prod, './/nfe:vProd', ns)) if prod is not None else 0.0
        total_vprod += v_prod
        v_desc = safe_float(get_text(prod, './/nfe:vDesc', ns)) if prod is not None else 0.0
        v_outro = safe_float(get_text(prod, './/nfe:vOutro', ns)) if prod is not None else 0.0
        v_fust = safe_float(get_text(imposto, './/nfe:FUST/nfe:vFUST', ns)) if imposto is not None else 0.0
        v_funttel = safe_float(get_text(imposto, './/nfe:FUNTTEL/nfe:vFUNTTEL', ns)) if imposto is not None else 0.0
        ibscbs = prod.find('.//nfe:IBSCBS', ns) if prod is not None and ns else (prod.find('.//IBSCBS') if prod is not None else None)
        v_ibs = safe_float(get_text(ibscbs, './/nfe:vIBS', ns)) if ibscbs is not None else 0.0
        v_cbs = safe_float(get_text(ibscbs, './/nfe:vCBS', ns)) if ibscbs is not None else 0.0
        vpis = safe_float(get_text(pis, './/nfe:vPIS', ns)) if pis is not None else 0.0
        vcofins = safe_float(get_text(cofins, './/nfe:vCOFINS', ns)) if cofins is not None else 0.0
        vbc, picms, vicms = _icms_vals(imposto, ns)

        itens.append({
            'cClass': '' if cClass == '0.00' else cClass,
            'CFOP': '' if cfop == '0.00' else cfop,
            'cProd': '' if cProd == '0.00' else cProd,
            'xProd': '' if xProd == '0.00' else xProd,
            'desc': '' if xProd == '0.00' else xProd,
            'uMed': '' if uMed == '0.00' else uMed,
            'un': '' if uMed == '0.00' else uMed,
            'qFaturada': qFaturada,
            'qCom': safe_float(qFaturada),
            'qtd': qFaturada,
            'vProd': v_prod,
            'vProd_br': br_money(v_prod),
            'v_total': br_money(v_prod),
            'v_unit': br_money(v_un),
            'vPIS': br_money(vpis),
            'vCOFINS': br_money(vcofins),
            'vBC': vbc,
            'pICMS': picms,
            'vICMS': vicms,
            'pis_cofins': f"{br_money(vpis)}/{br_money(vcofins)}",
            'vDesc': br_money(v_desc),
            'vOutro': br_money(v_outro),
            'vFUST': br_money(v_fust),
            'vFUNTTEL': br_money(v_funttel),
            'vIBS': br_money(v_ibs),
            'vCBS': br_money(v_cbs),
            'ibs_cbs': f"{br_money(v_ibs)}/{br_money(v_cbs)}",
            'icms': vicms,
        })

    print('IRRF:', ret_irrf)
    print('Valor Total (vNF):', valor_total)
    print('Itens extraídos:', len(itens))

    return {
        'tipo': 'NFe',
        'nNF': nNF if nNF != '0.00' else None,
        'serie': serie if serie != '0.00' else None,
        'cNF': cNF if cNF != '0.00' else None,
        'dhEmi': dhEmi if dhEmi != '0.00' else None,
        'dhEmi_fmt': br_date(dhEmi if dhEmi != '0.00' else ''),
        'emitente': {'xNome': None if emit_nome == '0.00' else emit_nome, 'CNPJ': None if emit_doc == '0.00' else emit_doc},
        'destinatario': {'xNome': None if dest_nome == '0.00' else dest_nome, 'doc': None if dest_doc == '0.00' else dest_doc},
        'itens': itens,
        'valor_total': valor_total,
        'totais': {
            'vNF_num': valor_total,
            'vNF': br_money(valor_total),
            'vProd': total_vprod,
            'vProd_br': br_money(total_vprod),
            'vICMS_total': br_money(vICMS_total),
            'vPIS_total': br_money(vPIS_total),
            'vCOFINS_total': br_money(vCOFINS_total),
            'vFUST_total': br_money(vFUST_total),
            'vFUNTTEL_total': br_money(vFUNTTEL_total),
            'vDesc_total': br_money(vDesc_total),
            'vOutro_total': br_money(vOutro_total),
            'vIBS_total': br_money(vIBS_total),
            'vCBS_total': br_money(vCBS_total),
            'vIBSCBS_total': f"{br_money(vIBS_total)}/{br_money(vCBS_total)}",
        },
        'ret_pis': ret_pis,
        'ret_cofins': ret_cofins,
        'ret_csll': ret_csll,
        'ret_irrf': ret_irrf,
        'retencoes': {
            'pis': br_money(ret_pis),
            'cofins': br_money(ret_cofins),
            'csll': br_money(ret_csll),
            'irrf': br_money(ret_irrf),
            'vRetPIS': ret_pis,
            'vRetCofins': ret_cofins,
            'vRetCSLL': ret_csll,
            'vIRRF': ret_irrf,
            'total': (ret_pis + ret_cofins + ret_csll + ret_irrf),
            'total_br': br_money(ret_pis + ret_cofins + ret_csll + ret_irrf),
        }
    }

def parse_xml_any(xml_bytes: bytes):
    # Detecta tipo pelo conteúdo
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return {"error": "XML inválido"}
    name = etree.QName(root).localname.lower()
    # Pode vir em *Proc
    if "nfcom" in name or root.xpath("//*[local-name()='infNFCom']"):
        try:
            return parse_nfcom_xml(xml_bytes)
        except Exception as e:
            return {"error": f"Falha NFCom: {e}"}
    if "nfe" in name or root.xpath("//*[local-name()='infNFe']"):
        try:
            return parse_nfe_xml(xml_bytes)
        except Exception as e:
            return {"error": f"Falha NFe: {e}"}
    return {"error": "Tipo XML não suportado (NFe/NFCom)"}

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
    # Busca pelo session_id da sessão
    sid = session.get("resumo_session_id")
    data = None
    if sid:
        data = r_get_json(f"resumo:data:{sid}")
    if not data:
        # fallback: exemplo
        data = gerar_dados_exemplo()
    validar_integridade(data)
    return render_template("resumo_resultado.html", data=data)

@app.route("/resumo/csv")
def resumo_csv_page():
    sid = session.get("resumo_session_id")
    data = r_get_json(f"resumo:data:{sid}") if sid else None
    if not data:
        return jsonify({"success": False, "error": "Resumo não encontrado para exportação"}), 404

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["cClass", "CFOP", "nNF", "Emitente", "Destinatário", "Data Emissão", "Valor"])

    for linha in data.get("linhas") or []:
        cclass = linha.get("cClass") or ""
        for cfop_data in linha.get("cfops") or []:
            cfop = cfop_data.get("cfop") or ""
            for nota in cfop_data.get("notas") or []:
                writer.writerow([
                    cclass,
                    cfop,
                    nota.get("nNF") or "",
                    nota.get("xNome") or "",
                    nota.get("xContato") or "",
                    nota.get("dhEmi_fmt") or "",
                    nota.get("valor") or 0,
                ])

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=resumo.csv"},
    )

@app.route("/csv")
def csv_page():
    return render_template("csv.html")

# =========================================================
# Lote assíncrono (com progresso + taxa)
# =========================================================
@app.route("/api/lote/processar", methods=["POST"])
def api_lote_processar():
    try:
        if "zip_xmls" not in request.files:
            return jsonify({"success": False, "error": "Envie o ZIP no campo zip_xmls"}), 400

        zf = request.files["zip_xmls"]
        if not zf.filename.lower().endswith(".zip"):
            return jsonify({"success": False, "error": "Envie um arquivo .zip"}), 400

        sid = str(uuid.uuid4())
        zip_path = os.path.join(UPLOADS_DIR, f"lote_{sid}.zip")
        zf.save(zip_path)

        remover_desconto = (request.form.get("remover_desconto", "false").lower() == "true")
        remover_outros = (request.form.get("remover_outros", "false").lower() == "true")
        regras_txt = request.form.get("regras_cclass_cfop", "")

        session["lote_session_id"] = sid
        _set_lote_status(sid, status="queued", done=False, progress=0, processed=0, total=None, rate_xml_s=0.0)

        th = threading.Thread(
            target=_process_zip_lote_async,
            args=(sid, zip_path, remover_desconto, remover_outros, regras_txt),
            daemon=True,
        )
        th.start()

        return jsonify({"success": True, "session_id": sid})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/lote/status/<sid>")
def api_lote_status(sid):
    st = r_get_json(f"lote:status:{sid}")
    if not st:
        return jsonify({"success": True, "status": "nao_encontrado", "done": True, "progress": 0})
    st["success"] = True
    return jsonify(st)

@app.route("/api/lote/baixar/<sid>")
def api_lote_baixar(sid):
    st = r_get_json(f"lote:status:{sid}")
    if not st:
        return jsonify({"success": False, "error": "Sessão não encontrada"}), 404
    if st.get("status") != "done" or not st.get("output_path"):
        return jsonify({"success": False, "error": "Processamento ainda não finalizado"}), 409

    out_path = st["output_path"]
    if not os.path.exists(out_path):
        return jsonify({"success": False, "error": "Arquivo de saída não encontrado"}), 404

    return send_file(out_path, as_attachment=True, download_name=f"lote_processado_{sid}.zip", mimetype="application/zip")

# =========================================================
# Nota única: retorna HTML via JS (a tela formata)
# =========================================================
@app.route("/api/nota/visualizar", methods=["POST"])
def api_nota_visualizar():
    try:
        if "xml_nota" not in request.files:
            return jsonify({"success": False, "error": "Envie o arquivo no campo xml_nota"}), 400
        f = request.files["xml_nota"]
        xml_bytes = f.read()
        dados = parse_xml_any(xml_bytes)
        if "error" in dados:
            return jsonify({"success": False, "error": dados["error"]}), 400
        return jsonify({"success": True, "data": dados})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# =========================================================
# Resumo assíncrono (evita timeout em ZIP grande no Render)
# =========================================================
SUMMARY_TTL = 60 * 60 * 4  # 4h
DETAILS_LIMIT = 800  # evita JSON gigante
LOTE_TTL = 60 * 60 * 4

def _set_status(sid, **kw):
    st = r_get_json(f"resumo:status:{sid}") or {"session_id": sid}
    st.update(kw)
    r_setex(f"resumo:status:{sid}", SUMMARY_TTL, st)

def _set_lote_status(sid, **kw):
    st = r_get_json(f"lote:status:{sid}") or {"session_id": sid}
    st.update(kw)
    r_setex(f"lote:status:{sid}", LOTE_TTL, st)

def _find_child(parent, tag_name):
    if parent is None:
        return None
    for child in parent.iterchildren():
        if etree.QName(child).localname == tag_name:
            return child
    return None

def _local_tag(parent, tag_name: str) -> str:
    """Retorna o nome da tag respeitando namespace do pai (quando existir)."""
    if parent is None or not isinstance(parent.tag, str):
        return tag_name
    if parent.tag.startswith("{"):
        ns = parent.tag.split("}", 1)[0][1:]
        return f"{{{ns}}}{tag_name}"
    return tag_name

def _upsert_child_text(parent, tag_name: str, text: str):
    """Atualiza um filho direto por nome local ou cria a tag caso não exista."""
    el = _find_child(parent, tag_name)
    if el is None:
        el = etree.SubElement(parent, _local_tag(parent, tag_name))
    el.text = text
    return el

def _parse_regras(regras_texto: str):
    regras = {}
    for raw in (regras_texto or "").splitlines():
        line = raw.strip()
        if not line or ";" not in line:
            continue
        cclass, cfop = [x.strip() for x in line.split(";", 1)]
        if cclass and cfop:
            regras[cclass] = cfop
    return regras

def _process_zip_lote_async(sid: str, zip_path: str, remover_desconto: bool, remover_outros: bool, regras_txt: str):
    started = time.time()
    regras = _parse_regras(regras_txt)

    _set_lote_status(
        sid,
        status="running",
        done=False,
        progress=0,
        processed=0,
        total=None,
        rate_xml_s=0.0,
        started_at=datetime.now().isoformat(),
        error=None,
    )

    out_zip_path = os.path.join(TEMP_DIR, f"lote_{sid}.zip")
    changed_files = 0
    total_changes = 0
    total_errors = 0

    try:
        with zipfile.ZipFile(zip_path, "r") as zin:
            names = [n for n in zin.namelist() if n.lower().endswith(".xml")]
            total = len(names)
            if total == 0:
                raise Exception("Nenhum XML encontrado no ZIP")

            _set_lote_status(sid, total=total)

            with zipfile.ZipFile(out_zip_path, "w", zipfile.ZIP_DEFLATED) as zout:
                for idx, name in enumerate(names, start=1):
                    try:
                        xml_bytes = zin.read(name)
                        root = etree.fromstring(xml_bytes)
                        changes_in_file = 0

                        for det in root.xpath("//*[local-name()='det']"):
                            prod = det.xpath(".//*[local-name()='prod']")
                            prod = prod[0] if prod else None
                            if prod is None:
                                continue

                            if remover_desconto:
                                el = _find_child(prod, "vDesc")
                                if el is not None and (el.text or "").strip() != "0":
                                    el.text = "0.00"
                                    changes_in_file += 1

                            if remover_outros:
                                el = _find_child(prod, "vOutro")
                                if el is not None and (el.text or "").strip() != "0":
                                    el.text = "0.00"
                                    changes_in_file += 1

                            cclass_el = _find_child(prod, "cClass")
                            cfop_el = _find_child(prod, "CFOP")
                            cclass = (cclass_el.text or "").strip() if cclass_el is not None else ""

                            # Regra principal: cClass existente no XML -> CFOP alvo.
                            target_cfop = regras.get(cclass)

                            # Caso não exista cClass no XML e haja somente uma regra,
                            # usa essa regra como valor padrão para incluir cClass/CFOP.
                            if not cclass and len(regras) == 1:
                                default_cclass, default_cfop = next(iter(regras.items()))
                                _upsert_child_text(prod, "cClass", default_cclass)
                                cclass = default_cclass
                                target_cfop = default_cfop
                                changes_in_file += 1

                            # Se houver CFOP alvo, altera quando existir e inclui quando faltar.
                            if target_cfop:
                                current_cfop = (cfop_el.text or "").strip() if cfop_el is not None else ""
                                if current_cfop != target_cfop:
                                    _upsert_child_text(prod, "CFOP", target_cfop)
                                    changes_in_file += 1

                        if changes_in_file > 0:
                            changed_files += 1
                            total_changes += changes_in_file

                        zout.writestr(name, etree.tostring(root, encoding="utf-8", xml_declaration=True))
                    except Exception:
                        total_errors += 1

                    elapsed = max(time.time() - started, 1e-9)
                    rate = idx / elapsed
                    pct = int((idx / total) * 100)
                    eta_seconds = max(int((total - idx) / max(rate, 1e-9)), 0)

                    changed_pct = round((changed_files / max(idx, 1)) * 100, 2)
                    if idx % 10 == 0 or idx == total:
                        _set_lote_status(
                            sid,
                            progress=pct,
                            processed=idx,
                            total=total,
                            rate_xml_s=round(rate, 2),
                            eta_seconds=eta_seconds,
                            changed_files=changed_files,
                            changed_pct=changed_pct,
                            total_changes=total_changes,
                            errors=total_errors,
                        )

        _set_lote_status(
            sid,
            status="done",
            done=True,
            progress=100,
            output_path=out_zip_path,
            finished_at=datetime.now().isoformat(),
            rate_xml_s=round((total / max(time.time() - started, 1e-9)), 2),
            changed_files=changed_files,
            changed_pct=round((changed_files / max(total, 1)) * 100, 2),
            total_changes=total_changes,
            errors=total_errors,
        )
    except Exception as e:
        _set_lote_status(sid, status="error", done=True, error=str(e), finished_at=datetime.now().isoformat())

def _process_zip_resumo(sid: str, zip_path: str):
    """
    Processa o ZIP em background e grava:
      - resumo:status:<sid> (progresso/estado)
      - resumo:data:<sid>   (payload compacto para o front)
    Importante: o agrupamento de notas por cClass/CFOP precisa ser completo, sem perda de registros.
    """
    try:
        _set_status(
            sid,
            status="running",
            progress=0,
            error=None,
            done=False,
            started_at=datetime.now().isoformat(),
            processed=0,
            total=None,
        )

        def add_note(lst, note):
            if lst is None:
                return
            lst.append(note)

        with zipfile.ZipFile(zip_path, "r") as z:
            names = [n for n in z.namelist() if n.lower().endswith(".xml")]
            total = len(names)
            if total == 0:
                raise Exception("Nenhum XML encontrado no ZIP")

            # Agregadores
            # cClass -> {desc,qtd_itens,v_total, cfops{cfop->{v_total,notas[]}}}
            by_cclass = {}
            by_item = {}    # (cProd,cClass,desc) -> {item,desc,cClass,qtd_itens,v_total, notas:[]}
            impostos = {"PIS Ret.": 0.0, "COFINS Ret.": 0.0, "CSLL Ret.": 0.0, "IRRF Ret.": 0.0}
            impostos_notas = {"PIS Ret.": [], "COFINS Ret.": [], "CSLL Ret.": [], "IRRF Ret.": []}

            emit_nome = None
            emit_cnpj = None
            total_geral = 0.0
            total_processadas = 0
            ok = 0
            falhas = 0
            primeiro_erro = None

            for i, name in enumerate(names, start=1):
                try:
                    xml_bytes = z.read(name)
                    d = parse_xml_any(xml_bytes)
                    if "error" in d:
                        raise Exception(d["error"])

                    if not emit_nome:
                        emit_nome = (d.get("emitente") or {}).get("xNome")
                        emit_cnpj = (d.get("emitente") or {}).get("CNPJ")

                    # informações básicas da nota (para sublinhas)
                    nota_base = {
                        "nNF": d.get("nNF"),
                        "cNF": d.get("cNF"),
                        "xNome": (d.get("emitente") or {}).get("xNome"),
                        "xContato": (d.get("destinatario") or {}).get("xNome"),
                        "dhEmi_fmt": d.get("dhEmi_fmt") or br_date(d.get("dhEmi") or ""),
                        "arquivo": name,
                    }

                    itens = d.get("itens") or []
                    for it in itens:
                        cClass = (it.get("cClass") or "").strip()
                        cfop = (it.get("CFOP") or "").strip()
                        cProd = (it.get("cProd") or "").strip()
                        xProd = (it.get("xProd") or "").strip()
                        v = float(it.get("vProd") or 0.0)

                        total_geral += v

                        # --- Agrupa por cClass
                        if cClass:
                            rec = by_cclass.setdefault(
                                cClass,
                                {
                                    "cClass": cClass,
                                    "desc": xProd or "",
                                    "qtd_itens": 0,
                                    "v_total": 0.0,
                                    "cfops": defaultdict(lambda: {"v_total": 0.0, "notas": []}),
                                },
                            )
                            if not rec["desc"] and xProd:
                                rec["desc"] = xProd
                            rec["qtd_itens"] += 1
                            rec["v_total"] += v

                            if cfop:
                                cfop_rec = rec["cfops"][cfop]
                                cfop_rec["v_total"] += v
                                cfop_rec["notas"].append({**nota_base, "valor": v, "valor_br": br_money(v)})
                                total_processadas += 1

                        # --- Agrupa por item (cProd)
                        if cProd:
                            key = (cProd, cClass, xProd)
                            ir = by_item.setdefault(
                                key,
                                {
                                    "item": cProd,
                                    "desc": xProd,
                                    "cClass": cClass,
                                    "qtd_itens": 0,
                                    "v_total": 0.0,
                                    "notas": [],
                                },
                            )
                            ir["qtd_itens"] += 1
                            ir["v_total"] += v
                            add_note(ir["notas"], {**nota_base, "valor": v, "valor_br": br_money(v)})

                    # --- Retenções (NFCom)
                    rtt = d.get("retencoes") or {}
                    v_pis = float(rtt.get("vRetPIS") or 0.0)
                    v_cof = float(rtt.get("vRetCofins") or 0.0)
                    v_csl = float(rtt.get("vRetCSLL") or 0.0)
                    v_irr = float(rtt.get("vIRRF") or 0.0)

                    if v_pis:
                        impostos["PIS Ret."] += v_pis
                        add_note(impostos_notas["PIS Ret."], {**nota_base, "valor": v_pis, "valor_br": br_money(v_pis)})
                    if v_cof:
                        impostos["COFINS Ret."] += v_cof
                        add_note(impostos_notas["COFINS Ret."], {**nota_base, "valor": v_cof, "valor_br": br_money(v_cof)})
                    if v_csl:
                        impostos["CSLL Ret."] += v_csl
                        add_note(impostos_notas["CSLL Ret."], {**nota_base, "valor": v_csl, "valor_br": br_money(v_csl)})
                    if v_irr:
                        impostos["IRRF Ret."] += v_irr
                        add_note(impostos_notas["IRRF Ret."], {**nota_base, "valor": v_irr, "valor_br": br_money(v_irr)})

                    ok += 1

                except Exception as e:
                    falhas += 1
                    if not primeiro_erro:
                        primeiro_erro = f"{name}: {e}"

                if i % 25 == 0 or i == total:
                    _set_status(sid, progress=int((i / total) * 100), processed=i, total=total)

            # Monta payload compacto
            linhas = []
            for c, rec in by_cclass.items():
                cfops_list = []
                for cfop, cfop_data in rec["cfops"].items():
                    cfops_list.append(
                        {
                            "cfop": cfop,
                            "v_total": cfop_data["v_total"],
                            "v_total_br": br_money(cfop_data["v_total"]),
                            "notas": cfop_data["notas"],
                        }
                    )
                linhas.append(
                    {
                        "cClass": c,
                        "desc": rec["desc"] or "",
                        "qtd_itens": rec["qtd_itens"],
                        "v_total": rec["v_total"],
                        "v_total_br": br_money(rec["v_total"]),
                        "pct": 0.0,
                        "pct_br": "",
                        "cfops": sorted(cfops_list, key=lambda x: x["v_total"], reverse=True)[:DETAILS_LIMIT],
                    }
                )

            total_base = max(total_geral, 1e-9)
            for l in linhas:
                pct = (l["v_total"] / total_base) * 100.0
                l["pct"] = pct
                l["pct_br"] = f"{pct:.2f}%".replace(".", ",")

            top = sorted(linhas, key=lambda x: x["v_total"], reverse=True)[:12]
            labels = [x["cClass"] for x in top]
            valores = [x["v_total"] for x in top]

            itens_linhas = list(by_item.values())
            for it in itens_linhas:
                it["v_total_br"] = br_money(it["v_total"])
            itens_linhas = sorted(itens_linhas, key=lambda x: x["v_total"], reverse=True)[:DETAILS_LIMIT]

            # impostos_linhas
            R = sum(impostos.values()) or 0.0
            impostos_linhas = []
            for tipo, v in impostos.items():
                if v <= 0:
                    continue
                pct = (v / max(R, 1e-9)) * 100.0
                impostos_linhas.append(
                    {
                        "tipo": tipo.replace(" Ret.", " Retido"),
                        "qtd_notas": ok,
                        "v_total": v,
                        "v_total_br": br_money(v),
                        "pct": pct,
                        "pct_br": f"{pct:.2f}%".replace(".", ","),
                        "notas": impostos_notas.get(tipo, []),
                    }
                )

            data = {
                "emitente_nome": emit_nome,
                "emitente_cnpj": emit_cnpj,
                "total_arquivos": ok,
                "total_geral": total_geral,
                "total_geral_br": br_money(total_geral),
                "total_impostos": R,
                "total_impostos_br": br_money(R),
                "labels": labels,
                "valores": valores,
                "linhas": sorted(linhas, key=lambda x: x["v_total"], reverse=True)[:DETAILS_LIMIT],
                "itens_linhas": itens_linhas,
                "impostos_linhas": impostos_linhas,
                "debug": {
                    "total_xml": len(names),
                    "total_ok": ok,
                    "total_falhas": falhas,
                    "primeiro_erro": primeiro_erro,
                    "total_notas_processadas": total_processadas,
                },
            }

            total_processadas_val, total_no_resumo = validar_integridade(data)
            print("Total notas processadas:", total_processadas_val)
            print("Total notas no resumo:", total_no_resumo)

            r_setex(f"resumo:data:{sid}", SUMMARY_TTL, data)
            _set_status(sid, status="done", progress=100, done=True, finished_at=datetime.now().isoformat(), error=None)

    except Exception as e:
        _set_status(sid, status="error", done=True, error=str(e), progress=100, finished_at=datetime.now().isoformat())


@app.route("/api/resumo/upload", methods=["POST"])
def api_resumo_upload():
    try:
        if "file" not in request.files:
            return jsonify({"success": False, "error": "Nenhum arquivo enviado"}), 400
        f = request.files["file"]
        if not f.filename or not f.filename.lower().endswith(".zip"):
            return jsonify({"success": False, "error": "Envie um arquivo .zip"}), 400

        sid = str(uuid.uuid4())
        zip_path = os.path.join(UPLOADS_DIR, f"{sid}.zip")
        f.save(zip_path)

        session["resumo_session_id"] = sid
        _set_status(sid, status="queued", progress=0, done=False, error=None, total=None, processed=0)

        th = threading.Thread(target=_process_zip_resumo, args=(sid, zip_path), daemon=True)
        th.start()

        return jsonify({"success": True, "session_id": sid})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/resumo/status")
def api_resumo_status():
    sid = request.args.get("session_id") or session.get("resumo_session_id")
    if not sid:
        return jsonify({"success": False, "error": "session_id é obrigatório"}), 400
    st = r_get_json(f"resumo:status:{sid}")
    if not st:
        return jsonify({"success": True, "status": "nao_encontrado", "done": True, "progress": 0})
    st["success"] = True
    st["redirect"] = url_for("resumo_resultado_page")
    return jsonify(st)

# =========================================================
# Lote por descrição (substitui funcionalidade antiga do CSV)
# =========================================================
def normalizar(texto):
    if not texto:
        return ""
    texto = texto.lower().strip()
    texto = " ".join(texto.split())
    texto = unicodedata.normalize("NFKD", texto)
    texto = texto.encode("ASCII", "ignore").decode("ASCII")
    return texto


def _parse_regras_descricao(regras_texto: str):
    regras = []
    for linha in (regras_texto or "").splitlines():
        linha = linha.strip()
        if not linha:
            continue

        partes = linha.split(";")
        if len(partes) != 2:
            continue

        descricao_regra = partes[0].strip()
        nova_cclass = partes[1].strip()
        if not descricao_regra or not nova_cclass:
            continue

        regras.append((descricao_regra, normalizar(descricao_regra), nova_cclass))
    return regras


def upsert_child_text(parent, tag, value, ns_uri):
    tag_full = f"{{{ns_uri}}}{tag}" if ns_uri else tag
    el = parent.find(tag_full)
    if el is None:
        el = etree.SubElement(parent, tag_full)
    el.text = value


def _find_child_local(parent, tag_name: str):
    if parent is None:
        return None
    for child in parent:
        if isinstance(child.tag, str) and etree.QName(child).localname == tag_name:
            return child
    return None


def _set_lote_descricao_status(sid, **kw):
    st = r_get_json(f"csv:status:{sid}") or {"session_id": sid}
    st.update(kw)
    r_setex(f"csv:status:{sid}", SUMMARY_TTL, st)


def _process_descricao_xml_stream(xml_stream, regras):
    context = etree.iterparse(xml_stream, events=("end",), recover=True, huge_tree=True)
    file_changes = 0

    for _, elem in context:
        if not isinstance(elem.tag, str) or etree.QName(elem).localname != "det":
            continue

        prod = _find_child_local(elem, "prod")
        if prod is None:
            continue

        xprod_el = _find_child_local(prod, "xProd")
        xprod = ((xprod_el.text or "") if xprod_el is not None else "").strip()
        if not xprod:
            continue

        xprod_norm = normalizar(xprod)
        for descricao_regra, regra_norm, cclass_rule in regras:
            match = regra_norm in xprod_norm
            print("Descrição XML:", xprod)
            print("Regra:", descricao_regra)
            print("Match:", match)
            if match:
                cclass_el = _find_child_local(prod, "cClass")
                current = (cclass_el.text or "").strip() if cclass_el is not None else ""
                if current != cclass_rule:
                    ns_uri = etree.QName(prod).namespace or ""
                    upsert_child_text(prod, "cClass", cclass_rule, ns_uri)
                    file_changes += 1
                break

    root = context.root
    xml_out = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    if root is not None:
        root.clear()
    del context
    return xml_out, file_changes


def _process_lote_descricao_async(sid: str, in_zip_path: str, regras):
    out_zip_path = os.path.join(UPLOADS_DIR, f"lote_descricao_{sid}.zip")
    total_xml = 0
    changed_files = 0
    total_changes = 0
    errors = 0
    processados = 0

    try:
        _set_lote_descricao_status(sid, status="running", done=False, processados=0, total=0, percentual=0)

        with zipfile.ZipFile(in_zip_path, "r") as zin:
            total_xml = sum(1 for i in zin.infolist() if (not i.is_dir()) and i.filename.lower().endswith('.xml'))

        _set_lote_descricao_status(sid, total=total_xml, percentual=0)

        with zipfile.ZipFile(in_zip_path, "r") as zin, zipfile.ZipFile(out_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for info in zin.infolist():
                name = info.filename
                if info.is_dir():
                    continue

                if not name.lower().endswith(".xml"):
                    try:
                        with zin.open(info, "r") as src:
                            zout.writestr(name, src.read())
                    except Exception:
                        errors += 1
                    continue

                try:
                    with zin.open(info, "r") as xml_file:
                        xml_out, file_changes = _process_descricao_xml_stream(xml_file, regras)
                    if file_changes > 0:
                        changed_files += 1
                        total_changes += file_changes
                    zout.writestr(name, xml_out)
                except Exception:
                    errors += 1

                processados += 1
                percentual = int((processados / max(total_xml, 1)) * 100)
                if processados % 1000 == 0:
                    print(f"Processados {processados} arquivos")
                if processados % 50 == 0 or processados == total_xml:
                    _set_lote_descricao_status(
                        sid,
                        processados=processados,
                        total=total_xml,
                        percentual=percentual,
                        status="running",
                        done=False,
                    )

        r_setex(
            f"csv:lote:{sid}",
            SUMMARY_TTL,
            {
                "output_path": out_zip_path,
                "total_xml": total_xml,
                "changed_files": changed_files,
                "total_changes": total_changes,
                "errors": errors,
            },
        )

        _set_lote_descricao_status(
            sid,
            status="done",
            done=True,
            processados=processados,
            total=total_xml,
            percentual=100,
            changed_files=changed_files,
            total_changes=total_changes,
            errors=errors,
        )
    except Exception as e:
        _set_lote_descricao_status(
            sid,
            status="error",
            done=True,
            processados=processados,
            total=total_xml,
            percentual=int((processados / max(total_xml, 1)) * 100) if total_xml else 0,
            error=str(e),
            errors=errors + 1,
        )
    finally:
        try:
            if os.path.exists(in_zip_path):
                os.remove(in_zip_path)
        except Exception:
            pass


@app.route("/api/csv/gerar", methods=["POST"])
def api_csv_gerar():
    try:
        if "zip_xmls" not in request.files:
            return jsonify({"success": False, "error": "Envie o ZIP no campo zip_xmls"}), 400

        zf = request.files["zip_xmls"]
        if not zf.filename.lower().endswith(".zip"):
            return jsonify({"success": False, "error": "Envie um arquivo .zip"}), 400

        regras = _parse_regras_descricao(request.form.get("regras_descricao_cclass", ""))
        if not regras:
            return jsonify({"success": False, "error": "Informe regras válidas no formato descricao;cClass"}), 400

        sid = str(uuid.uuid4())
        in_zip_path = os.path.join(UPLOADS_DIR, f"lote_descricao_in_{sid}.zip")
        zf.save(in_zip_path)

        session["lote_descricao_session_id"] = sid
        _set_lote_descricao_status(sid, status="queued", done=False, processados=0, total=0, percentual=0)

        th = threading.Thread(target=_process_lote_descricao_async, args=(sid, in_zip_path, regras), daemon=True)
        th.start()

        return jsonify({"success": True, "status": "iniciado", "session_id": sid})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/lote-descricao/status")
def lote_descricao_status():
    sid = request.args.get("session_id") or session.get("lote_descricao_session_id")
    if not sid:
        return jsonify({"success": False, "error": "session_id é obrigatório"}), 400

    st = r_get_json(f"csv:status:{sid}")
    if not st:
        return jsonify({"success": True, "status": "nao_encontrado", "processados": 0, "total": 0, "percentual": 0, "done": True})

    return jsonify({"success": True, **st})


@app.route("/api/csv/baixar/<sid>")
def api_csv_baixar(sid):
    data = r_get_json(f"csv:lote:{sid}")
    if not data:
        return jsonify({"success": False, "error": "Sessão não encontrada"}), 404

    out_path = data.get("output_path")
    if not out_path or not os.path.exists(out_path):
        return jsonify({"success": False, "error": "Arquivo processado não encontrado"}), 404

    return send_file(
        out_path,
        as_attachment=True,
        download_name=f"lote_descricao_{sid}.zip",
        mimetype="application/zip",
    )


# =========================================================
# Dados exemplo
# =========================================================
def gerar_dados_exemplo():
    return {
        "emitente_nome": "NOVA TELECOM LTDA",
        "emitente_cnpj": "87.783.220/0017-80",
        "total_arquivos": 3,
        "total_geral": 185033.16,
        "total_geral_br": "R$ 185.033,16",
        "total_impostos": 8881.59,
        "total_impostos_br": "R$ 8.881,59",
        "labels": ["600601", "400401"],
        "valores": [184319.65, 713.51],
        "linhas": [],
        "itens_linhas": [],
        "impostos_linhas": [],
        "debug": {"total_xml": 3, "total_ok": 3, "total_falhas": 0, "primeiro_erro": None, "total_notas_processadas": 0},
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
