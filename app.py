from flask import Flask, render_template, request, jsonify, send_file, session, url_for
import os
import uuid
import json
from datetime import datetime
import zipfile
import io
import threading
import time

import pandas as pd
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

def parse_nfcom_xml(xml_bytes: bytes):
    root = etree.fromstring(xml_bytes)
    inf = root.xpath("//*[local-name()='infNFCom']")[0] if root.xpath("//*[local-name()='infNFCom']") else root

    emit = inf.xpath(".//*[local-name()='emit']")[0] if inf.xpath(".//*[local-name()='emit']") else None
    dest = inf.xpath(".//*[local-name()='dest']")[0] if inf.xpath(".//*[local-name()='dest']") else None

    nNF = x1(inf, ".//*[local-name()='nNF']/text()")
    serie = x1(inf, ".//*[local-name()='serie']/text()")
    cNF = x1(inf, ".//*[local-name()='cNF']/text()")
    dhEmi = x1(inf, ".//*[local-name()='dhEmi']/text()") or x1(inf, ".//*[local-name()='dEmi']/text()")

    emit_nome = x1(emit, ".//*[local-name()='xNome']/text()") if emit is not None else None
    emit_doc = x1(emit, ".//*[local-name()='CNPJ']/text()") if emit is not None else None

    dest_nome = x1(dest, ".//*[local-name()='xNome']/text()") if dest is not None else None
    dest_doc = x1(dest, ".//*[local-name()='CNPJ']/text()") if dest is not None else (x1(dest, ".//*[local-name()='CPF']/text()") if dest is not None else None)

    # Totais e retenções
    vProd = safe_float(x1(inf, ".//*[local-name()='vProd']/text()"))
    vNF = safe_float(x1(inf, ".//*[local-name()='vNF']/text()"))  # total a pagar em alguns layouts
    total_pagar = vNF if vNF else vProd

    # vRetTribTot
    ret = inf.xpath(".//*[local-name()='vRetTribTot']")[0] if inf.xpath(".//*[local-name()='vRetTribTot']") else None
    vRetPIS = safe_float(x1(ret, ".//*[local-name()='vRetPIS']/text()")) if ret is not None else 0.0
    vRetCofins = safe_float(x1(ret, ".//*[local-name()='vRetCofins']/text()")) if ret is not None else 0.0
    vRetCSLL = safe_float(x1(ret, ".//*[local-name()='vRetCSLL']/text()")) if ret is not None else 0.0
    vIRRF = safe_float(x1(ret, ".//*[local-name()='vIRRF']/text()")) if ret is not None else 0.0

    itens = []
    for det in inf.xpath(".//*[local-name()='det']"):
        prod = det.xpath(".//*[local-name()='prod']")[0] if det.xpath(".//*[local-name()='prod']") else det
        cClass = x1(prod, ".//*[local-name()='cClass']/text()") or ""
        cfop = x1(prod, ".//*[local-name()='CFOP']/text()") or ""
        cProd = x1(prod, ".//*[local-name()='cProd']/text()") or ""
        xProd = x1(prod, ".//*[local-name()='xProd']/text()") or ""
        qCom = safe_float(x1(prod, ".//*[local-name()='qCom']/text()"))
        vProd_i = safe_float(x1(prod, ".//*[local-name()='vProd']/text()"))
        itens.append({
            "cClass": cClass,
            "CFOP": cfop,
            "cProd": cProd,
            "xProd": xProd,
            "qCom": qCom,
            "vProd": vProd_i,
            "vProd_br": br_money(vProd_i),
        })

    return {
        "tipo": "NFCom",
        "nNF": nNF,
        "serie": serie,
        "cNF": cNF,
        "dhEmi": dhEmi,
        "dhEmi_fmt": br_date(dhEmi or ""),
        "emitente": {"xNome": emit_nome, "CNPJ": emit_doc},
        "destinatario": {"xNome": dest_nome, "doc": dest_doc},
        "itens": itens,
        "totais": {
            "vProd": vProd,
            "vProd_br": br_money(vProd),
            "vPagar": total_pagar,
            "vPagar_br": br_money(total_pagar),
        },
        "retencoes": {
            "vRetPIS": vRetPIS, "vRetPIS_br": br_money(vRetPIS),
            "vRetCofins": vRetCofins, "vRetCofins_br": br_money(vRetCofins),
            "vRetCSLL": vRetCSLL, "vRetCSLL_br": br_money(vRetCSLL),
            "vIRRF": vIRRF, "vIRRF_br": br_money(vIRRF),
            "total": (vRetPIS + vRetCofins + vRetCSLL + vIRRF),
            "total_br": br_money(vRetPIS + vRetCofins + vRetCSLL + vIRRF),
        }
    }

def parse_nfe_xml(xml_bytes: bytes):
    root = etree.fromstring(xml_bytes)
    inf = root.xpath("//*[local-name()='infNFe']")[0] if root.xpath("//*[local-name()='infNFe']") else root

    emit = inf.xpath(".//*[local-name()='emit']")[0] if inf.xpath(".//*[local-name()='emit']") else None
    dest = inf.xpath(".//*[local-name()='dest']")[0] if inf.xpath(".//*[local-name()='dest']") else None

    nNF = x1(inf, ".//*[local-name()='nNF']/text()")
    serie = x1(inf, ".//*[local-name()='serie']/text()")
    cNF = x1(inf, ".//*[local-name()='cNF']/text()")
    dhEmi = x1(inf, ".//*[local-name()='dhEmi']/text()") or x1(inf, ".//*[local-name()='dEmi']/text()")

    emit_nome = x1(emit, ".//*[local-name()='xNome']/text()") if emit is not None else None
    emit_doc = x1(emit, ".//*[local-name()='CNPJ']/text()") if emit is not None else None

    dest_nome = x1(dest, ".//*[local-name()='xNome']/text()") if dest is not None else None
    dest_doc = x1(dest, ".//*[local-name()='CNPJ']/text()") if dest is not None else (x1(dest, ".//*[local-name()='CPF']/text()") if dest is not None else None)

    itens = []
    total_vprod = 0.0
    for det in inf.xpath(".//*[local-name()='det']"):
        prod = det.xpath(".//*[local-name()='prod']")[0] if det.xpath(".//*[local-name()='prod']") else det
        cfop = x1(prod, ".//*[local-name()='CFOP']/text()") or ""
        cProd = x1(prod, ".//*[local-name()='cProd']/text()") or ""
        xProd = x1(prod, ".//*[local-name()='xProd']/text()") or ""
        qCom = safe_float(x1(prod, ".//*[local-name()='qCom']/text()"))
        vProd_i = safe_float(x1(prod, ".//*[local-name()='vProd']/text()"))
        total_vprod += vProd_i
        itens.append({
            "CFOP": cfop,
            "cProd": cProd,
            "xProd": xProd,
            "qCom": qCom,
            "vProd": vProd_i,
            "vProd_br": br_money(vProd_i),
        })

    return {
        "tipo": "NFe",
        "nNF": nNF,
        "serie": serie,
        "cNF": cNF,
        "dhEmi": dhEmi,
        "dhEmi_fmt": br_date(dhEmi or ""),
        "emitente": {"xNome": emit_nome, "CNPJ": emit_doc},
        "destinatario": {"xNome": dest_nome, "doc": dest_doc},
        "itens": itens,
        "totais": {"vProd": total_vprod, "vProd_br": br_money(total_vprod)}
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
    return render_template("resumo_resultado.html", data=data)

@app.route("/csv")
def csv_page():
    return render_template("csv.html")

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

def _set_status(sid, **kw):
    st = r_get_json(f"resumo:status:{sid}") or {"session_id": sid}
    st.update(kw)
    r_setex(f"resumo:status:{sid}", SUMMARY_TTL, st)

def _process_zip_resumo(sid: str, zip_path: str):
    try:
        _set_status(sid, status="running", progress=0, error=None, done=False, started_at=datetime.now().isoformat())
        with zipfile.ZipFile(zip_path, "r") as z:
            names = [n for n in z.namelist() if n.lower().endswith(".xml")]
            total = len(names)
            if total == 0:
                raise Exception("Nenhum XML encontrado no ZIP")

            # Agregadores
            by_cclass = {}      # cClass -> {desc, qtd, total, cfops{cfop->total}}
            by_item = {}        # (cProd,cClass,desc) -> {qtd,total}
            impostos = {"PIS Ret.":0.0,"COFINS Ret.":0.0,"CSLL Ret.":0.0,"IRRF Ret.":0.0}
            emit_nome = None
            emit_cnpj = None
            total_geral = 0.0
            ok = 0
            falhas = 0
            primeiro_erro = None

            # Limites de detalhes (notas relacionadas) — em ZIP grande, não explode
            for i, name in enumerate(names, start=1):
                try:
                    xml_bytes = z.read(name)
                    d = parse_xml_any(xml_bytes)
                    if "error" in d:
                        raise Exception(d["error"])

                    if not emit_nome:
                        emit_nome = (d.get("emitente") or {}).get("xNome")
                        emit_cnpj = (d.get("emitente") or {}).get("CNPJ")

                    # Totais por itens
                    itens = d.get("itens") or []
                    for it in itens:
                        cClass = (it.get("cClass") or "").strip()
                        cfop = (it.get("CFOP") or "").strip()
                        cProd = (it.get("cProd") or "").strip()
                        xProd = (it.get("xProd") or "").strip()
                        v = float(it.get("vProd") or 0.0)
                        total_geral += v

                        # cClass
                        if cClass:
                            rec = by_cclass.setdefault(cClass, {"cClass": cClass, "desc": xProd or "", "qtd_itens": 0, "v_total": 0.0, "cfops": {}})
                            if not rec["desc"] and xProd:
                                rec["desc"] = xProd
                            rec["qtd_itens"] += 1
                            rec["v_total"] += v
                            if cfop:
                                rec["cfops"][cfop] = rec["cfops"].get(cfop, 0.0) + v

                        # item
                        key = (cProd, cClass, xProd)
                        if cProd:
                            ir = by_item.setdefault(key, {"item": cProd, "desc": xProd, "cClass": cClass, "qtd_itens": 0, "v_total": 0.0})
                            ir["qtd_itens"] += 1
                            ir["v_total"] += v

                    # Retenções (NFCom)
                    rtt = d.get("retencoes") or {}
                    impostos["PIS Ret."] += float(rtt.get("vRetPIS") or 0.0)
                    impostos["COFINS Ret."] += float(rtt.get("vRetCofins") or 0.0)
                    impostos["CSLL Ret."] += float(rtt.get("vRetCSLL") or 0.0)
                    impostos["IRRF Ret."] += float(rtt.get("vIRRF") or 0.0)

                    ok += 1
                except Exception as e:
                    falhas += 1
                    if not primeiro_erro:
                        primeiro_erro = f"{name}: {e}"

                if i % 25 == 0 or i == total:
                    _set_status(sid, progress=int((i/total)*100), processed=i, total=total)

            # Monta payload compacto
            linhas = []
            for c, rec in by_cclass.items():
                cfops_list = []
                for cfop, v in rec["cfops"].items():
                    cfops_list.append({"cfop": cfop, "v_total": v, "v_total_br": br_money(v), "notas": []})
                linhas.append({
                    "cClass": c,
                    "desc": rec["desc"] or "",
                    "qtd_itens": rec["qtd_itens"],
                    "v_total": rec["v_total"],
                    "v_total_br": br_money(rec["v_total"]),
                    "pct": 0.0,
                    "pct_br": "",
                    "cfops": cfops_list[:DETAILS_LIMIT],
                })

            # porcentagens
            total = max(total_geral, 1e-9)
            for l in linhas:
                pct = (l["v_total"] / total) * 100.0
                l["pct"] = pct
                l["pct_br"] = f"{pct:.2f}%".replace(".", ",")

            # labels top 12
            top = sorted(linhas, key=lambda x: x["v_total"], reverse=True)[:12]
            labels = [x["cClass"] for x in top]
            valores = [x["v_total"] for x in top]

            itens_linhas = list(by_item.values())
            for it in itens_linhas:
                it["v_total_br"] = br_money(it["v_total"])
            itens_linhas = sorted(itens_linhas, key=lambda x: x["v_total"], reverse=True)[:DETAILS_LIMIT]

            # impostos_linhas
            total = sum(impostos.values()) or 0.0
            impostos_linhas = []
            for tipo, v in impostos.items():
                if v <= 0:
                    continue
                pct = (v / max(R, 1e-9)) * 100.0 if (R:=R) else 0.0
            # compute properly
            R = sum(impostos.values()) or 0.0
            for tipo, v in impostos.items():
                if v <= 0:
                    continue
                pct = (v / max(R, 1e-9)) * 100.0
                impostos_linhas.append({
                    "tipo": tipo.replace(" Ret.", " Retido") if "Ret." in tipo else tipo,
                    "qtd_notas": ok,
                    "v_total": v,
                    "v_total_br": br_money(v),
                    "pct": pct,
                    "pct_br": f"{pct:.2f}%".replace(".", ","),
                    "notas": [],
                })

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
                "debug": {"total_xml": len(names), "total_ok": ok, "total_falhas": falhas, "primeiro_erro": primeiro_erro},
            }

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
# CSV export (mantém simples)
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
                d = parse_xml_any(zip_in.read(name))
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
                    "emitente_doc": (d.get("emitente") or {}).get("CNPJ"),
                    "dest_nome": (d.get("destinatario") or {}).get("xNome"),
                    "dest_doc": (d.get("destinatario") or {}).get("doc"),
                    "total_vProd": (d.get("totais") or {}).get("vProd"),
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
        "debug": {"total_xml": 3, "total_ok": 3, "total_falhas": 0, "primeiro_erro": None},
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
