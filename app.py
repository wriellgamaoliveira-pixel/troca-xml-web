from flask import Flask, render_template, request, jsonify, send_file, session, url_for
import os
import uuid
import json
from datetime import datetime
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-troca-xml-web")

app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

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


# =========================================================
# XML: parse básico (NFe / NFCom)
# =========================================================
def _text(node):
    return node.text.strip() if node is not None and node.text else None

def detect_root(xml_text: str):
    try:
        return ET.fromstring(xml_text)
    except Exception:
        return None

def parse_nfe(root: ET.Element):
    ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
    inf = root.find(".//nfe:infNFe", ns)

    emit = inf.find(".//nfe:emit", ns) if inf is not None else None
    dest = inf.find(".//nfe:dest", ns) if inf is not None else None

    dados = {
        "tipo": "NFe",
        "nNF": _text(inf.find(".//nfe:nNF", ns)) if inf is not None else None,
        "serie": _text(inf.find(".//nfe:serie", ns)) if inf is not None else None,
        "cNF": _text(inf.find(".//nfe:cNF", ns)) if inf is not None else None,
        "dhEmi": _text(inf.find(".//nfe:dhEmi", ns)) if inf is not None else None,
        "dhEmi_fmt": br_date(_text(inf.find(".//nfe:dhEmi", ns)) if inf is not None else ""),
        "emitente": {
            "xNome": _text(emit.find("nfe:xNome", ns)) if emit is not None else None,
            "CNPJ": _text(emit.find("nfe:CNPJ", ns)) if emit is not None else None,
        },
        "destinatario": {
            "xNome": _text(dest.find("nfe:xNome", ns)) if dest is not None else None,
            "doc": _text(dest.find("nfe:CNPJ", ns)) if dest is not None else _text(dest.find("nfe:CPF", ns)) if dest is not None else None,
        },
        "itens": [],
        "totais": {},
    }

    dets = inf.findall(".//nfe:det", ns) if inf is not None else []
    total = 0.0
    for det in dets:
        prod = det.find("nfe:prod", ns)
        if prod is None:
            continue
        vprod = float(_text(prod.find("nfe:vProd", ns)) or 0)
        total += vprod
        dados["itens"].append(
            {
                "cProd": _text(prod.find("nfe:cProd", ns)),
                "xProd": _text(prod.find("nfe:xProd", ns)),
                "NCM": _text(prod.find("nfe:NCM", ns)),
                "CFOP": _text(prod.find("nfe:CFOP", ns)),
                "qCom": float(_text(prod.find("nfe:qCom", ns)) or 0),
                "vProd": vprod,
                "vProd_br": br_money(vprod),
            }
        )

    dados["totais"] = {"vProd": total, "vProd_br": br_money(total)}
    return dados

def parse_nfcom(root: ET.Element):
    ns = {"nfcom": "http://www.portalfiscal.inf.br/nfcom"}
    inf = root.find(".//nfcom:infNFCom", ns)

    emit = inf.find(".//nfcom:emit", ns) if inf is not None else None
    dest = inf.find(".//nfcom:dest", ns) if inf is not None else None

    dados = {
        "tipo": "NFCom",
        "nNF": _text(inf.find(".//nfcom:nNF", ns)) if inf is not None else None,
        "serie": _text(inf.find(".//nfcom:serie", ns)) if inf is not None else None,
        "cNF": _text(inf.find(".//nfcom:cNF", ns)) if inf is not None else None,
        "dhEmi": _text(inf.find(".//nfcom:dhEmi", ns)) if inf is not None else None,
        "dhEmi_fmt": br_date(_text(inf.find(".//nfcom:dhEmi", ns)) if inf is not None else ""),
        "emitente": {
            "xNome": _text(emit.find("nfcom:xNome", ns)) if emit is not None else None,
            "CNPJ": _text(emit.find("nfcom:CNPJ", ns)) if emit is not None else None,
        },
        "destinatario": {
            "xNome": _text(dest.find("nfcom:xNome", ns)) if dest is not None else None,
            "doc": _text(dest.find("nfcom:CNPJ", ns)) if dest is not None else _text(dest.find("nfcom:CPF", ns)) if dest is not None else None,
        },
        "itens": [],
        "totais": {},
    }

    total = 0.0
    dets = inf.findall(".//nfcom:det", ns) if inf is not None else []
    for det in dets:
        prod = det.find(".//nfcom:prod", ns)
        if prod is None:
            continue
        vprod = float(_text(prod.find("nfcom:vProd", ns)) or 0)
        total += vprod
        dados["itens"].append(
            {
                "cProd": _text(prod.find("nfcom:cProd", ns)),
                "xProd": _text(prod.find("nfcom:xProd", ns)),
                "cClass": _text(prod.find("nfcom:cClass", ns)),
                "CFOP": _text(prod.find("nfcom:CFOP", ns)),
                "qCom": float(_text(prod.find("nfcom:qCom", ns)) or 0),
                "vProd": vprod,
                "vProd_br": br_money(vprod),
            }
        )

    dados["totais"] = {"vProd": total, "vProd_br": br_money(total)}
    return dados

def parse_xml(xml_text: str):
    root = detect_root(xml_text)
    if root is None:
        return {"error": "XML inválido"}

    tag = root.tag.lower()
    if tag.endswith("nfe"):
        return parse_nfe(root)
    if tag.endswith("nfcom"):
        return parse_nfcom(root)

    # fallback: nfeProc / nfcomProc
    nfe = root.find(".//{http://www.portalfiscal.inf.br/nfe}NFe")
    if nfe is not None:
        return parse_nfe(nfe)
    nfcom = root.find(".//{http://www.portalfiscal.inf.br/nfcom}NFCom")
    if nfcom is not None:
        return parse_nfcom(nfcom)

    return {"error": "Tipo XML não suportado (apenas NFe/NFCom)"}


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
    dados = session.get("resumo_data") or gerar_dados_exemplo()
    return render_template("resumo_resultado.html", data=dados)

@app.route("/csv")
def csv_page():
    return render_template("csv.html")


# =========================================================
# API - Resumo
# =========================================================
@app.route("/api/resumo/upload", methods=["POST"])
def api_resumo_upload():
    """
    Recebe um .zip com XMLs e gera o resumo real (cClass / Itens / Impostos retidos).
    Salva o resultado na sessão para exibir em /resumo/resultado.
    """
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

        dados = build_resumo_from_zip(zip_path)

        session["resumo_session_id"] = session_id
        session["resumo_data"] = dados

        return jsonify({"success": True, "session_id": session_id, "redirect": url_for("resumo_resultado_page")})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================================================
# API - Nota única


# =========================================================
@app.route("/api/nota/visualizar", methods=["POST"])
def api_nota_visualizar():
    try:
        if "xml_nota" not in request.files:
            return jsonify({"success": False, "error": "Envie o arquivo no campo xml_nota"}), 400
        f = request.files["xml_nota"]
        xml_text = f.read().decode("utf-8", errors="replace")
        dados = parse_xml(xml_text)
        if "error" in dados:
            return jsonify({"success": False, "error": dados["error"]}), 400
        return jsonify({"success": True, "data": dados})
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
# API - Lote (dummy ZIP -> ZIP)
# =========================================================
@app.route("/api/lote/processar", methods=["POST"])
def api_lote_processar():
    """
    Processa um ZIP de XMLs, aplicando regras de cClass -> CFOP:
      - Se existir <CFOP> no mesmo <prod> do <cClass>, troca pelo CFOP definido
      - Se NÃO existir <CFOP>, cria a tag <CFOP> ao lado do <cClass>
    Regras (textarea):
      uma por linha: cClass;CFOP
    """
    try:
        if "zip_xmls" not in request.files:
            return jsonify({"success": False, "error": "Envie o ZIP no campo zip_xmls"}), 400

        zf = request.files["zip_xmls"]
        if not zf.filename.lower().endswith(".zip"):
            return jsonify({"success": False, "error": "Envie um arquivo .zip"}), 400

        session_id = request.form.get("session_id") or str(uuid.uuid4())
        regras_texto = request.form.get("regras_cclass_cfop", "") or ""

        # ---- Parse regras ----
        def norm(s: str) -> str:
            s = (s or "").strip()
            if s.isdigit():
                s2 = s.lstrip("0")
                return s2 if s2 else "0"
            return s

        regras = {}
        for line in regras_texto.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ";" not in line:
                continue
            cclass, cfop = [p.strip() for p in line.split(";", 1)]
            if not cclass or not cfop:
                continue
            regras[cclass] = cfop
            regras[norm(cclass)] = cfop  # permite casar sem zeros à esquerda

        if not regras:
            return jsonify({"success": False, "error": "Informe pelo menos 1 regra no formato cClass;CFOP"}), 400

        # ---- Processa ZIP ----
        import io
        from lxml import etree

        zip_in = zipfile.ZipFile(io.BytesIO(zf.read()), "r")
        out_buffer = io.BytesIO()
        relatorio = []

        with zipfile.ZipFile(out_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zip_out:
            for name in zip_in.namelist():
                data = zip_in.read(name)
                if not name.lower().endswith(".xml"):
                    zip_out.writestr(name, data)
                    continue

                try:
                    parser = etree.XMLParser(remove_blank_text=False, recover=True, huge_tree=True)
                    root = etree.fromstring(data, parser=parser)

                    # Encontra todos os <prod> que tenham <cClass>
                    prods = root.xpath("//*[local-name()='prod' and ./*[local-name()='cClass']]")
                    alteracoes = 0

                    for prod in prods:
                        cclass_el = prod.xpath("./*[local-name()='cClass']")
                        if not cclass_el:
                            continue
                        cclass_val = (cclass_el[0].text or "").strip()
                        alvo = regras.get(cclass_val) or regras.get(norm(cclass_val))
                        if not alvo:
                            continue

                        cfop_el = prod.xpath("./*[local-name()='CFOP']")
                        if cfop_el:
                            if (cfop_el[0].text or "").strip() != alvo:
                                cfop_el[0].text = alvo
                                alteracoes += 1
                        else:
                            # cria CFOP logo após cClass (mantém ordem)
                            new_el = etree.Element("CFOP")
                            new_el.text = alvo
                            idx = prod.index(cclass_el[0])
                            prod.insert(idx + 1, new_el)
                            alteracoes += 1

                    xml_bytes = etree.tostring(root, xml_declaration=True, encoding="utf-8", pretty_print=True)
                    zip_out.writestr(name, xml_bytes)

                    relatorio.append(f"{name}: {alteracoes} alteração(ões)")
                except Exception as e:
                    # se falhar, não perde o arquivo original
                    zip_out.writestr(name, data)
                    relatorio.append(f"{name}: ERRO ao processar ({str(e)})")

            zip_out.writestr("relatorio_processamento.txt", "\n".join(relatorio))

        out_buffer.seek(0)
        zip_path = os.path.join(TEMP_DIR, f"{session_id}_processado.zip")
        with open(zip_path, "wb") as f:
            f.write(out_buffer.getvalue())

        return jsonify({"success": True, "session_id": session_id, "download_url": f"/download/{session_id}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================================================
# API - CSV

# (gera CSV simples do ZIP)
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
                xml_text = zip_in.read(name).decode("utf-8", errors="replace")
                d = parse_xml(xml_text)
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



# =========================================================
# Resumo real (ZIP -> Estrutura para resumo_resultado.html)
# =========================================================
from lxml import etree

def _xpath_text(node, xp: str):
    try:
        r = node.xpath(xp)
        if not r:
            return None
        if isinstance(r[0], etree._Element):
            return (r[0].text or "").strip() if r[0].text else None
        return str(r[0]).strip()
    except Exception:
        return None

def parse_nfcom_document(xml_bytes: bytes):
    """
    Parse NFCom (com ou sem namespace) e retorna:
    - header (emit/dest/ide)
    - itens (lista)
    - totais (dict)
    - retencoes (somatório por nota)
    """
    parser = etree.XMLParser(remove_blank_text=False, recover=True, huge_tree=True)
    root = etree.fromstring(xml_bytes, parser=parser)

    infs = root.xpath("//*[local-name()='infNFCom']")
    if not infs:
        raise ValueError("XML não parece ser NFCom (infNFCom não encontrado)")
    inf = infs[0]

    ide = inf.xpath("./*[local-name()='ide']")
    ide = ide[0] if ide else inf

    emit = inf.xpath("./*[local-name()='emit']")
    emit = emit[0] if emit else inf
    dest = inf.xpath("./*[local-name()='dest']")
    dest = dest[0] if dest else inf

    nNF = _xpath_text(ide, ".//*[local-name()='nNF']")
    serie = _xpath_text(ide, ".//*[local-name()='serie']")
    cNF = _xpath_text(ide, ".//*[local-name()='cNF']")
    dhEmi = _xpath_text(ide, ".//*[local-name()='dhEmi']")
    dhEmi_fmt = br_date(dhEmi or "")

    emit_nome = _xpath_text(emit, ".//*[local-name()='xNome']")
    emit_doc = _xpath_text(emit, ".//*[local-name()='CNPJ']") or _xpath_text(emit, ".//*[local-name()='CPF']")
    dest_nome = _xpath_text(dest, ".//*[local-name()='xNome']")
    dest_doc = _xpath_text(dest, ".//*[local-name()='CNPJ']") or _xpath_text(dest, ".//*[local-name()='CPF']")

    # itens: det/prod
    itens = []
    dets = inf.xpath(".//*[local-name()='det']")
    for det in dets:
        prod = det.xpath("./*[local-name()='prod']")
        prod = prod[0] if prod else None
        if prod is None:
            continue

        cProd = _xpath_text(prod, "./*[local-name()='cProd']")
        xProd = _xpath_text(prod, "./*[local-name()='xProd']")
        cClass = _xpath_text(prod, "./*[local-name()='cClass']")
        CFOP = _xpath_text(prod, "./*[local-name()='CFOP']")
        unidade = _xpath_text(prod, "./*[local-name()='uMed']")
        qtd = _xpath_text(prod, "./*[local-name()='qFaturada']")
        v_un = _xpath_text(prod, "./*[local-name()='vItem']")
        v_prod = _xpath_text(prod, "./*[local-name()='vProd']") or v_un

        try:
            v_prod_f = float(v_prod or 0)
        except Exception:
            v_prod_f = 0.0

        # impostos do item (opcional)
        imp = det.xpath("./*[local-name()='imposto']")
        imp = imp[0] if imp else det

        pis_cof = None
        # alguns layouts usam vPIS / vCOFINS em imposto total, aqui só exibe se existir no item
        vpis = _xpath_text(imp, ".//*[local-name()='PIS']//*[local-name()='vPIS']") or _xpath_text(imp, ".//*[local-name()='vPIS']")
        vcof = _xpath_text(imp, ".//*[local-name()='COFINS']//*[local-name()='vCOFINS']") or _xpath_text(imp, ".//*[local-name()='vCOFINS']")
        if vpis or vcof:
            try:
                pis_cof = br_money(float(vpis or 0) + float(vcof or 0))
            except Exception:
                pis_cof = None

        bc_icms = _xpath_text(imp, ".//*[local-name()='ICMS']//*[local-name()='vBC']") or _xpath_text(imp, ".//*[local-name()='vBC']")
        aliq_icms = _xpath_text(imp, ".//*[local-name()='ICMS']//*[local-name()='pICMS']") or _xpath_text(imp, ".//*[local-name()='pICMS']")
        v_icms = _xpath_text(imp, ".//*[local-name()='ICMS']//*[local-name()='vICMS']") or _xpath_text(imp, ".//*[local-name()='vICMS']")

        itens.append({
            "cProd": cProd,
            "xProd": xProd,
            "cClass": cClass,
            "CFOP": CFOP,
            "unidade": unidade,
            "qtd": qtd,
            "v_un": v_un,
            "v_prod": v_prod_f,
            "v_prod_br": br_money(v_prod_f),
            "pis_cofins": pis_cof,
            "bc_icms": br_money(float(bc_icms)) if (bc_icms and str(bc_icms).replace(".","",1).isdigit()) else (bc_icms or ""),
            "aliq_icms": (f"{aliq_icms}%" if aliq_icms else ""),
            "icms": br_money(float(v_icms)) if (v_icms and str(v_icms).replace(".","",1).isdigit()) else (v_icms or ""),
        })

    # totais (da nota)
    total_node = inf.xpath(".//*[local-name()='total']")
    total_node = total_node[0] if total_node else inf

    def fnum(x):
        try:
            return float(x or 0)
        except Exception:
            return 0.0

    totais = {
        "vNF": fnum(_xpath_text(total_node, ".//*[local-name()='vNF']")),
        "vProd": fnum(_xpath_text(total_node, ".//*[local-name()='vProd']")),
        "vBC": fnum(_xpath_text(total_node, ".//*[local-name()='ICMSTot']//*[local-name()='vBC']") or _xpath_text(total_node, ".//*[local-name()='vBC']")),
        "vICMS": fnum(_xpath_text(total_node, ".//*[local-name()='ICMSTot']//*[local-name()='vICMS']") or _xpath_text(total_node, ".//*[local-name()='vICMS']")),
        "vPIS": fnum(_xpath_text(total_node, ".//*[local-name()='vPIS']")),
        "vCOFINS": fnum(_xpath_text(total_node, ".//*[local-name()='vCOFINS']")),
        "vFUST": fnum(_xpath_text(total_node, ".//*[local-name()='vFUST']")),
        "vFUNTTEL": fnum(_xpath_text(total_node, ".//*[local-name()='vFUNTTEL']")),
        "vRetTribTot": fnum(_xpath_text(total_node, ".//*[local-name()='vRetTribTot']")),
        "vDesc": fnum(_xpath_text(total_node, ".//*[local-name()='vDesc']")),
        "vOutro": fnum(_xpath_text(total_node, ".//*[local-name()='vOutro']")),
    }

    # retenções por nota: somatório de det/imposto/retTrib
    ret_pis = 0.0
    ret_cof = 0.0
    ret_csll = 0.0
    ret_irrf = 0.0

    for det in dets:
        ret = det.xpath(".//*[local-name()='retTrib']")
        if not ret:
            continue
        ret = ret[0]
        ret_pis += fnum(_xpath_text(ret, ".//*[local-name()='vRetPIS']"))
        ret_cof += fnum(_xpath_text(ret, ".//*[local-name()='vRetCofins']"))
        ret_csll += fnum(_xpath_text(ret, ".//*[local-name()='vRetCSLL']"))
        ret_irrf += fnum(_xpath_text(ret, ".//*[local-name()='vIRRF']"))

    header = {
        "nNF": nNF,
        "serie": serie,
        "cNF": cNF,
        "dhEmi_fmt": dhEmi_fmt,
        "emitente_nome": emit_nome,
        "emitente_doc": emit_doc,
        "destinatario_nome": dest_nome,
        "destinatario_doc": dest_doc,
    }

    retencoes = {
        "pis": ret_pis,
        "cofins": ret_cof,
        "csll": ret_csll,
        "irrf": ret_irrf,
        "total": ret_pis + ret_cof + ret_csll + ret_irrf,
    }

    return header, itens, totais, retencoes


def build_resumo_from_zip(zip_path: str):
    """
    Lê um ZIP, processa NFComs e monta:
      - linhas (por cClass) com CFOPs e notas
      - itens_linhas (por cProd/xProd)
      - impostos_linhas (retenções agregadas)
    """
    debug = {"total_xml": 0, "total_ok": 0, "total_falhas": 0, "primeiro_erro": None}

    # agregações
    cclass_map = {}   # cClass -> {desc, qtd_itens, v_total, cfops{cfop:{v_total, notas[]}}}
    item_map = {}     # (cProd,xProd,cClass) -> { ... }
    impostos_por_nota = []  # lista de notas com retenções

    emit_nome = None
    emit_doc = None

    total_geral = 0.0

    with zipfile.ZipFile(zip_path, "r") as z:
        names = [n for n in z.namelist() if n.lower().endswith(".xml")]
        debug["total_xml"] = len(names)

        for name in names:
            try:
                xml_bytes = z.read(name)
                header, itens, totais, ret = parse_nfcom_document(xml_bytes)

                emit_nome = emit_nome or header.get("emitente_nome")
                emit_doc = emit_doc or header.get("emitente_doc")

                # total por nota: preferir vNF, senão somatório dos itens
                vnota = totais.get("vNF") or 0.0
                if not vnota:
                    vnota = sum(float(i.get("v_prod") or 0) for i in itens)
                total_geral += float(vnota or 0)

                # retenções por nota
                if ret.get("total", 0) > 0:
                    impostos_por_nota.append({
                        "nNF": header.get("nNF"),
                        "cNF": header.get("cNF"),
                        "emitente": header.get("emitente_nome"),
                        "destinatario": header.get("destinatario_nome"),
                        "emissao": header.get("dhEmi_fmt"),
                        "pis_ret": br_money(ret["pis"]),
                        "cofins_ret": br_money(ret["cofins"]),
                        "csll_ret": br_money(ret["csll"]),
                        "irrf_ret": br_money(ret["irrf"]),
                        "total_retido": br_money(ret["total"]),
                        "_pis": ret["pis"], "_cof": ret["cofins"], "_csll": ret["csll"], "_irrf": ret["irrf"], "_tot": ret["total"],
                    })

                # itens
                for it in itens:
                    cClass = (it.get("cClass") or "").strip()
                    cfop = (it.get("CFOP") or "").strip()
                    desc = (it.get("xProd") or "").strip()
                    cProd = (it.get("cProd") or "").strip()
                    v = float(it.get("v_prod") or 0)
                    total_geral_it = v

                    # por cClass
                    ent = cclass_map.setdefault(cClass or "—", {
                        "cClass": cClass or "—",
                        "desc": desc or "",
                        "qtd_itens": 0,
                        "v_total": 0.0,
                        "cfops": {},
                    })
                    ent["qtd_itens"] += 1
                    ent["v_total"] += v
                    if desc and not ent.get("desc"):
                        ent["desc"] = desc

                    if cfop:
                        cf = ent["cfops"].setdefault(cfop, {"cfop": cfop, "v_total": 0.0, "notas": []})
                        cf["v_total"] += v
                        cf["notas"].append({
                            "nNF": header.get("nNF"),
                            "cNF": header.get("cNF"),
                            "xNome": header.get("emitente_nome"),
                            "xContato": header.get("destinatario_nome"),
                            "dhEmi_fmt": header.get("dhEmi_fmt"),
                            "vProd_br": br_money(v),
                        })

                    # por item
                    key = (cProd or "—", desc or "—", cClass or "—")
                    itent = item_map.setdefault(key, {
                        "item": cProd or "—",
                        "desc": desc or "—",
                        "cClass": cClass or "—",
                        "qtd_itens": 0,
                        "v_total": 0.0,
                        "notas": [],
                    })
                    itent["qtd_itens"] += 1
                    itent["v_total"] += v
                    itent["notas"].append({
                        "nNF": header.get("nNF"),
                        "cNF": header.get("cNF"),
                        "xNome": header.get("emitente_nome"),
                        "xContato": header.get("destinatario_nome"),
                        "dhEmi_fmt": header.get("dhEmi_fmt"),
                        "vProd_br": br_money(v),
                    })

                debug["total_ok"] += 1
            except Exception as e:
                debug["total_falhas"] += 1
                if not debug["primeiro_erro"]:
                    debug["primeiro_erro"] = f"{name}: {str(e)}"

    # montar linhas cClass
    linhas = []
    for cClass, ent in cclass_map.items():
        cfops_list = []
        for cfop, cf in ent["cfops"].items():
            cfops_list.append({
                "cfop": cfop,
                "v_total": cf["v_total"],
                "v_total_br": br_money(cf["v_total"]),
                "notas": cf["notas"],
            })
        # ordenar cfops por maior valor
        cfops_list.sort(key=lambda x: x["v_total"], reverse=True)

        linhas.append({
            "cClass": ent["cClass"],
            "desc": ent.get("desc") or "",
            "qtd_itens": ent["qtd_itens"],
            "v_total": ent["v_total"],
            "v_total_br": br_money(ent["v_total"]),
            "pct": 0.0,
            "pct_br": "0,00%",
            "cfops": cfops_list,
        })

    # percentuais e top 12
    linhas.sort(key=lambda x: x["v_total"], reverse=True)
    for l in linhas:
        pct = (l["v_total"] / total_geral * 100.0) if total_geral else 0.0
        l["pct"] = pct
        l["pct_br"] = f"{pct:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")

    labels = [l["cClass"] for l in linhas[:12]]
    valores = [l["v_total"] for l in linhas[:12]]

    # itens_linhas
    itens_linhas = list(item_map.values())
    itens_linhas.sort(key=lambda x: x["v_total"], reverse=True)
    for it in itens_linhas:
        pct = (it["v_total"] / total_geral * 100.0) if total_geral else 0.0
        it["v_total_br"] = br_money(it["v_total"])
        it["pct"] = pct
        it["pct_br"] = f"{pct:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")

    # impostos_linhas (retenções)
    total_ret = sum(n["_tot"] for n in impostos_por_nota) if impostos_por_nota else 0.0
    impostos_linhas = []

    def add_tax(tipo, key_field, display_field):
        tot = sum(n[key_field] for n in impostos_por_nota)
        if tot <= 0:
            return
        notas = []
        for n in impostos_por_nota:
            if n[key_field] <= 0:
                continue
            notas.append({
                "nNF": n["nNF"],
                "cNF": n["cNF"],
                "emitente": n["emitente"],
                "destinatario": n["destinatario"],
                "emissao": n["emissao"],
                "pis_ret": n["pis_ret"],
                "cofins_ret": n["cofins_ret"],
                "csll_ret": n["csll_ret"],
                "irrf_ret": n["irrf_ret"],
                "total_retido": n["total_retido"],
            })
        pct = (tot / total_ret * 100.0) if total_ret else 0.0
        impostos_linhas.append({
            "tipo": tipo,
            "qtd_notas": len(notas),
            "v_total": tot,
            "v_total_br": br_money(tot),
            "pct": pct,
            "pct_br": f"{pct:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."),
            "notas": notas,
        })

    add_tax("PIS Retido", "_pis", "pis_ret")
    add_tax("COFINS Retido", "_cof", "cofins_ret")
    add_tax("CSLL Retido", "_csll", "csll_ret")
    add_tax("IRRF Retido", "_irrf", "irrf_ret")

    # ordenar impostos por maior valor
    impostos_linhas.sort(key=lambda x: x["v_total"], reverse=True)

    return {
        "emitente_nome": emit_nome or "",
        "emitente_cnpj": emit_doc or "",
        "total_arquivos": debug["total_ok"],
        "total_geral": total_geral,
        "total_geral_br": br_money(total_geral),
        "total_impostos": total_ret,
        "total_impostos_br": br_money(total_ret),
        "labels": labels,
        "valores": valores,
        "linhas": linhas,
        "itens_linhas": itens_linhas,
        "impostos_linhas": impostos_linhas,
        "debug": debug,
    }

# =========================================================
# Dados exemplo (layout novo)
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
        "linhas": [
            {
                "cClass": "600601",
                "desc": "CONCENTRADOR SCI 50 MBPS",
                "qtd_itens": 56,
                "v_total": 184319.65,
                "v_total_br": "R$ 184.319,65",
                "pct": 99.61,
                "pct_br": "99,61%",
                "cfops": [
                    {
                        "cfop": "5307",
                        "v_total_br": "R$ 184.319,65",
                        "notas": [
                            {
                                "nNF": "10841",
                                "cNF": "730003",
                                "xNome": "NOVA TELECOM LTDA",
                                "xContato": "AGENCIA DE REGULACAO, CONTROLE E FISCALIZACAO DE SERVICOS PU",
                                "dhEmi_fmt": "04/12/2025",
                                "vProd_br": "R$ 713,51",
                            }
                        ],
                    }
                ],
            },
            {
                "cClass": "400401",
                "desc": "GOV SCM 40 MBPS",
                "qtd_itens": 1,
                "v_total": 713.51,
                "v_total_br": "R$ 713,51",
                "pct": 0.39,
                "pct_br": "0,39%",
                "cfops": [],
            },
        ],
        "itens_linhas": [
            {
                "item": "165",
                "desc": "GOV SCI 10 MBPS",
                "cClass": "600601",
                "qtd_itens": 37,
                "v_total": 57248.25,
                "v_total_br": "R$ 57.248,25",
                "pct": 30.94,
                "pct_br": "30,94%",
                "notas": [
                    {
                        "nNF": "10841",
                        "cNF": "730003",
                        "xNome": "NOVA TELECOM LTDA",
                        "xContato": "AGENCIA DE REGULACAO, CONTROLE E FISCALIZACAO DE SERVICOS PU",
                        "dhEmi_fmt": "04/12/2025",
                        "vProd_br": "R$ 713,51",
                    }
                ],
            },
            {
                "item": "158",
                "desc": "GOV SCM 40 MBPS",
                "cClass": "400401",
                "qtd_itens": 1,
                "v_total": 713.51,
                "v_total_br": "R$ 713,51",
                "pct": 0.39,
                "pct_br": "0,39%",
                "notas": [
                    {
                        "nNF": "10841",
                        "cNF": "730003",
                        "xNome": "NOVA TELECOM LTDA",
                        "xContato": "AGENCIA DE REGULACAO, CONTROLE E FISCALIZACAO DE SERVICOS PU",
                        "dhEmi_fmt": "04/12/2025",
                        "vProd_br": "R$ 713,51",
                    }
                ],
            },
        ],
        "impostos_linhas": [
            {
                "tipo": "IRRF Retido",
                "qtd_notas": 3,
                "v_total": 8881.59,
                "v_total_br": "R$ 8.881,59",
                "pct": 100.00,
                "pct_br": "100,00%",
                "notas": [
                    {
                        "nNF": "10907",
                        "cNF": "336482",
                        "emitente": "NOVA TELECOM LTDA",
                        "destinatario": "AGENCIA DE DEFESA AGROPECUARIA DO ESTADO DO TOCANTINS",
                        "emissao": "05/12/2025",
                        "pis_ret": "R$ 0,00",
                        "cofins_ret": "R$ 0,00",
                        "csll_ret": "R$ 0,00",
                        "irrf_ret": "R$ 6.927,36",
                        "total_retido": "R$ 6.927,36",
                    },
                    {
                        "nNF": "10896",
                        "cNF": "212182",
                        "emitente": "NOVA TELECOM LTDA",
                        "destinatario": "AGENCIA DE TECNOLOGIA DA INFORMACAO",
                        "emissao": "05/12/2025",
                        "pis_ret": "R$ 0,00",
                        "cofins_ret": "R$ 0,00",
                        "csll_ret": "R$ 0,00",
                        "irrf_ret": "R$ 1.593,60",
                        "total_retido": "R$ 1.593,60",
                    },
                    {
                        "nNF": "10841",
                        "cNF": "730003",
                        "emitente": "NOVA TELECOM LTDA",
                        "destinatario": "AGENCIA DE REGULACAO, CONTROLE E FISCALIZACAO DE SERVICOS PU",
                        "emissao": "04/12/2025",
                        "pis_ret": "R$ 0,00",
                        "cofins_ret": "R$ 0,00",
                        "csll_ret": "R$ 0,00",
                        "irrf_ret": "R$ 360,63",
                        "total_retido": "R$ 360,63",
                    },
                ],
            }
        ],
        "debug": {"total_xml": 3, "total_ok": 3, "total_falhas": 0, "primeiro_erro": None},
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
