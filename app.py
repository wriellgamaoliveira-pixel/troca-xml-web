from flask import Flask, render_template, request, jsonify, send_file, session, url_for
import os
import uuid
import json
from datetime import datetime
import zipfile
import io
import tempfile
import xml.etree.ElementTree as ET
import pandas as pd

# =========================================================
# APP
# =========================================================
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-troca-xml-2024")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

TEMP_DIR = "temp"
UPLOADS_DIR = "uploads"
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

# =========================================================
# REDIS (fallback FakeRedis caso REDIS_URL não exista)
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
        import redis  # redis==5.x
        return redis.Redis.from_url(redis_url)
    except Exception:
        return FakeRedis()


redis_store = get_redis()

def redis_setex(key: str, ttl: int, value: bytes):
    # FakeRedis aceita qualquer tipo; redis real precisa bytes/str
    if hasattr(redis_store, "setex"):
        return redis_store.setex(key, ttl, value)
    return None

def redis_get(key: str):
    if hasattr(redis_store, "get"):
        return redis_store.get(key)
    return None

def redis_ttl(key: str):
    if hasattr(redis_store, "ttl"):
        return redis_store.ttl(key)
    return 0


# =========================================================
# UTIL: formatação
# =========================================================
def br_money(v: float) -> str:
    try:
        return "R$ {:,.2f}".format(float(v)).replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

def br_date(iso: str) -> str:
    # tenta converter 2025-12-04T... -> 04/12/2025
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
# XML: parse simples (NFe / NFCom)
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

    # Itens NFCom variam por schema; tentamos pegar det (se existir) + fallback
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
        return {"error": "XML inválido (parse falhou)"}

    tag = root.tag.lower()
    if tag.endswith("nfe"):
        return parse_nfe(root)
    if tag.endswith("nfcom"):
        return parse_nfcom(root)

    # fallback: tenta procurar filhos
    # (alguns XMLs vêm como <nfeProc> etc)
    if root.find(".//{http://www.portalfiscal.inf.br/nfe}NFe") is not None:
        nfe = root.find(".//{http://www.portalfiscal.inf.br/nfe}NFe")
        return parse_nfe(nfe)
    if root.find(".//{http://www.portalfiscal.inf.br/nfcom}NFCom") is not None:
        nfcom = root.find(".//{http://www.portalfiscal.inf.br/nfcom}NFCom")
        return parse_nfcom(nfcom)

    return {"error": "Tipo XML não suportado (não é NFe/NFCom)"}


# =========================================================
# ROTAS PÁGINAS
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
    # Se tiver dados na sessão, usa; senão usa exemplo
    dados = session.get("resumo_data") or gerar_dados_exemplo()
    return render_template("resumo_resultado.html", data=dados)

@app.route("/csv")
def csv_page():
    return render_template("csv.html")


# =========================================================
# API - RESUMO (compatível com seu resumo.html)
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
            return jsonify({"success": False, "error": "Apenas arquivos ZIP são aceitos"}), 400

        session_id = str(uuid.uuid4())
        zip_path = os.path.join(UPLOADS_DIR, f"{session_id}.zip")
        f.save(zip_path)

        dados = processar_zip_resumo(zip_path)

        session["resumo_session_id"] = session_id
        session["resumo_data"] = dados

        return jsonify({"success": True, "session_id": session_id, "redirect": url_for("resumo_resultado_page")})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/resumo/dados")
def api_resumo_dados():
    sid = request.args.get("session_id")
    if sid and sid == session.get("resumo_session_id"):
        dados = session.get("resumo_data") or gerar_dados_exemplo()
    else:
        dados = gerar_dados_exemplo()
    return jsonify(dados)


# =========================================================
# API - NOTA ÚNICA
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
# API - SESSÃO (simples)
# =========================================================
@app.route("/api/sessao/criar", methods=["POST"])
def api_sessao_criar():
    session_id = str(uuid.uuid4())
    session["current_session"] = session_id

    sessao_data = {
        "id": session_id,
        "criado_em": datetime.now().isoformat(),
        "status": "ativa",
        "arquivos": [],
        "chunks_recebidos": 0,
    }

    # 4 horas
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

@app.route("/api/sessao/upload-chunk", methods=["POST"])
def api_sessao_upload_chunk():
    try:
        if "chunk" not in request.files:
            return jsonify({"success": False, "error": "Nenhum chunk enviado"}), 400

        session_id = request.form.get("session_id")
        if not session_id:
            return jsonify({"success": False, "error": "session_id é obrigatório"}), 400

        chunk_index = int(request.form.get("chunk_index", 0))
        total_chunks = int(request.form.get("total_chunks", 1))

        chunk = request.files["chunk"].read()

        redis_setex(f"session:chunk:{session_id}:{chunk_index}", 14400, chunk)

        raw = redis_get(f"session:{session_id}")
        if raw:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            sessao_data = json.loads(raw)
            sessao_data["chunks_recebidos"] = int(sessao_data.get("chunks_recebidos", 0)) + 1
            redis_setex(f"session:{session_id}", 14400, json.dumps(sessao_data).encode("utf-8"))

        return jsonify({"success": True, "chunk": chunk_index, "total": total_chunks})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================================================
# API - LOTE (aceita multipart OU json)
# =========================================================
@app.route("/api/lote/processar", methods=["POST"])
def api_lote_processar():
    try:
        # JSON (fallback)
        if request.is_json:
            data = request.get_json(silent=True) or {}
            session_id = data.get("session_id", str(uuid.uuid4()))
            regras = data.get("regras", {})
        else:
            # multipart/form-data (mais comum em HTML)
            session_id = request.form.get("session_id") or str(uuid.uuid4())
            regras_texto = request.form.get("regras_cclass_cfop", "") or ""
            remover_desconto = (request.form.get("remover_desconto", "false") == "true")
            remover_outros = (request.form.get("remover_outros", "false") == "true")
            regras = {
                "texto": regras_texto,
                "remover_desconto": remover_desconto,
                "remover_outros": remover_outros,
            }

        # Se veio ZIP, só guardamos (dummy)
        zip_file = request.files.get("zip_xmls")
        if zip_file and zip_file.filename:
            _ = zip_file.read()  # aqui você processaria de verdade

        # Gera ZIP de resposta (dummy)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("processado.txt", f"XMLs processados com regras:\n{json.dumps(regras, ensure_ascii=False, indent=2)}\n")
            z.writestr("relatorio.txt", "Relatório de processamento: exemplo (sem parser completo)\n")

        zip_buffer.seek(0)

        zip_path = os.path.join(TEMP_DIR, f"{session_id}_processado.zip")
        with open(zip_path, "wb") as f:
            f.write(zip_buffer.getvalue())

        return jsonify(
            {
                "success": True,
                "session_id": session_id,
                "arquivos_processados": 0,
                "download_url": f"/download/{session_id}",
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================================================
# API - CSV (gera CSV simples a partir de XMLs no ZIP)
# =========================================================
@app.route("/api/csv/gerar", methods=["POST"])
def api_csv_gerar():
    try:
        if "zip_xmls" not in request.files:
            return jsonify({"success": False, "error": "Envie o ZIP no campo zip_xmls"}), 400

        zf = request.files["zip_xmls"]
        if not zf.filename.lower().endswith(".zip"):
            return jsonify({"success": False, "error": "Envie um arquivo .zip"}), 400

        # Mapeamento simples (coluna=path) opcional
        # Exemplo aceito: nNF;data;emitente_nome;dest_nome
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
                    row = {k: row.get(k) for k in requested if k in row}  # filtra
                    if "arquivo" not in row:
                        row["arquivo"] = name
                rows.append(row)

        if not rows:
            return jsonify({"success": False, "error": "Nenhum XML válido encontrado no ZIP"}), 400

        df = pd.DataFrame(rows)
        csv_bytes = df.to_csv(index=False).encode("utf-8")

        return send_file(
            io.BytesIO(csv_bytes),
            as_attachment=True,
            download_name="export.csv",
            mimetype="text/csv",
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =========================================================
# DOWNLOAD
# =========================================================
@app.route("/download/<session_id>")
def download_file(session_id):
    zip_path = os.path.join(TEMP_DIR, f"{session_id}_processado.zip")
    if os.path.exists(zip_path):
        return send_file(zip_path, as_attachment=True, download_name=f"processado_{session_id}.zip")

    # fallback
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as z:
        z.writestr("exemplo.txt", "Arquivo processado pelo Troca XML Web")
    zip_buffer.seek(0)
    return send_file(zip_buffer, as_attachment=True, download_name=f"processado_{session_id}.zip", mimetype="application/zip")


# =========================================================
# RESUMO (mantém seu layout e dados)
# =========================================================
def processar_zip_resumo(zip_path: str):
    # Aqui você colocaria o parser real.
    # Por agora, mantém o comportamento do seu app: retorna exemplo.
    return gerar_dados_exemplo()

def gerar_dados_exemplo():
    return {
        "emitente_nome": "NOVA TELECOM LTDA",
        "emitente_cnpj": "01.555.241/0001-20",
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
                "item": "167",
                "desc": "GOV SCI 20 MBPS",
                "cClass": "600601",
                "qtd_itens": 8,
                "v_total": 34609.06,
                "v_total_br": "R$ 34.609,06",
                "pct": 18.70,
                "pct_br": "18,70%",
                "notas": [],
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
                        "xNome": "NOVA TELECOM LTDA",
                        "xContato": "AGENCIA DE DEFESA AGROPECUARIA DO ESTADO DO TOCANTINS",
                        "dhEmi_fmt": "05/12/2025",
                        "pis_ret": 0.00,
                        "cofins_ret": 0.00,
                        "csll_ret": 0.00,
                        "irrf_ret": 6927.36,
                        "total_retido": 6927.36,
                    }
                ],
            }
        ],
        "debug": {"total_xml": 3, "total_ok": 3, "total_falhas": 0, "primeiro_erro": None},
    }

def limpar_temporarios():
    agora = datetime.now().timestamp()
    for dir_path in [TEMP_DIR, UPLOADS_DIR]:
        if os.path.exists(dir_path):
            for filename in os.listdir(dir_path):
                file_path = os.path.join(dir_path, filename)
                if os.path.isfile(file_path):
                    age = agora - os.path.getmtime(file_path)
                    if age > 86400:
                        try:
                            os.remove(file_path)
                        except Exception:
                            pass

@app.before_request
def before_request():
    if not hasattr(app, "last_cleanup"):
        app.last_cleanup = datetime.now().timestamp()
    agora = datetime.now().timestamp()
    if agora - app.last_cleanup > 3600:
        limpar_temporarios()
        app.last_cleanup = agora


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
