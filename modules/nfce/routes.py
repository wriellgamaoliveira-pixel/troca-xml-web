import csv
import io
import threading
import uuid
import zipfile

from flask import Blueprint, Response, jsonify, render_template, request, session

from .parser import parse_nfce_relatorio_cst

bp = Blueprint("nfce", __name__, url_prefix="/nfce", template_folder="templates")

COLUNAS_CST_NFCE = [
    ("tipo", "Tipo"),
    ("inscricao", "Inscrição"),
    ("documento", "Documento"),
    ("nome", "Nome Emit./Dest."),
    ("cfop", "CFOP"),
    ("produto", "Codigo Prod."),
    ("descricao", "Descrição item"),
    ("ncm", "NCM"),
    ("qtd", "Qtd."),
    ("v_unit", "Valor unitário"),
    ("v_desc", "Valor Desc."),
    ("v_prod", "Valor Cont."),
    ("bc_icms", "Base do ICMS"),
    ("aliq_icms", "Aliq. ICMS"),
    ("v_icms", "Valor do ICMS"),
    ("cst_pis", "CST PIS"),
    ("bc_pis", "Base do PIS"),
    ("aliq_pis", "Aliq. PIS"),
    ("v_pis", "Valor do PIS"),
    ("cst_cofins", "CST COFINS"),
    ("bc_cofins", "Base COFINS"),
    ("aliq_cofins", "Aliq. COFI."),
    ("v_cofins", "Valor COFINS"),
    ("total", "Valor total"),
]

CAMPOS_MONETARIOS = {"v_unit", "v_desc", "v_prod", "bc_icms", "v_icms", "bc_pis", "v_pis", "bc_cofins", "v_cofins", "total"}

_RELATORIO_CACHE: dict[str, list[dict[str, str]]] = {}
_RELATORIO_JOBS: dict[str, dict] = {}
_RELATORIO_JOB_DATA: dict[str, dict] = {}


def formatar_valor_br(valor):
    if valor is None:
        return "0,00"
    try:
        return f"{float(str(valor).replace(',', '.')):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0,00"


def _gerar_relatorio_cst(zip_file_storage, progress_cb=None):
    if zip_file_storage is None:
        return None, "Selecione um arquivo ZIP com XMLs de NFC-e."

    raw = zip_file_storage.read()
    if not raw:
        return None, "Arquivo ZIP vazio."

    total_arquivos = 0
    total_ok = 0
    linhas = []

    try:
        with zipfile.ZipFile(io.BytesIO(raw), "r") as zf:
            nomes = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            total_arquivos = len(nomes)

            for idx, nome in enumerate(nomes):
                try:
                    itens = parse_nfce_relatorio_cst(zf.read(nome))
                    if itens:
                        total_ok += 1
                        for item in itens:
                            row = dict(item)
                            for campo in CAMPOS_MONETARIOS:
                                row[campo] = formatar_valor_br(row.get(campo))
                            linhas.append(row)
                except Exception:
                    pass
                finally:
                    if progress_cb:
                        progresso = int(((idx + 1) / max(len(nomes), 1)) * 100)
                        progress_cb(progresso)

            if progress_cb and not nomes:
                progress_cb(100)

        return {
            "linhas": linhas,
            "total_arquivos": total_arquivos,
            "total_ok": total_ok,
            "total_itens": len(linhas),
        }, None
    except Exception as e:
        return None, f"Falha ao processar ZIP: {e}"


def _processar_relatorio_cst_job(sid: str, zip_bytes: bytes):
    _RELATORIO_JOBS[sid] = {"status": "processing", "progress": 0, "error": None}

    class _FileObj:
        def __init__(self, raw: bytes):
            self._raw = raw

        def read(self):
            return self._raw

    def _progress_cb(v):
        _RELATORIO_JOBS[sid]["progress"] = max(0, min(100, int(v)))

    data, error = _gerar_relatorio_cst(_FileObj(zip_bytes), progress_cb=_progress_cb)
    if error:
        _RELATORIO_JOBS[sid] = {"status": "error", "progress": 100, "error": error}
        return

    _RELATORIO_JOB_DATA[sid] = data or {}
    _RELATORIO_JOBS[sid] = {"status": "done", "progress": 100, "error": None}


@bp.route("/resumo")
def resumo_page():
    from app import _get_resumo_data

    sid, data = _get_resumo_data()
    session["modulo"] = "nfce"
    return render_template("nfce/resumo.html", data=data, session_id=sid, modulo="nfce", current_modulo="nfce")


@bp.route("/alteracao")
def alteracao_page():
    session["modulo"] = "nfce"
    return render_template("nfce/alteracao.html", modulo="nfce", current_modulo="nfce")


@bp.route("/nota-unica")
def nota_unica_page():
    session["modulo"] = "nfce"
    return render_template("nfce/nota_unica.html", modulo="nfce", current_modulo="nfce")


@bp.route("/relatorio-cst", methods=["GET", "POST"])
def relatorio_cst_page():
    session["modulo"] = "nfce"
    data = None
    error = None

    sid = request.args.get("sid", "").strip()
    if sid and sid in _RELATORIO_JOB_DATA:
        data = _RELATORIO_JOB_DATA[sid]
        token = str(uuid.uuid4())
        _RELATORIO_CACHE[token] = data.get("linhas", [])
        session["nfce_relatorio_cst_token"] = token

    if request.method == "POST":
        zip_file = request.files.get("zip_xmls_nfce")
        data, error = _gerar_relatorio_cst(zip_file)
        if data:
            token = str(uuid.uuid4())
            _RELATORIO_CACHE[token] = data["linhas"]
            session["nfce_relatorio_cst_token"] = token

    return render_template(
        "nfce/relatorio_cst.html",
        data=data,
        error=error,
        colunas=COLUNAS_CST_NFCE,
        modulo="nfce",
        current_modulo="nfce",
    )


@bp.route("/relatorio-cst/processar", methods=["POST"])
def relatorio_cst_processar():
    session["modulo"] = "nfce"
    zip_file = request.files.get("zip_xmls_nfce")
    if zip_file is None:
        return jsonify({"success": False, "error": "Selecione um arquivo ZIP."}), 400

    raw = zip_file.read()
    if not raw:
        return jsonify({"success": False, "error": "Arquivo ZIP vazio."}), 400

    sid = str(uuid.uuid4())
    t = threading.Thread(target=_processar_relatorio_cst_job, args=(sid, raw), daemon=True)
    t.start()
    return jsonify({"success": True, "sid": sid})


@bp.route("/relatorio-cst/status/<sid>", methods=["GET"])
def relatorio_cst_status(sid):
    st = _RELATORIO_JOBS.get(sid)
    if not st:
        return jsonify({"success": False, "error": "Sessão não encontrada."}), 404
    return jsonify({"success": True, **st})


@bp.route("/relatorio-cst/csv")
def relatorio_cst_csv():
    token = session.get("nfce_relatorio_cst_token")
    linhas = _RELATORIO_CACHE.get(token or "") if token else None
    if not linhas:
        return Response("Nenhum relatório CST NFC-e em memória para exportar.", status=400, mimetype="text/plain")

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";", lineterminator="\n")
    writer.writerow([label for _, label in COLUNAS_CST_NFCE])
    for linha in linhas:
        writer.writerow([linha.get(k, "") for k, _ in COLUNAS_CST_NFCE])

    content = output.getvalue()
    output.close()

    return Response(
        content,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=relatorio_cst_nfce.csv"},
    )
