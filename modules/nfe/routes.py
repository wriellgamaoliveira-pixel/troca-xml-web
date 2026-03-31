import csv
import io
import uuid

from flask import Blueprint, Response, render_template, request, session

from .nota_unica import processar_nota_unica_nfe
from .resumo import RELATORIO_CST_COLUMNS, gerar_relatorio_cst, gerar_relatorio_ncm

bp = Blueprint("nfe", __name__, url_prefix="/nfe", template_folder="templates")

_RELATORIO_CST_CACHE: dict[str, list[dict[str, str]]] = {}


def _render_relatorio_ncm():
    session["modulo"] = "nfe"
    data = None
    error = None

    if request.method == "POST":
        zip_file = request.files.get("zip_xmls_nfe")
        data, error = gerar_relatorio_ncm(zip_file)

    return render_template(
        "nfe/resumo_resultado.html",
        data=data,
        error=error,
        modulo="nfe",
        current_modulo="nfe",
    )


@bp.route("/resumo", methods=["GET", "POST"])
def resumo_page():
    return _render_relatorio_ncm()


@bp.route("/relatorio-ncm", methods=["GET", "POST"])
def relatorio_ncm_page():
    return _render_relatorio_ncm()


@bp.route("/relatorio-cst", methods=["GET", "POST"])
def relatorio_cst_page():
    session["modulo"] = "nfe"
    data = None
    error = None

    if request.method == "POST":
        zip_file = request.files.get("zip_xmls_nfe")
        data, error = gerar_relatorio_cst(zip_file)
        if data:
            token = str(uuid.uuid4())
            _RELATORIO_CST_CACHE[token] = data["linhas"]
            session["relatorio_cst_token"] = token

    return render_template(
        "nfe/relatorio_cst.html",
        data=data,
        error=error,
        columns=RELATORIO_CST_COLUMNS,
        modulo="nfe",
        current_modulo="nfe",
    )


@bp.route("/relatorio-cst/csv", methods=["GET"])
def relatorio_cst_csv():
    token = session.get("relatorio_cst_token")
    linhas = _RELATORIO_CST_CACHE.get(token or "") if token else None
    if not linhas:
        return Response("Nenhum relatório CST em memória para exportar.", status=400, mimetype="text/plain")

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";", lineterminator="\n")
    writer.writerow([label for _, label in RELATORIO_CST_COLUMNS])

    for linha in linhas:
        writer.writerow([linha.get(key, "") for key, _ in RELATORIO_CST_COLUMNS])

    csv_data = output.getvalue()
    output.close()

    return Response(
        csv_data,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=relatorio_cst_nfe.csv"},
    )


@bp.route("/alteracao")
def alteracao_page():
    session["modulo"] = "nfe"
    return render_template("nfe/alteracao.html", modulo="nfe", current_modulo="nfe")


@bp.route("/nota-unica", methods=["GET", "POST"])
def nota_unica_page():
    session["modulo"] = "nfe"
    data = None
    error = None

    if request.method == "POST":
        xml_file = request.files.get("xml_nota_nfe")
        data, error = processar_nota_unica_nfe(xml_file)

    return render_template(
        "nfe/nota_unica.html",
        modulo="nfe",
        current_modulo="nfe",
        data=data,
        error=error,
    )
