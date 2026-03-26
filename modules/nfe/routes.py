from flask import Blueprint, render_template, request, session

from .nota_unica import processar_nota_unica_nfe
from .resumo import gerar_relatorio_ncm

bp = Blueprint("nfe", __name__, url_prefix="/nfe", template_folder="templates")


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
