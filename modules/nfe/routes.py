from flask import Blueprint, render_template, request, session

from .nota_unica import processar_nota_unica_nfe

bp = Blueprint("nfe", __name__, url_prefix="/nfe", template_folder="templates")


@bp.route("/resumo")
def resumo_page():
    from app import _get_resumo_data
    sid, data = _get_resumo_data()
    session["modulo"] = "nfe"
    return render_template("nfe/resumo.html", data=data, session_id=sid, modulo="nfe", current_modulo="nfe")


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
