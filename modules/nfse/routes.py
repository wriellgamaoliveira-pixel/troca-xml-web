from flask import Blueprint, render_template, session

bp = Blueprint("nfse", __name__, url_prefix="/nfse", template_folder="templates")


@bp.route("/resumo")
def resumo_page():
    from app import _get_resumo_data
    sid, data = _get_resumo_data()
    session["modulo"] = "nfse"
    return render_template("nfse/resumo.html", data=data, session_id=sid, modulo="nfse", current_modulo="nfse")


@bp.route("/alteracao")
def alteracao_page():
    session["modulo"] = "nfse"
    return render_template("nfse/alteracao.html", modulo="nfse", current_modulo="nfse")


@bp.route("/nota-unica")
def nota_unica_page():
    session["modulo"] = "nfse"
    return render_template("nfse/nota_unica.html", modulo="nfse", current_modulo="nfse")
