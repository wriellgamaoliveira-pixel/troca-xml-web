from flask import Blueprint, render_template, session

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


@bp.route("/nota-unica")
def nota_unica_page():
    session["modulo"] = "nfe"
    return render_template("nfe/nota_unica.html", modulo="nfe", current_modulo="nfe")
