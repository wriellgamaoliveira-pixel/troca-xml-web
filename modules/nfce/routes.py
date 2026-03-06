from flask import Blueprint, render_template, session

bp = Blueprint("nfce", __name__, url_prefix="/nfce", template_folder="templates")


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
