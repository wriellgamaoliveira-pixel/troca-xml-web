from flask import Blueprint, render_template, session

bp = Blueprint("nfcom", __name__, url_prefix="/nfcom", template_folder="templates")


@bp.route("/resumo")
def resumo_page():
    from app import _get_resumo_data
    sid, data = _get_resumo_data()
    session["modulo"] = "nfcom"
    return render_template("nfcom/resumo.html", data=data, session_id=sid, modulo="nfcom", current_modulo="nfcom")


@bp.route("/alteracao")
def alteracao_page():
    session["modulo"] = "nfcom"
    return render_template("nfcom/alteracao.html", modulo="nfcom", current_modulo="nfcom")


@bp.route("/nota-unica")
def nota_unica_page():
    session["modulo"] = "nfcom"
    return render_template("nfcom/nota_unica.html", modulo="nfcom", current_modulo="nfcom")
