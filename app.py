import io
from flask import Flask, render_template, request, send_file, redirect, url_for, flash

from core import (
    parse_regras_texto,
    processar_lote_zip,
    parse_nfcom_from_bytes,
    gerar_pdf_nfcom_bytes,
    processar_xml_unico,
    parse_mapping_text,
    gerar_csv_de_zip,
    resumo_de_zip,
    _fmt_br,
)

app = Flask(__name__)
app.secret_key = "troca-xml-web-secret"  # pode ser qualquer texto


@app.get("/")
def home():
    return render_template("index.html")


# =========================
# LOTE
# =========================
@app.get("/lote")
def lote():
    return render_template("lote.html")


@app.post("/lote/processar")
def lote_processar():
    arq = request.files.get("zip_xmls")
    if not arq:
        flash("Envie um arquivo .zip", "erro")
        return redirect(url_for("lote"))

    remover_desconto = bool(request.form.get("remover_desconto"))
    remover_outros = bool(request.form.get("remover_outros"))
    regras_txt = request.form.get("regras_cclass_cfop", "")
    regras = parse_regras_texto(regras_txt)

    out_zip = processar_lote_zip(arq.read(), regras, remover_desconto, remover_outros)

    return send_file(
        io.BytesIO(out_zip),
        as_attachment=True,
        download_name="resultado.zip",
        mimetype="application/zip",
    )


# =========================
# NOTA ÚNICA
# =========================
@app.get("/nota")
def nota():
    return render_template("nota.html")


@app.post("/nota/visualizar")
def nota_visualizar():
    arq = request.files.get("xml_unico")
    if not arq:
        flash("Envie um XML", "erro")
        return redirect(url_for("nota"))

    dados, err = parse_nfcom_from_bytes(arq.read())
    if err:
        flash(err, "erro")
        return redirect(url_for("nota"))

    return render_template("resultado.html", titulo="Visualização da Nota", dados=dados)


@app.post("/nota/pdf")
def nota_pdf():
    arq = request.files.get("xml_unico")
    if not arq:
        flash("Envie um XML", "erro")
        return redirect(url_for("nota"))

    dados, err = parse_nfcom_from_bytes(arq.read())
    if err:
        flash(err, "erro")
        return redirect(url_for("nota"))

    pdf_bytes = gerar_pdf_nfcom_bytes(dados)
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name="nota.pdf",
        mimetype="application/pdf",
    )


@app.post("/nota/xml_editado")
def nota_xml_editado():
    arq = request.files.get("xml_unico")
    if not arq:
        flash("Envie um XML", "erro")
        return redirect(url_for("nota"))

    remover_desconto = bool(request.form.get("remover_desconto"))
    remover_outros = bool(request.form.get("remover_outros"))
    regras = parse_regras_texto(request.form.get("regras_cclass_cfop", ""))

    novo_xml = processar_xml_unico(arq.read(), regras, remover_desconto, remover_outros)
    return send_file(
        io.BytesIO(novo_xml),
        as_attachment=True,
        download_name="xml_editado.xml",
        mimetype="application/xml",
    )


# =========================
# CSV
# =========================
@app.get("/csv")
def csv_page():
    mapping_exemplo = (
        "Numero;nNF\n"
        "Serie;serie\n"
        "Chave;chNFCom\n"
        "Emitente;emit/xNome\n"
        "Destinatario;dest/xNome\n"
        "ValorNF;total/vNF\n"
    )
    return render_template("csv.html", mapping_exemplo=mapping_exemplo)


@app.post("/csv/gerar")
def csv_gerar():
    arq = request.files.get("zip_xmls")
    if not arq:
        flash("Envie um .zip com XMLs", "erro")
        return redirect(url_for("csv_page"))

    mapping_text = request.form.get("mapping", "")
    mapping = parse_mapping_text(mapping_text)
    if not mapping:
        flash("Mapping vazio. Preencha cabeçalho;campo por linha.", "erro")
        return redirect(url_for("csv_page"))

    csv_bytes = gerar_csv_de_zip(arq.read(), mapping, delimiter=";")
    return send_file(
        io.BytesIO(csv_bytes),
        as_attachment=True,
        download_name="relatorio.csv",
        mimetype="text/csv",
    )


# =========================
# RESUMO
# =========================
@app.get("/resumo")
def resumo_page():
    return render_template("resumo.html")


@app.post("/resumo/gerar")
def resumo_gerar():
    arq = request.files.get("zip_xmls")
    if not arq:
        flash("Envie um .zip com XMLs", "erro")
        return redirect(url_for("resumo_page"))

    resumo = resumo_de_zip(arq.read())
    # formata valores pro template
    resumo["total_geral_br"] = _fmt_br(resumo.get("total_geral", 0.0))
    for r in resumo["linhas"]:
        r["v_total_br"] = _fmt_br(r["v_total"])
        r["pct_br"] = f"{r['pct']:.2f}".replace(".", ",")

    return render_template("resumo.html", resumo=resumo)


if __name__ == "__main__":
    app.run(debug=True)
