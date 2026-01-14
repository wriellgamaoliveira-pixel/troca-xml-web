import io
from flask import Flask, render_template, request, send_file, redirect, url_for, flash

from core import (
    carregar_cclass_lista,
    parse_regras_texto,
    processar_lote_zip,
    parse_nfcom,
    gerar_csv_de_zip,
)

app = Flask(__name__)
app.secret_key = "troca-xml"

_CCLASS = None


@app.get("/")
def home():
    return render_template("index.html")


# ================= LOTE =================
@app.get("/lote")
def lote():
    global _CCLASS
    if _CCLASS is None:
        _CCLASS = carregar_cclass_lista()
    return render_template("lote.html", cclass_lista=_CCLASS)


@app.post("/lote/processar")
def lote_processar():
    zipf = request.files.get("zip_xmls")
    if not zipf:
        flash("Envie um ZIP", "erro")
        return redirect(url_for("lote"))

    regras = parse_regras_texto(request.form.get("regras_cclass_cfop"))
    remover_desc = bool(request.form.get("remover_desconto"))
    remover_outros = bool(request.form.get("remover_outros"))

    out = processar_lote_zip(zipf.read(), regras, remover_desc, remover_outros)

    return send_file(
        io.BytesIO(out),
        as_attachment=True,
        download_name="resultado.zip",
        mimetype="application/zip"
    )


# ================= NOTA =================
@app.get("/nota")
def nota():
    return render_template("nota.html")


@app.post("/nota/visualizar")
def nota_visualizar():
    xml = request.files.get("xml_unico")
    dados = parse_nfcom(xml.read())
    return render_template("resultado.html", dados=dados)


# ================= CSV =================
@app.get("/csv")
def csv_page():
    exemplo = "Numero;nNF\nSerie;serie"
    return render_template("csv.html", mapping_exemplo=exemplo)


@app.post("/csv/gerar")
def csv_gerar():
    zipf = request.files.get("zip_xmls")
    mapping = [(l.split(";")[0], l.split(";")[1])
               for l in request.form.get("mapping").splitlines()
               if ";" in l]

    out = gerar_csv_de_zip(zipf.read(), mapping)

    return send_file(
        io.BytesIO(out),
        as_attachment=True,
        download_name="relatorio.csv",
        mimetype="text/csv"
    )


if __name__ == "__main__":
    app.run(debug=True)
