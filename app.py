from __future__ import annotations

import io
from flask import Flask, render_template, request, send_file, flash

from core import (
    carregar_cclass_lista,
    cclass_desc_map,
    parse_regras_texto,
    processar_lote_zip,
    gerar_csv_de_zip,
    gerar_resumo_de_zip,
    gerar_dados_nota_xml,  # <<< NOTA
)

app = Flask(__name__)
app.secret_key = "troca-xml"

_CCLASS_LISTA = None
_DESC_MAP = None


def _load_cclass():
    global _CCLASS_LISTA, _DESC_MAP
    if _CCLASS_LISTA is None:
        _CCLASS_LISTA = carregar_cclass_lista()
        _DESC_MAP = cclass_desc_map(_CCLASS_LISTA)
    return _CCLASS_LISTA, _DESC_MAP


@app.get("/")
def index():
    return render_template("index.html")


# =========================
# LOTE
# =========================
@app.get("/lote")
def lote():
    cclass_lista, _ = _load_cclass()
    return render_template("lote.html", cclass_lista=cclass_lista)


@app.post("/lote/processar")
def lote_processar():
    cclass_lista, _ = _load_cclass()

    fzip = request.files.get("zip_xmls")
    if not fzip:
        flash("Envie um arquivo .zip")
        return render_template("lote.html", cclass_lista=cclass_lista)

    remover_desconto = bool(request.form.get("remover_desconto"))
    remover_outros = bool(request.form.get("remover_outros"))
    regras_txt = request.form.get("regras_txt", "")
    regras = parse_regras_texto(regras_txt)

    out_zip = processar_lote_zip(
        fzip.read(),
        regras=regras,
        remover_desconto=remover_desconto,
        remover_outros=remover_outros,
    )

    return send_file(
        io.BytesIO(out_zip),
        as_attachment=True,
        download_name="resultado.zip",
        mimetype="application/zip",
    )


# =========================
# NOTA ÚNICA (VISUALIZAÇÃO)
# =========================
@app.get("/nota")
def nota():
    # Página com upload do XML (template nota.html)
    return render_template("nota.html")


@app.post("/nota/visualizar")
def nota_visualizar():
    fxml = request.files.get("xml_nota")
    if not fxml:
        flash("Envie um XML.")
        return render_template("nota.html")

    try:
        xml_bytes = fxml.read()
        d = gerar_dados_nota_xml(xml_bytes)
        # Renderiza exatamente o seu template de DANFECom/resultado
        return render_template("resultado.html", d=d)
    except Exception as e:
        flash(f"Erro ao processar XML: {e}")
        return render_template("nota.html")


# =========================
# CSV
# =========================
@app.get("/csv")
def csv_page():
    return render_template("csv.html")


@app.post("/csv/gerar")
def csv_gerar():
    fzip = request.files.get("zip_xmls")
    if not fzip:
        flash("Envie um arquivo .zip")
        return render_template("csv.html")

    mapping_txt = request.form.get("mapping_txt", "")
    mapping = []
    for l in (mapping_txt or "").splitlines():
        l = l.strip()
        if not l or l.startswith("#"):
            continue
        if ";" in l:
            a, b = [x.strip() for x in l.split(";", 1)]
            if a and b:
                mapping.append((a, b))

    if not mapping:
        flash("Informe o mapeamento no formato CABEÇALHO;TAG")
        return render_template("csv.html")

    out = gerar_csv_de_zip(fzip.read(), mapping)

    return send_file(
        io.BytesIO(out),
        as_attachment=True,
        download_name="relatorio.csv",
        mimetype="text/csv",
    )


# =========================
# RESUMO
# =========================
@app.get("/resumo")
def resumo():
    return render_template("resumo.html", resumo=None)


@app.post("/resumo/gerar")
def resumo_gerar():
    _, desc_map = _load_cclass()

    fzip = request.files.get("zip_xmls")
    if not fzip:
        flash("Envie um arquivo .zip")
        return render_template("resumo.html", resumo=None)

    resumo_data = gerar_resumo_de_zip(fzip.read(), desc_map=desc_map)
    return render_template("resumo.html", resumo=resumo_data)


if __name__ == "__main__":
    app.run(debug=True)
