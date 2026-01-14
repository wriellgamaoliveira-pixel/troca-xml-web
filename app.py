from flask import Flask, render_template, request, send_file
from pathlib import Path
import tempfile
import zipfile
import shutil

from core import aplicar_regras_texto  # e as outras que você for usar

app = Flask(__name__)

@app.get("/")
def index():
    return render_template("index.html")

@app.post("/processar")
def processar():
    # 1) receber arquivos (ex.: .zip com XMLs)
    arq = request.files.get("zip_xmls")
    if not arq or not arq.filename.lower().endswith(".zip"):
        return "Envie um .zip com seus XMLs", 400

    # 2) criar pastas temporárias
    tmpdir = Path(tempfile.mkdtemp())
    entrada = tmpdir / "entrada"
    saida = tmpdir / "saida"
    entrada.mkdir()
    saida.mkdir()

    zip_path = tmpdir / "entrada.zip"
    arq.save(zip_path)

    # 3) descompactar
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(entrada)

    # 4) aplicar regras (exemplo simples)
    # Aqui você vai ler seus XMLs, aplicar aplicar_regras_texto, salvar_em_saida etc.
    # (vou deixar como “stub” porque seu script tem várias opções/regras)
    total = 0
    for p in entrada.rglob("*"):
        if p.is_file() and p.suffix.lower() in [".xml", ".txt"]:
            txt = p.read_text(encoding="utf-8", errors="ignore")
            novo = aplicar_regras_texto(
                txt,
                regras_cclass_cfop={},         # você vai montar isso com base no formulário
                remover_desconto=False,
                remover_outros=False
            )
            out = saida / p.relative_to(entrada)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(novo, encoding="utf-8", errors="ignore")
            total += 1

    # 5) gerar zip de saída
    out_zip = tmpdir / "resultado.zip"
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for p in saida.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(saida))

    # 6) devolver o zip
    return send_file(out_zip, as_attachment=True, download_name="resultado.zip")

if __name__ == "__main__":
    app.run(debug=True)
