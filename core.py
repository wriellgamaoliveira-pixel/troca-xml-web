from pathlib import Path
import io
import csv
import re
import zipfile
import tempfile
import xml.etree.ElementTree as ET

# =========================
# cClass (Excel)
# =========================
def carregar_cclass_lista(xlsx_path="data/Tabela-cClass.xlsx"):
    import openpyxl

    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb.active

    header = [str(ws.cell(1, c).value or "").strip() for c in range(1, ws.max_column + 1)]

    def col(nome):
        for i, h in enumerate(header, start=1):
            if h.lower() == nome.lower():
                return i
        return None

    col_code = col("Grupo/Código") or 1
    col_desc = col("Descrição") or 2

    lista = []
    for r in range(2, ws.max_row + 1):
        code = ws.cell(r, col_code).value
        desc = ws.cell(r, col_desc).value
        if not code or not desc:
            continue

        code = str(code).strip()
        desc = str(desc).strip()

        if not code.isdigit():
            continue
        if len(code) <= 3:
            continue

        lista.append({"code": code, "desc": desc})

    return lista


# =========================
# Regras
# =========================
def parse_regras_texto(txt):
    regras = {}
    for ln in (txt or "").splitlines():
        ln = ln.strip()
        if not ln or ";" not in ln:
            continue
        c, f = ln.split(";", 1)
        regras[c.strip()] = f.strip()
    return regras


def aplicar_regras_texto(texto, regras, remover_desc, remover_outros):
    novo = texto

    if remover_desc:
        novo = re.sub(r"<vDesc>.*?</vDesc>", "", novo, flags=re.DOTALL)

    if remover_outros:
        novo = re.sub(r"<vOutro>.*?</vOutro>", "", novo, flags=re.DOTALL)

    for cclass, cfop in regras.items():
        padrao = rf"(<cClass>{cclass}</cClass>)(?!\s*<CFOP>)"
        novo = re.sub(padrao, rf"\1<CFOP>{cfop}</CFOP>", novo)

    return novo


# =========================
# LOTE
# =========================
def processar_lote_zip(zip_bytes, regras, remover_desc=False, remover_outros=False):
    tmp = Path(tempfile.mkdtemp())
    inp = tmp / "in"
    out = tmp / "out"
    inp.mkdir()
    out.mkdir()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        z.extractall(inp)

    for f in inp.rglob("*.xml"):
        txt = f.read_text(encoding="utf-8", errors="ignore")
        novo = aplicar_regras_texto(txt, regras, remover_desc, remover_outros)
        dest = out / f.relative_to(inp)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(novo, encoding="utf-8")

    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as z:
        for f in out.rglob("*.xml"):
            z.write(f, f.relative_to(out))

    return bio.getvalue()


# =========================
# NOTA ÚNICA
# =========================
def parse_nfcom(xml_bytes):
    root = ET.fromstring(xml_bytes)
    ns = {"n": root.tag.split("}")[0].strip("{")}

    def g(p, t):
        e = p.find(f".//n:{t}", ns)
        return e.text if e is not None else ""

    inf = root.find(".//n:infNFCom", ns)

    itens = []
    for d in inf.findall(".//n:det", ns):
        prod = d.find(".//n:prod", ns)
        itens.append({
            "cClass": g(prod, "cClass"),
            "xProd": g(prod, "xProd"),
            "qCom": g(prod, "qCom"),
            "vProd": g(prod, "vProd"),
        })

    return {
        "nNF": g(inf, "nNF"),
        "serie": g(inf, "serie"),
        "emit": g(inf, "xNome"),
        "dest": g(inf, "xNome"),
        "itens": itens
    }


# =========================
# CSV
# =========================
def gerar_csv_de_zip(zip_bytes, mapping):
    tmp = Path(tempfile.mkdtemp())
    inp = tmp / "in"
    inp.mkdir()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        z.extractall(inp)

    bio = io.StringIO()
    w = csv.writer(bio, delimiter=";")
    w.writerow([m[0] for m in mapping])

    for f in inp.rglob("*.xml"):
        root = ET.fromstring(f.read_bytes())
        row = []
        for _, campo in mapping:
            el = root.find(f".//{campo}")
            row.append(el.text if el is not None else "")
        w.writerow(row)

    return bio.getvalue().encode("utf-8-sig")
