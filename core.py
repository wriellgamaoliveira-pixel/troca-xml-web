from pathlib import Path
import io
import csv
import re
import zipfile
import tempfile
import xml.etree.ElementTree as ET


# =========================
# Helpers
# =========================
def _to_float(v: str) -> float:
    if v is None:
        return 0.0
    s = str(v).strip()
    if not s:
        return 0.0
    # aceita "1.234,56" ou "1234.56"
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _br_money(x: float) -> str:
    # formata 1234.5 -> 1.234,50
    s = f"{x:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def _br_num(x: float) -> str:
    s = f"{x:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


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
    def text(e):
        return (e.text or "").strip() if e is not None else ""

    def find(root, path, ns):
        return root.find(path, ns) if root is not None else None

    def findall(root, path, ns):
        return root.findall(path, ns) if root is not None else []

    root = ET.fromstring(xml_bytes)

    # Namespace seguro (caso venha sem namespace)
    if "}" in root.tag:
        ns_uri = root.tag.split("}")[0].strip("{")
        ns = {"n": ns_uri}
        pref = "n:"
    else:
        ns = {}
        pref = ""

    inf = find(root, f".//{pref}infNFCom", ns)

    emit = find(inf, f".//{pref}emit", ns)
    dest = find(inf, f".//{pref}dest", ns)

    nNF = text(find(inf, f".//{pref}nNF", ns))
    serie = text(find(inf, f".//{pref}serie", ns))
    dhEmi = text(find(inf, f".//{pref}dhEmi", ns))
    chNFCom = text(find(root, f".//{pref}chNFCom", ns)) or text(find(inf, f".//{pref}chNFCom", ns))

    prot = find(root, f".//{pref}protNFCom", ns)
    prot_num = text(find(prot, f".//{pref}nProt", ns))
    prot_data = text(find(prot, f".//{pref}dhRecbto", ns))

    emit_nome = text(find(emit, f".//{pref}xNome", ns))
    emit_fant = text(find(emit, f".//{pref}xFant", ns))
    emit_cnpj = text(find(emit, f".//{pref}CNPJ", ns))
    emit_ie = text(find(emit, f".//{pref}IE", ns))

    end_emit = find(emit, f".//{pref}enderEmit", ns)
    emit_l1 = " ".join([x for x in [
        text(find(end_emit, f".//{pref}xLgr", ns)),
        text(find(end_emit, f".//{pref}nro", ns)),
        text(find(end_emit, f".//{pref}xBairro", ns)),
    ] if x]).strip()
    emit_l2 = " - ".join([x for x in [
        text(find(end_emit, f".//{pref}xMun", ns)),
        text(find(end_emit, f".//{pref}UF", ns)),
        text(find(end_emit, f".//{pref}CEP", ns)),
    ] if x]).strip()

    dest_nome = text(find(dest, f".//{pref}xNome", ns))
    dest_cpf = text(find(dest, f".//{pref}CPF", ns))
    dest_cnpj = text(find(dest, f".//{pref}CNPJ", ns))
    dest_doc = dest_cnpj or dest_cpf

    end_dest = find(dest, f".//{pref}enderDest", ns)
    dest_l1 = " ".join([x for x in [
        text(find(end_dest, f".//{pref}xLgr", ns)),
        text(find(end_dest, f".//{pref}nro", ns)),
        text(find(end_dest, f".//{pref}xBairro", ns)),
    ] if x]).strip()
    dest_l2 = " - ".join([x for x in [
        text(find(end_dest, f".//{pref}xMun", ns)),
        text(find(end_dest, f".//{pref}UF", ns)),
        text(find(end_dest, f".//{pref}CEP", ns)),
    ] if x]).strip()

    total = find(inf, f".//{pref}total", ns)
    vNF = text(find(total, f".//{pref}vNF", ns)) or text(find(inf, f".//{pref}vNF", ns))
    bc_total = text(find(total, f".//{pref}vBC", ns))
    icms_total = text(find(total, f".//{pref}vICMS", ns))
    vPIS = text(find(total, f".//{pref}vPIS", ns))
    vCOFINS = text(find(total, f".//{pref}vCOFINS", ns))

    itens = []
    for det in findall(inf, f".//{pref}det", ns):
        prod = find(det, f".//{pref}prod", ns)
        imposto = find(det, f".//{pref}imposto", ns)

        cClass = text(find(prod, f".//{pref}cClass", ns))
        xProd = text(find(prod, f".//{pref}xProd", ns))
        un = text(find(prod, f".//{pref}uCom", ns)) or text(find(prod, f".//{pref}uMed", ns))
        qtd = text(find(prod, f".//{pref}qCom", ns)) or text(find(prod, f".//{pref}qMed", ns))
        vUnit = text(find(prod, f".//{pref}vUnCom", ns)) or text(find(prod, f".//{pref}vUnMed", ns))
        vTotal = text(find(prod, f".//{pref}vProd", ns)) or text(find(prod, f".//{pref}vItem", ns))

        bc_icms = text(find(imposto, f".//{pref}vBC", ns))
        aliq_icms = text(find(imposto, f".//{pref}pICMS", ns))
        v_icms = text(find(imposto, f".//{pref}vICMS", ns))

        v_pis_item = text(find(imposto, f".//{pref}vPIS", ns))
        v_cof_item = text(find(imposto, f".//{pref}vCOFINS", ns))
        pis_cof = ""
        if v_pis_item or v_cof_item:
            pis_cof = f"{v_pis_item or '0,00'} / {v_cof_item or '0,00'}"

        itens.append({
            "cClass": cClass,
            "xProd": xProd,
            "un": un,
            "qtd": qtd,
            "vUnit": vUnit,
            "vTotal": vTotal,
            "pis_cofins": pis_cof,
            "bc_icms": bc_icms,
            "aliq_icms": aliq_icms,
            "v_icms": v_icms,
        })

    return {
        "nNF": nNF,
        "serie": serie,
        "dhEmi": dhEmi,
        "chNFCom": chNFCom,
        "prot_num": prot_num,
        "prot_data": prot_data,

        "emit_nome": emit_nome,
        "emit_fantasia": emit_fant,
        "emit_cnpj": emit_cnpj,
        "emit_ie": emit_ie,
        "emit_endereco_linha1": emit_l1,
        "emit_endereco_linha2": emit_l2,

        "dest_nome": dest_nome,
        "dest_doc": dest_doc,
        "dest_endereco_linha1": dest_l1,
        "dest_endereco_linha2": dest_l2,

        "total_fmt": (f"R$ {vNF}" if vNF else ""),
        "bc_total_fmt": (f"R$ {bc_total}" if bc_total else ""),
        "icms_total_fmt": (f"R$ {icms_total}" if icms_total else ""),
        "pis_total_fmt": (f"R$ {vPIS}" if vPIS else ""),
        "cofins_total_fmt": (f"R$ {vCOFINS}" if vCOFINS else ""),

        "qrcode_url": "",
        "itens": itens,

        "area_contribuinte": "",
        "info_complementar": "",
        "anatel_texto": "",
        "referencia": "",
        "vencimento": "",
        "periodo": "",
        "telefone": "",
        "cod_assinante": "",
        "contrato": "",
        "total_pagar_fmt": (f"R$ {vNF}" if vNF else ""),
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


# =========================
# RESUMO (ZIP -> Tabela + Pizza)
# =========================
def gerar_resumo_de_zip(zip_bytes):
    """
    Lê ZIP de XMLs, soma valor por cClass (usando vProd/vItem se existir)
    Retorna o objeto no formato que o templates/resumo.html espera.
    """
    tmp = Path(tempfile.mkdtemp())
    inp = tmp / "in"
    inp.mkdir()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        z.extractall(inp)

    total_arquivos = 0
    total_geral = 0.0

    # cClass -> {qtd_itens, v_total}
    mapa = {}

    for f in inp.rglob("*.xml"):
        total_arquivos += 1
        xml = f.read_bytes()

        try:
            root = ET.fromstring(xml)
        except Exception:
            # ignora xml inválido
            continue

        # tenta pegar itens: qualquer tag cClass dentro de det/prod
        # e valor: vProd ou vItem (se existir)
        for det in root.findall(".//det"):
            cclass_el = det.find(".//cClass")
            v_el = det.find(".//vProd") or det.find(".//vItem")

            cclass = (cclass_el.text or "").strip() if cclass_el is not None else ""
            v = _to_float(v_el.text) if v_el is not None else 0.0

            if not cclass:
                continue

            if cclass not in mapa:
                mapa[cclass] = {"qtd_itens": 0, "v_total": 0.0}

            mapa[cclass]["qtd_itens"] += 1
            mapa[cclass]["v_total"] += v
            total_geral += v

    # monta linhas ordenadas por valor desc
    linhas = []
    for cclass, info in mapa.items():
        v_total = info["v_total"]
        pct = (v_total / total_geral * 100.0) if total_geral > 0 else 0.0
        linhas.append({
            "cClass": cclass,
            "qtd_itens": info["qtd_itens"],
            "v_total": v_total,
            "v_total_br": _br_money(v_total),
            "pct": pct,
            "pct_br": _br_num(pct),
        })

    linhas.sort(key=lambda x: x["v_total"], reverse=True)

    # gráfico top 12
    top = linhas[:12]
    labels = [r["cClass"] for r in top]
    valores = [round(r["v_total"], 2) for r in top]

    return {
        "total_arquivos": total_arquivos,
        "total_geral": total_geral,
        "total_geral_br": _br_money(total_geral),
        "linhas": linhas,
        "labels": labels,
        "valores": valores,
    }
