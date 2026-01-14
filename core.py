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
    import xml.etree.ElementTree as ET

    def text(e):
        return (e.text or "").strip() if e is not None else ""

    def find(root, path, ns):
        return root.find(path, ns)

    def findall(root, path, ns):
        return root.findall(path, ns)

    root = ET.fromstring(xml_bytes)
    ns_uri = root.tag.split("}")[0].strip("{")
    ns = {"n": ns_uri}

    inf = find(root, ".//n:infNFCom", ns)

    # emitente/destinatário (nomes variam por implementação; usamos o que existir)
    emit = find(inf, ".//n:emit", ns) if inf is not None else None
    dest = find(inf, ".//n:dest", ns) if inf is not None else None

    # alguns campos comuns
    nNF = text(find(inf, ".//n:nNF", ns))
    serie = text(find(inf, ".//n:serie", ns))
    dhEmi = text(find(inf, ".//n:dhEmi", ns))
    chNFCom = text(find(root, ".//n:chNFCom", ns)) or text(find(inf, ".//n:chNFCom", ns))

    # protocolo (quando presente)
    prot = find(root, ".//n:protNFCom", ns)
    prot_num = text(find(prot, ".//n:nProt", ns))
    prot_data = text(find(prot, ".//n:dhRecbto", ns))

    # emit
    emit_nome = text(find(emit, ".//n:xNome", ns))
    emit_fant = text(find(emit, ".//n:xFant", ns))
    emit_cnpj = text(find(emit, ".//n:CNPJ", ns))
    emit_ie = text(find(emit, ".//n:IE", ns))

    end_emit = find(emit, ".//n:enderEmit", ns)
    emit_l1 = " ".join([x for x in [
        text(find(end_emit, ".//n:xLgr", ns)),
        text(find(end_emit, ".//n:nro", ns)),
        text(find(end_emit, ".//n:xBairro", ns)),
    ] if x]).strip()
    emit_l2 = " - ".join([x for x in [
        text(find(end_emit, ".//n:xMun", ns)),
        text(find(end_emit, ".//n:UF", ns)),
        text(find(end_emit, ".//n:CEP", ns)),
    ] if x]).strip()

    # dest
    dest_nome = text(find(dest, ".//n:xNome", ns))
    dest_cpf = text(find(dest, ".//n:CPF", ns))
    dest_cnpj = text(find(dest, ".//n:CNPJ", ns))
    dest_doc = dest_cnpj or dest_cpf

    end_dest = find(dest, ".//n:enderDest", ns)
    dest_l1 = " ".join([x for x in [
        text(find(end_dest, ".//n:xLgr", ns)),
        text(find(end_dest, ".//n:nro", ns)),
        text(find(end_dest, ".//n:xBairro", ns)),
    ] if x]).strip()
    dest_l2 = " - ".join([x for x in [
        text(find(end_dest, ".//n:xMun", ns)),
        text(find(end_dest, ".//n:UF", ns)),
        text(find(end_dest, ".//n:CEP", ns)),
    ] if x]).strip()

    # totais (tenta pegar vNF e alguns impostos, se existirem)
    total = find(inf, ".//n:total", ns) if inf is not None else None
    vNF = text(find(total, ".//n:vNF", ns)) or text(find(inf, ".//n:vNF", ns))
    bc_total = text(find(total, ".//n:vBC", ns))
    icms_total = text(find(total, ".//n:vICMS", ns))
    vPIS = text(find(total, ".//n:vPIS", ns))
    vCOFINS = text(find(total, ".//n:vCOFINS", ns))

    # itens (det/prod/imposto variam; pegamos o que existir)
    itens = []
    for det in findall(inf, ".//n:det", ns) if inf is not None else []:
        prod = find(det, ".//n:prod", ns)
        imposto = find(det, ".//n:imposto", ns)

        cClass = text(find(prod, ".//n:cClass", ns))
        xProd = text(find(prod, ".//n:xProd", ns))
        un = text(find(prod, ".//n:uCom", ns)) or text(find(prod, ".//n:uMed", ns))
        qtd = text(find(prod, ".//n:qCom", ns)) or text(find(prod, ".//n:qMed", ns))
        vUnit = text(find(prod, ".//n:vUnCom", ns)) or text(find(prod, ".//n:vUnMed", ns))
        vTotal = text(find(prod, ".//n:vProd", ns)) or text(find(prod, ".//n:vItem", ns))

        # ICMS (se existir)
        bc_icms = text(find(imposto, ".//n:vBC", ns))
        aliq_icms = text(find(imposto, ".//n:pICMS", ns))
        v_icms = text(find(imposto, ".//n:vICMS", ns))

        # PIS/COFINS (se existir)
        v_pis_item = text(find(imposto, ".//n:vPIS", ns))
        v_cof_item = text(find(imposto, ".//n:vCOFINS", ns))
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

        "qrcode_url": "",  # se você tiver a URL no XML a gente liga aqui
        "itens": itens,

        # campos do modelo que podem não existir no XML:
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

