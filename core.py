# core.py
from __future__ import annotations

from pathlib import Path
import csv
import io
import json
import os
import re
import sys
import tempfile
import traceback
import zipfile
from typing import Dict, List, Optional, Tuple, Any

import xml.etree.ElementTree as ET


# =========================
# CONFIG
# =========================
EXTENSOES = [".xml", ".txt"]

TAGS_DESCONTO = ["<vDesc>", "</vDesc>"]
TAGS_OUTROS = ["<vOutro>", "</vOutro>"]

NF_NS = {"n": "http://www.portalfiscal.inf.br/nfcom"}

# Se você quiser persistência no servidor, dá pra usar.
# Em plataformas web, o filesystem pode ser temporário.
SETTINGS_FILE = Path(__file__).parent / "ultima_selecao.json"


# =========================
# SETTINGS (sem Tkinter)
# =========================
def load_settings() -> dict:
    try:
        if SETTINGS_FILE.exists():
            return json.loads(SETTINGS_FILE.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        pass
    return {}


def save_settings(data: dict) -> None:
    try:
        SETTINGS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # em ambiente web pode falhar por permissão; ignorar é aceitável
        pass


# =========================
# DEPENDÊNCIAS (sem messagebox)
# =========================
def ensure_openpyxl() -> None:
    try:
        import openpyxl  # noqa
    except ImportError as e:
        raise RuntimeError("Dependência ausente: openpyxl. Instale com: pip install openpyxl") from e


def diagnostico_reportlab() -> str:
    try:
        import reportlab
        from reportlab.pdfgen import canvas  # noqa
        return (
            "ReportLab OK!\n"
            f"Python: {sys.executable}\n"
            f"Versão: {sys.version}\n"
            f"ReportLab: {getattr(reportlab, '__version__', 'desconhecida')}\n"
        )
    except Exception:
        return (
            "Falha ao importar ReportLab\n"
            f"Python: {sys.executable}\n"
            f"Versão: {sys.version}\n\n"
            + traceback.format_exc()
        )


def ensure_reportlab() -> None:
    try:
        from reportlab.pdfgen import canvas  # noqa
    except Exception as e:
        raise RuntimeError("Dependência ausente: reportlab. Instale com: pip install reportlab\n\n" + diagnostico_reportlab()) from e


# =========================
# EXCEL TABELA cClass
# =========================
def carregar_tabela_excel(caminho_excel: str | Path) -> List[Tuple[str, str]]:
    ensure_openpyxl()
    import openpyxl

    p = Path(caminho_excel)
    wb = openpyxl.load_workbook(str(p))
    ws = wb.active

    header = [str(cell.value).strip() if cell.value else "" for cell in ws[1]]

    def find_col(names: List[str]) -> Optional[int]:
        names_lower = [n.lower() for n in names]
        for i, colname in enumerate(header):
            if colname.lower() in names_lower:
                return i
        return None

    col_cclass = find_col(["cclass", "cClass", "codigo", "código", "classificacao", "classificação"])
    col_desc = find_col(["descricao", "descrição", "desc", "item", "nome"])

    if col_cclass is None:
        col_cclass = 0
    if col_desc is None:
        col_desc = 1 if len(header) > 1 else None

    dados: List[Tuple[str, str]] = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or row[col_cclass] is None:
            continue
        cclass = str(row[col_cclass]).strip()
        desc = (
            str(row[col_desc]).strip()
            if (col_desc is not None and col_desc < len(row) and row[col_desc] is not None)
            else ""
        )
        dados.append((cclass, desc))
    return dados


def montar_mapa_descricao_cclass(caminho_excel: str | Path) -> Dict[str, str]:
    p = Path(caminho_excel) if caminho_excel else None
    if not p or not p.exists():
        return {}
    try:
        lista = carregar_tabela_excel(p)
    except Exception:
        return {}
    mapa: Dict[str, str] = {}
    for cclass, desc in lista:
        cc = str(cclass).strip()
        if cc and cc not in mapa:
            mapa[cc] = str(desc).strip()
    return mapa


# =========================
# REGRAS (CFOP + REMOÇÕES)
# =========================
def aplicar_regras_texto(
    texto: str,
    regras_cclass_cfop: Dict[str, str],
    remover_desconto: bool,
    remover_outros: bool,
) -> str:
    novo = texto

    if remover_desconto:
        for tag in TAGS_DESCONTO:
            novo = novo.replace(tag, "")

    if remover_outros:
        for tag in TAGS_OUTROS:
            novo = novo.replace(tag, "")

    for cclass, cfop in (regras_cclass_cfop or {}).items():
        if not str(cfop).strip():
            continue
        padrao = rf"(<cClass>{re.escape(str(cclass))}</cClass>)(?!<CFOP>)(<uMed>)"
        substitui = rf"\1<CFOP>{cfop}</CFOP>\2"
        novo = re.sub(padrao, substitui, novo)

    return novo


def salvar_em_saida(
    arquivo_original: Path,
    base_entrada: Path,
    base_saida: Path,
    novo_conteudo: str,
) -> Path:
    rel = arquivo_original.relative_to(base_entrada)
    destino = base_saida / rel
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(novo_conteudo, encoding="utf-8", errors="ignore")
    return destino


def processar_lote_pasta(
    pasta_entrada: str | Path,
    pasta_saida: str | Path,
    regras_cclass_cfop: Dict[str, str],
    remover_desconto: bool = False,
    remover_outros: bool = False,
) -> Dict[str, Any]:
    base_in = Path(pasta_entrada)
    base_out = Path(pasta_saida)

    if not base_in.exists():
        raise FileNotFoundError("Pasta de entrada não existe.")
    if base_in.resolve() == base_out.resolve():
        raise ValueError("A pasta de saída deve ser diferente da pasta de entrada.")

    arquivos = [f for f in base_in.rglob("*") if f.is_file() and f.suffix.lower() in EXTENSOES]
    if not arquivos:
        return {"total": 0, "alterados": 0, "copiados": 0}

    alterados = 0
    copiados = 0

    for f in arquivos:
        original = f.read_text(encoding="utf-8", errors="ignore")
        novo = aplicar_regras_texto(original, regras_cclass_cfop, remover_desconto, remover_outros)

        if novo != original:
            salvar_em_saida(f, base_in, base_out, novo)
            alterados += 1
        else:
            salvar_em_saida(f, base_in, base_out, original)
            copiados += 1

    return {"total": len(arquivos), "alterados": alterados, "copiados": copiados}


def processar_lote_zip(
    zip_bytes: bytes,
    regras_cclass_cfop: Dict[str, str],
    remover_desconto: bool = False,
    remover_outros: bool = False,
) -> bytes:
    """
    Entrada: ZIP (bytes) contendo XML/TXT em qualquer estrutura de pastas.
    Saída: ZIP (bytes) com os arquivos processados preservando estrutura.
    Ideal para web (upload/download).
    """
    tmpdir = Path(tempfile.mkdtemp())
    entrada = tmpdir / "entrada"
    saida = tmpdir / "saida"
    entrada.mkdir()
    saida.mkdir()

    zip_in = tmpdir / "in.zip"
    zip_in.write_bytes(zip_bytes)

    with zipfile.ZipFile(zip_in, "r") as z:
        z.extractall(entrada)

    processar_lote_pasta(entrada, saida, regras_cclass_cfop, remover_desconto, remover_outros)

    zip_out = tmpdir / "out.zip"
    with zipfile.ZipFile(zip_out, "w", zipfile.ZIP_DEFLATED) as z:
        for p in saida.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(saida))

    return zip_out.read_bytes()


# =========================
# XML HELPERS (NFCom)
# =========================
def _find(parent, tag):
    if parent is None:
        return None
    return parent.find(f"n:{tag}", NF_NS)


def gettxt(parent, tag, default=""):
    el = _find(parent, tag)
    if el is None or el.text is None:
        return default
    return el.text.strip()


def get0(parent, tag, default="0.00"):
    el = _find(parent, tag)
    if el is None or el.text is None or el.text.strip() == "":
        return default
    return el.text.strip()


def find_any_text(root, endings, default=""):
    if root is None:
        return default
    for el in root.iter():
        t = (el.tag or "").lower()
        for end in endings:
            if t.endswith(end.lower()):
                if el.text and el.text.strip():
                    return el.text.strip()
    return default


def fmt_addr(addr):
    if not addr:
        return ""
    xLgr = gettxt(addr, "xLgr", "")
    nro = gettxt(addr, "nro", "")
    xBairro = gettxt(addr, "xBairro", "")
    xMun = gettxt(addr, "xMun", "")
    UF = gettxt(addr, "UF", "")
    CEP = gettxt(addr, "CEP", "")

    p1 = ", ".join([p for p in [xLgr, nro, xBairro] if p])
    p2 = " - ".join([p for p in [xMun, UF] if p])
    if CEP:
        p2 = (p2 + f"  CEP: {CEP}") if p2 else f"CEP: {CEP}"
    return (p1 + ("\n" + p2 if p2 else "")).strip()


def parse_nfcom(path_xml: str | Path) -> Tuple[Optional[dict], Optional[str]]:
    path_xml = Path(path_xml)
    try:
        tree_xml = ET.parse(path_xml)
        root = tree_xml.getroot()
    except Exception as e:
        return None, f"Erro lendo XML: {e}"

    inf = root.find(".//n:infNFCom", NF_NS)
    if inf is None:
        inf = root.find(".//infNFCom")
    if inf is None:
        return None, "Não encontrei o bloco infNFCom no XML."

    emit = inf.find("n:emit", NF_NS)
    dest = inf.find("n:dest", NF_NS)

    end_emit = inf.find(".//n:enderEmit", NF_NS)
    end_dest = inf.find(".//n:enderDest", NF_NS)

    total = inf.find(".//n:total", NF_NS)
    icms_tot = total.find(".//n:ICMSTot", NF_NS) if total is not None else None

    dados: Dict[str, Any] = {}

    dados["nNF"] = gettxt(inf, "nNF", "")
    dados["serie"] = gettxt(inf, "serie", "")
    dados["dhEmi"] = gettxt(inf, "dhEmi", "") or find_any_text(root, ["dEmi", "dhEmi"], "")
    dados["chave"] = gettxt(inf, "chNFCom", "") or find_any_text(root, ["chnfcom", "chnfe", "chnf"], "")

    dados["indContrib"] = find_any_text(root, ["indiedest", "indcontrib", "contribuinte"], default="")
    dados["referencia"] = find_any_text(root, ["mesref", "dref", "competencia", "ref"], default="")
    dados["vencimento"] = find_any_text(root, ["dvenc", "dhvenc", "dvcto", "venc"], default="")

    dados["emit_nome"] = gettxt(emit, "xNome", "") if emit is not None else ""
    dados["emit_cnpj"] = gettxt(emit, "CNPJ", "") if emit is not None else ""
    dados["emit_cpf"] = gettxt(emit, "CPF", "") if emit is not None else ""
    dados["emit_ie"] = gettxt(emit, "IE", "") if emit is not None else ""
    dados["emit_ender"] = fmt_addr(end_emit)

    dados["dest_nome"] = gettxt(dest, "xNome", "") if dest is not None else ""
    dados["dest_cnpj"] = gettxt(dest, "CNPJ", "") if dest is not None else ""
    dados["dest_cpf"] = gettxt(dest, "CPF", "") if dest is not None else ""
    dados["dest_ie"] = gettxt(dest, "IE", "") if dest is not None else ""
    dados["dest_ender"] = fmt_addr(end_dest)

    dados["vProd"] = get0(total, "vProd")
    dados["vDesc"] = get0(total, "vDesc")
    dados["vOutro"] = get0(total, "vOutro")
    dados["vNF"] = get0(total, "vNF")

    dados["vBC"] = get0(icms_tot, "vBC")
    dados["vICMS"] = get0(icms_tot, "vICMS")
    dados["vICMSDeson"] = get0(icms_tot, "vICMSDeson")
    dados["vFCP"] = get0(icms_tot, "vFCP")

    dados["vPIS"] = get0(total, "vPIS")
    dados["vCOFINS"] = get0(total, "vCOFINS")
    dados["vFUST"] = get0(total, "vFUST")
    dados["vFUNTTEL"] = get0(total, "vFUNTTEL")

    vRetTribTot = total.find(".//n:vRetTribTot", NF_NS) if total is not None else None
    dados["vRetPIS"] = get0(vRetTribTot, "vRetPIS")
    dados["vRetCofins"] = get0(vRetTribTot, "vRetCofins")
    dados["vRetCSLL"] = get0(vRetTribTot, "vRetCSLL")
    dados["vIRRF"] = get0(vRetTribTot, "vIRRF")

    itens = []
    for det in inf.findall(".//n:det", NF_NS):
        item = {"cClass": "", "xProd": "", "qCom": "", "vProd": ""}
        prod = det.find(".//n:prod", NF_NS)
        if prod is not None:
            item["cClass"] = gettxt(prod, "cClass", "")
            item["xProd"] = gettxt(prod, "xProd", "")
            item["qCom"] = gettxt(prod, "qCom", "")
            item["vProd"] = gettxt(prod, "vProd", "")

        if item["cClass"] or item["xProd"]:
            itens.append(item)

    dados["itens"] = itens
    return dados, None


# =========================
# EXTRAÇÃO GENÉRICA PARA CSV
# =========================
def _tagname(el_tag: str) -> str:
    if "}" in el_tag:
        return el_tag.split("}", 1)[1]
    return el_tag


def _find_first_by_tag_anyns(root, tagname: str):
    tn = tagname.strip()
    if not tn:
        return None
    for el in root.iter():
        if _tagname(el.tag) == tn:
            return el
    return None


def _find_by_path_anyns(root, path: str):
    p = (path or "").strip().strip("/")
    if not p:
        return None

    parts = [x.strip() for x in p.split("/") if x.strip()]
    if len(parts) == 1:
        return _find_first_by_tag_anyns(root, parts[0])

    first = parts[0]
    for cand in root.iter():
        if _tagname(cand.tag) != first:
            continue
        cur = cand
        ok = True
        for seg in parts[1:]:
            nxt = None
            for ch in list(cur):
                if _tagname(ch.tag) == seg:
                    nxt = ch
                    break
            if nxt is None:
                ok = False
                break
            cur = nxt
        if ok:
            return cur
    return None


def extrair_valor_csv(xml_path: str | Path, campo: str) -> str:
    try:
        tree = ET.parse(str(xml_path))
        root = tree.getroot()
    except Exception:
        return ""

    campo = (campo or "").strip()
    if not campo:
        return ""

    inf = root.find(".//n:infNFCom", NF_NS)
    if inf is None:
        for el in root.iter():
            if _tagname(el.tag) == "infNFCom":
                inf = el
                break

    if campo.startswith("@"):
        att = campo[1:].strip()
        if not att:
            return ""
        if inf is not None and att in inf.attrib:
            return str(inf.attrib.get(att, "")).strip()
        return str(root.attrib.get(att, "")).strip()

    if inf is not None:
        el = _find_by_path_anyns(inf, campo)
        if el is not None and el.text:
            return el.text.strip()

    el = _find_by_path_anyns(root, campo)
    if el is not None and el.text:
        return el.text.strip()

    return ""


def gerar_csv_relatorio_bytes(
    xml_paths: List[Path],
    mapping: List[Tuple[str, str]],
    delimiter: str = ";",
    utf8_sig: bool = True,
) -> bytes:
    """
    Gera CSV em bytes (bom para download web).
    mapping: [(cabecalho, campo), ...]
    """
    if not mapping:
        raise ValueError("mapping vazio (nenhuma coluna).")

    headers = [cab for cab, _ in mapping]

    enc = "utf-8-sig" if utf8_sig else "utf-8"
    bio = io.StringIO()
    w = csv.writer(bio, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
    w.writerow(headers)

    for x in xml_paths:
        row = [extrair_valor_csv(x, campo) for _, campo in mapping]
        w.writerow(row)

    return bio.getvalue().encode(enc, errors="ignore")


def gerar_csv_relatorio_zip(
    zip_bytes: bytes,
    mapping: List[Tuple[str, str]],
    delimiter: str = ";",
    utf8_sig: bool = True,
    incluir_subpastas: bool = True,
    nome_arquivo: str = "relatorio_nfcom.csv",
) -> bytes:
    """
    Recebe ZIP com XMLs e devolve ZIP contendo um CSV pronto.
    """
    tmpdir = Path(tempfile.mkdtemp())
    entrada = tmpdir / "entrada"
    entrada.mkdir()
    zip_in = tmpdir / "in.zip"
    zip_in.write_bytes(zip_bytes)

    with zipfile.ZipFile(zip_in, "r") as z:
        z.extractall(entrada)

    xmls = list(entrada.rglob("*.xml")) if incluir_subpastas else list(entrada.glob("*.xml"))
    csv_bytes = gerar_csv_relatorio_bytes(xmls, mapping, delimiter=delimiter, utf8_sig=utf8_sig)

    zip_out = tmpdir / "out.zip"
    with zipfile.ZipFile(zip_out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(nome_arquivo, csv_bytes)

    return zip_out.read_bytes()


# =========================
# PDF NFCom
# =========================
def gerar_pdf_nfcom(dados: dict, pdf_path: str | Path) -> None:
    ensure_reportlab()
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import A4

    pdf_path = Path(pdf_path)
    c = canvas.Canvas(str(pdf_path), pagesize=A4)
    W, H = A4

    M = 40
    GAP = 10

    def box_top(x, top_y, w, h, title=None):
        y = top_y - h
        c.rect(x, y, w, h, stroke=1, fill=0)
        if title:
            c.setFont("Helvetica-Bold", 9)
            c.drawString(x + 5, top_y - 12, title)
        return y

    def text_top(x, y, s, bold=False, size=9):
        c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        c.drawString(x, y, s)

    def wrap_lines(s, max_chars):
        s = (s or "").strip()
        if not s:
            return []
        words = s.split()
        lines, cur = [], ""
        for w in words:
            if len(cur) + len(w) + 1 <= max_chars:
                cur = (cur + " " + w).strip()
            else:
                lines.append(cur)
                cur = w
        if cur:
            lines.append(cur)
        return lines

    y = H - M
    text_top(M, y, "DOCUMENTO AUXILIAR NFCom (SISTEMA CONTAXX)", bold=True, size=12)
    y -= 18
    text_top(M, y, f"Chave: {dados.get('chave','')}", size=9); y -= 14
    text_top(M, y, f"Nº: {dados.get('nNF','')}   Série: {dados.get('serie','')}   Emissão: {dados.get('dhEmi','')}", size=9); y -= 14
    text_top(M, y, f"Referência: {dados.get('referencia','')}   Vencimento: {dados.get('vencimento','')}", size=9); y -= 18

    box_h = 120
    left_w = (W - 2*M - GAP) / 2
    top_boxes = y

    box_top(M, top_boxes, left_w, box_h, "EMITENTE")
    box_top(M + left_w + GAP, top_boxes, left_w, box_h, "CLIENTE / DESTINATÁRIO")

    emit_x = M + 8
    emit_y = top_boxes - 28
    emit_id = dados.get("emit_cnpj") or dados.get("emit_cpf") or ""
    text_top(emit_x, emit_y, f"Nome: {dados.get('emit_nome','')}", size=9); emit_y -= 13
    text_top(emit_x, emit_y, f"CNPJ/CPF: {emit_id}", size=9); emit_y -= 13
    text_top(emit_x, emit_y, f"IE: {dados.get('emit_ie','')}", size=9); emit_y -= 13

    addr_emit = (dados.get("emit_ender","") or "").replace("\n", " ")
    for line in wrap_lines("End: " + addr_emit, 55)[:2]:
        text_top(emit_x, emit_y, line, size=9); emit_y -= 13

    dest_x = M + left_w + GAP + 8
    dest_y = top_boxes - 28
    dest_id = dados.get("dest_cnpj") or dados.get("dest_cpf") or ""
    text_top(dest_x, dest_y, f"Nome: {dados.get('dest_nome','')}", size=9); dest_y -= 13
    text_top(dest_x, dest_y, f"CNPJ/CPF: {dest_id}", size=9); dest_y -= 13
    text_top(dest_x, dest_y, f"IE: {dados.get('dest_ie','')}", size=9); dest_y -= 13

    addr_dest = (dados.get("dest_ender","") or "").replace("\n", " ")
    for line in wrap_lines("End: " + addr_dest, 55)[:2]:
        text_top(dest_x, dest_y, line, size=9); dest_y -= 13

    y = top_boxes - box_h - 12

    contrib_h = 35
    box_top(M, y, W - 2*M, contrib_h, "CONTRIBUINTE")
    text_top(M + 8, y - 22, f"Indicador/Info: {dados.get('indContrib','')}", size=9)
    y = y - contrib_h - 12

    tot_h = 120
    top_tot = y
    box_top(M, top_tot, left_w, tot_h, "TOTAIS")
    box_top(M + left_w + GAP, top_tot, left_w, tot_h, "INFORMAÇÕES DOS TRIBUTOS")

    tx = M + 8
    ty = top_tot - 28

    def tline(lbl, val):
        nonlocal ty
        text_top(tx, ty, f"{lbl}: {val}", size=9)
        ty -= 13

    tline("VALOR NF", dados.get("vNF","0.00"))
    tline("Total Base Cálculo (vBC)", dados.get("vBC","0.00"))
    tline("Valor ICMS (vICMS)", dados.get("vICMS","0.00"))
    tline("Valor Isento (vICMSDeson)", dados.get("vICMSDeson","0.00"))
    tline("Valor Outros (vOutro)", dados.get("vOutro","0.00"))
    tline("Desconto (vDesc)", dados.get("vDesc","0.00"))

    tx2 = M + left_w + GAP + 8
    ty2 = top_tot - 28

    def tline2(lbl, val):
        nonlocal ty2
        text_top(tx2, ty2, f"{lbl}: {val}", size=9)
        ty2 -= 13

    tline2("PIS", dados.get("vPIS","0.00"))
    tline2("COFINS", dados.get("vCOFINS","0.00"))
    tline2("FUST", dados.get("vFUST","0.00"))
    tline2("FUNTTEL", dados.get("vFUNTTEL","0.00"))
    tline2("Ret PIS", dados.get("vRetPIS","0.00"))
    tline2("Ret Cofins", dados.get("vRetCofins","0.00"))

    y = top_tot - tot_h - 12

    itens_h = 220
    top_it = y
    box_top(M, top_it, W - 2*M, itens_h, "ITENS")

    c.setFont("Helvetica-Bold", 9)
    c.drawString(M + 10, top_it - 30, "cClass")
    c.drawString(M + 70, top_it - 30, "Descrição")
    c.drawRightString(W - M - 120, top_it - 30, "Qtd")
    c.drawRightString(W - M - 10, top_it - 30, "Valor")

    c.setFont("Helvetica", 9)
    y_line = top_it - 45
    for it in (dados.get("itens") or [])[:10]:
        c.drawString(M + 10, y_line, (it.get("cClass") or "")[:12])
        c.drawString(M + 70, y_line, (it.get("xProd") or "")[:70])
        c.drawRightString(W - M - 120, y_line, (it.get("qCom") or "")[:12])
        c.drawRightString(W - M - 10, y_line, (it.get("vProd") or "")[:12])
        y_line -= 14
        if y_line < (top_it - itens_h + 20):
            break

    c.setFont("Helvetica", 8)
    c.drawString(M, 25, "PDF gerado automaticamente a partir do XML NFCom.")
    c.showPage()
    c.save()


# =========================
# RESUMO (Somatório por cClass)
# =========================
def _to_float(valor_txt: str) -> float:
    if valor_txt is None:
        return 0.0
    s = str(valor_txt).strip()
    if not s:
        return 0.0
    s = s.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _fmt_br(v: float) -> str:
    try:
        return f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0,00"


def _classificar_coluna_por_cclass(cclass: str) -> str:
    cc = (cclass or "").strip()
    if cc.startswith("06"):
        return "sva"
    if cc.startswith("11"):
        return "apps"
    return "scm"


def montar_relatorio_resumo(
    pasta: str | Path,
    incluir_subpastas: bool,
    mapa_desc: Dict[str, str],
) -> dict:
    base = Path(pasta)
    if not base.exists():
        return {"linhas_item": [], "linhas_cclass": [], "total_geral": 0.0, "total_notas_distintas": 0, "labels": [], "valores": [], "total_arquivos": 0}

    arquivos = list(base.rglob("*.xml")) if incluir_subpastas else list(base.glob("*.xml"))
    total_arquivos = len(arquivos)

    por_item: Dict[Tuple[str, str], dict] = {}
    por_cclass: Dict[str, dict] = {}

    for f in arquivos:
        dados, err = parse_nfcom(f)
        if err or not dados:
            continue

        nota_id = (dados.get("chave") or "").strip() or f.name

        for it in (dados.get("itens", []) or []):
            cclass = (it.get("cClass") or "").strip() or "SEM_CCLASS"
            xprod = (it.get("xProd") or "").strip() or "(Sem descrição do item)"
            vprod = _to_float(it.get("vProd", "0"))

            k = (xprod, cclass)
            if k not in por_item:
                por_item[k] = {"notas": set(), "v_scm": 0.0, "v_sva": 0.0, "v_apps": 0.0, "v_total": 0.0}
            por_item[k]["notas"].add(nota_id)

            col = _classificar_coluna_por_cclass(cclass)
            if col == "sva":
                por_item[k]["v_sva"] += vprod
            elif col == "apps":
                por_item[k]["v_apps"] += vprod
            else:
                por_item[k]["v_scm"] += vprod
            por_item[k]["v_total"] += vprod

            if cclass not in por_cclass:
                por_cclass[cclass] = {"notas": set(), "v_total": 0.0, "qtd_itens": 0}
            por_cclass[cclass]["notas"].add(nota_id)
            por_cclass[cclass]["qtd_itens"] += 1
            por_cclass[cclass]["v_total"] += vprod

    total_geral = sum(v["v_total"] for v in por_item.values()) or 0.0

    linhas_item = []
    for (xprod, cclass), info in por_item.items():
        qtd_notas = len(info["notas"])
        v_scm = info["v_scm"]
        v_sva = info["v_sva"]
        v_apps = info["v_apps"]
        v_total = info["v_total"]
        pct = (v_total / total_geral * 100.0) if total_geral else 0.0
        linhas_item.append((xprod, cclass, qtd_notas, v_scm, v_sva, v_apps, v_total, pct))
    linhas_item.sort(key=lambda r: r[6], reverse=True)

    linhas_cclass = []
    for cclass, info in por_cclass.items():
        descricao = mapa_desc.get(cclass, "") or "(sem descrição na Tabela-cClass)"
        linhas_cclass.append((cclass, len(info["notas"]), info.get("qtd_itens", 0), info["v_total"], descricao))
    linhas_cclass.sort(key=lambda r: r[3], reverse=True)

    total_notas_distintas = len({nid for info in por_cclass.values() for nid in info["notas"]})

    top = linhas_cclass[:20]
    labels = [r[0] for r in top]
    valores = [float(r[3] or 0.0) for r in top]

    return {
        "linhas_item": linhas_item,
        "linhas_cclass": linhas_cclass,
        "total_geral": float(total_geral or 0.0),
        "total_notas_distintas": int(total_notas_distintas),
        "labels": labels,
        "valores": valores,
        "total_arquivos": total_arquivos,
    }
