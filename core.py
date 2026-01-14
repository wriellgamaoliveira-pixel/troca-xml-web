from __future__ import annotations

from pathlib import Path
import csv
import io
import re
import tempfile
import zipfile
from typing import Dict, List, Tuple, Any, Optional

import xml.etree.ElementTree as ET

NF_NS = {"n": "http://www.portalfiscal.inf.br/nfcom"}
EXTENSOES = [".xml", ".txt"]

TAGS_DESCONTO = ["<vDesc>", "</vDesc>"]
TAGS_OUTROS = ["<vOutro>", "</vOutro>"]


# =========================
# Utilidades
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


def parse_regras_texto(regras_texto: str) -> Dict[str, str]:
    """
    Espera linhas no formato:
      cClass;CFOP
    Ex:
      060101;5102
    """
    regras: Dict[str, str] = {}
    for ln in (regras_texto or "").splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        if ";" in ln:
            cclass, cfop = ln.split(";", 1)
        elif "," in ln:
            cclass, cfop = ln.split(",", 1)
        else:
            continue
        cclass = cclass.strip()
        cfop = cfop.strip()
        if cclass and cfop:
            regras[cclass] = cfop
    return regras


# =========================
# Regras de edição
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

    # Insere <CFOP> depois do <cClass> quando não existe
    for cclass, cfop in (regras_cclass_cfop or {}).items():
        if not str(cfop).strip():
            continue
        padrao = rf"(<cClass>{re.escape(str(cclass))}</cClass>)(?!\s*<CFOP>)(\s*<uMed>)"
        substitui = rf"\1<CFOP>{cfop}</CFOP>\2"
        novo = re.sub(padrao, substitui, novo)

    return novo


def processar_lote_zip(
    zip_bytes: bytes,
    regras_cclass_cfop: Dict[str, str],
    remover_desconto: bool = False,
    remover_outros: bool = False,
) -> bytes:
    """
    Entrada: ZIP com XML/TXT.
    Saída: ZIP com arquivos editados mantendo a estrutura.
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

    for f in entrada.rglob("*"):
        if not f.is_file():
            continue
        if f.suffix.lower() not in EXTENSOES:
            continue

        original = f.read_text(encoding="utf-8", errors="ignore")
        novo = aplicar_regras_texto(original, regras_cclass_cfop, remover_desconto, remover_outros)

        out = saida / f.relative_to(entrada)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(novo, encoding="utf-8", errors="ignore")

    zip_out = tmpdir / "out.zip"
    with zipfile.ZipFile(zip_out, "w", zipfile.ZIP_DEFLATED) as z:
        for p in saida.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(saida))

    return zip_out.read_bytes()


def processar_xml_unico(
    xml_bytes: bytes,
    regras_cclass_cfop: Dict[str, str],
    remover_desconto: bool = False,
    remover_outros: bool = False,
) -> bytes:
    texto = xml_bytes.decode("utf-8", errors="ignore")
    novo = aplicar_regras_texto(texto, regras_cclass_cfop, remover_desconto, remover_outros)
    return novo.encode("utf-8", errors="ignore")


# =========================
# NFCom: parse básico para tela "Nota Única"
# =========================
def _tagname(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def _find_any_text(root, endings, default=""):
    if root is None:
        return default
    for el in root.iter():
        t = (_tagname(el.tag) or "").lower()
        for end in endings:
            if t.endswith(end.lower()):
                if el.text and el.text.strip():
                    return el.text.strip()
    return default


def parse_nfcom_from_bytes(xml_bytes: bytes) -> Tuple[Optional[dict], Optional[str]]:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        return None, f"Erro lendo XML: {e}"

    inf = root.find(".//n:infNFCom", NF_NS)
    if inf is None:
        for el in root.iter():
            if _tagname(el.tag) == "infNFCom":
                inf = el
                break

    if inf is None:
        return None, "Não encontrei o bloco infNFCom no XML."

    def gettxt(parent, tag, default=""):
        if parent is None:
            return default
        el = parent.find(f"n:{tag}", NF_NS)
        if el is None:
            # fallback sem namespace
            for ch in list(parent):
                if _tagname(ch.tag) == tag:
                    el = ch
                    break
        if el is None or el.text is None:
            return default
        return el.text.strip()

    emit = inf.find("n:emit", NF_NS)
    dest = inf.find("n:dest", NF_NS)

    total = inf.find(".//n:total", NF_NS)
    icms_tot = total.find(".//n:ICMSTot", NF_NS) if total is not None else None

    dados: Dict[str, Any] = {}
    dados["nNF"] = gettxt(inf, "nNF", "")
    dados["serie"] = gettxt(inf, "serie", "")
    dados["dhEmi"] = gettxt(inf, "dhEmi", "") or _find_any_text(root, ["dEmi", "dhEmi"], "")
    dados["chave"] = gettxt(inf, "chNFCom", "") or _find_any_text(root, ["chNFCom", "chNFe", "chNF"], "")
    dados["emit_nome"] = gettxt(emit, "xNome", "")
    dados["dest_nome"] = gettxt(dest, "xNome", "")

    def get0(parent, tag, default="0.00"):
        v = gettxt(parent, tag, default)
        return v if v else default

    dados["vNF"] = get0(total, "vNF")
    dados["vDesc"] = get0(total, "vDesc")
    dados["vOutro"] = get0(total, "vOutro")
    dados["vBC"] = get0(icms_tot, "vBC")
    dados["vICMS"] = get0(icms_tot, "vICMS")

    itens = []
    for det in inf.findall(".//n:det", NF_NS):
        prod = det.find(".//n:prod", NF_NS)
        if prod is None:
            continue
        itens.append(
            {
                "cClass": gettxt(prod, "cClass", ""),
                "xProd": gettxt(prod, "xProd", ""),
                "qCom": gettxt(prod, "qCom", ""),
                "vProd": gettxt(prod, "vProd", ""),
            }
        )
    dados["itens"] = itens

    return dados, None


# =========================
# CSV: mapeamento simples (cabecalho;campo)
# campo pode ser:
#  - tag simples: "nNF"
#  - caminho: "emit/xNome" (busca por tags ignorando namespace)
#  - atributo: "@versao"
# =========================
def _find_by_path_anyns(root, path: str):
    p = (path or "").strip().strip("/")
    if not p:
        return None
    parts = [x.strip() for x in p.split("/") if x.strip()]

    # busca o primeiro segmento em qualquer nível
    first = parts[0]
    for cand in root.iter():
        if _tagname(cand.tag) == first:
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


def extrair_valor_csv(xml_bytes: bytes, campo: str) -> str:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return ""

    campo = (campo or "").strip()
    if not campo:
        return ""

    # atributo
    if campo.startswith("@"):
        att = campo[1:].strip()
        return str(root.attrib.get(att, "")).strip()

    # tenta dentro de infNFCom primeiro
    inf = root.find(".//n:infNFCom", NF_NS)
    if inf is None:
        for el in root.iter():
            if _tagname(el.tag) == "infNFCom":
                inf = el
                break

    if inf is not None:
        el = _find_by_path_anyns(inf, campo)
        if el is not None and el.text:
            return el.text.strip()

    el = _find_by_path_anyns(root, campo)
    if el is not None and el.text:
        return el.text.strip()

    return ""


def parse_mapping_text(mapping_text: str) -> List[Tuple[str, str]]:
    """
    Linhas:
      Cabeçalho;campo
    Ex:
      Numero;nNF
      Emitente;emit/xNome
    """
    out: List[Tuple[str, str]] = []
    for ln in (mapping_text or "").splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        if ";" in ln:
            a, b = ln.split(";", 1)
        elif "," in ln:
            a, b = ln.split(",", 1)
        else:
            continue
        a = a.strip()
        b = b.strip()
        if a and b:
            out.append((a, b))
    return out


def gerar_csv_de_zip(zip_bytes: bytes, mapping: List[Tuple[str, str]], delimiter: str = ";") -> bytes:
    tmpdir = Path(tempfile.mkdtemp())
    entrada = tmpdir / "entrada"
    entrada.mkdir()

    zip_in = tmpdir / "in.zip"
    zip_in.write_bytes(zip_bytes)
    with zipfile.ZipFile(zip_in, "r") as z:
        z.extractall(entrada)

    xmls = list(entrada.rglob("*.xml"))

    bio = io.StringIO()
    w = csv.writer(bio, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
    w.writerow([cab for cab, _ in mapping])

    for p in xmls:
        xbytes = p.read_bytes()
        row = [extrair_valor_csv(xbytes, campo) for _, campo in mapping]
        w.writerow(row)

    return bio.getvalue().encode("utf-8-sig", errors="ignore")


# =========================
# RESUMO: somatório por cClass
# =========================
def resumo_de_zip(zip_bytes: bytes) -> dict:
    tmpdir = Path(tempfile.mkdtemp())
    entrada = tmpdir / "entrada"
    entrada.mkdir()

    zip_in = tmpdir / "in.zip"
    zip_in.write_bytes(zip_bytes)
    with zipfile.ZipFile(zip_in, "r") as z:
        z.extractall(entrada)

    xmls = list(entrada.rglob("*.xml"))
    por_cclass: Dict[str, Dict[str, Any]] = {}

    for p in xmls:
        dados, err = parse_nfcom_from_bytes(p.read_bytes())
        if err or not dados:
            continue
        for it in dados.get("itens", []):
            cclass = (it.get("cClass") or "").strip() or "SEM_CCLASS"
            v = _to_float(it.get("vProd") or "0")
            if cclass not in por_cclass:
                por_cclass[cclass] = {"v_total": 0.0, "qtd_itens": 0}
            por_cclass[cclass]["v_total"] += v
            por_cclass[cclass]["qtd_itens"] += 1

    linhas = []
    total_geral = sum(x["v_total"] for x in por_cclass.values()) or 0.0
    for cclass, info in por_cclass.items():
        v = float(info["v_total"])
        pct = (v / total_geral * 100.0) if total_geral else 0.0
        linhas.append({"cClass": cclass, "qtd_itens": info["qtd_itens"], "v_total": v, "pct": pct})
    linhas.sort(key=lambda r: r["v_total"], reverse=True)

    # dados pro gráfico (pizza)
    labels = [r["cClass"] for r in linhas[:12]]
    valores = [r["v_total"] for r in linhas[:12]]

    return {
        "total_arquivos": len(xmls),
        "total_geral": total_geral,
        "linhas": linhas,
        "labels": labels,
        "valores": valores,
    }


# =========================
# PDF simples (Nota Única)
# =========================
def gerar_pdf_nfcom_bytes(dados: dict) -> bytes:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import A4

    bio = io.BytesIO()
    c = canvas.Canvas(bio, pagesize=A4)
    W, H = A4
    y = H - 40

    def line(txt, bold=False, size=10, dy=14):
        nonlocal y
        c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        c.drawString(40, y, txt)
        y -= dy

    line("DOCUMENTO AUXILIAR NFCom (WEB)", bold=True, size=12, dy=20)
    line(f"Chave: {dados.get('chave','')}", size=9)
    line(f"Nº: {dados.get('nNF','')}  Série: {dados.get('serie','')}  Emissão: {dados.get('dhEmi','')}", size=9)
    y -= 10
    line(f"Emitente: {dados.get('emit_nome','')}", size=10)
    line(f"Destinatário: {dados.get('dest_nome','')}", size=10)
    y -= 10
    line(f"vNF: {dados.get('vNF','0.00')}  vICMS: {dados.get('vICMS','0.00')}  vDesc: {dados.get('vDesc','0.00')}  vOutro: {dados.get('vOutro','0.00')}", size=9)
    y -= 10

    line("ITENS:", bold=True, size=10)
    for it in (dados.get("itens") or [])[:18]:
        line(f"- {it.get('cClass','')} | {it.get('xProd','')[:70]} | Qtd {it.get('qCom','')} | Valor {it.get('vProd','')}", size=8, dy=12)

    c.showPage()
    c.save()
    return bio.getvalue()
