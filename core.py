from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET


# =========================
# Util
# =========================
def _br_money(v: float) -> str:
    # 1234.56 -> "R$ 1.234,56"
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def _to_float(s: str | None) -> float:
    if not s:
        return 0.0
    s = str(s).strip()
    if not s:
        return 0.0
    # aceita "1234.56" e "1.234,56"
    s = s.replace(".", "").replace(",", ".") if "," in s else s
    try:
        return float(s)
    except Exception:
        return 0.0


def _strip_namespaces(root: ET.Element) -> ET.Element:
    # Remove namespaces in-place
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    return root


def _zip_iter_files(zip_bytes: bytes) -> List[Tuple[str, bytes]]:
    out = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
        for name in z.namelist():
            if name.endswith("/"):
                continue
            data = z.read(name)
            out.append((name, data))
    return out


# =========================
# cClass (Excel)
# =========================
def carregar_cclass_lista(xlsx_path: str = "data/Tabela-cClass.xlsx") -> List[Dict[str, str]]:
    """
    Retorna lista para dropdown:
      [{"code":"0600601","desc":"..."}]
    """

    # tenta caminhos comuns (pra evitar dor no Render)
    candidatos = [
        Path(xlsx_path),
        Path("Tabela-cClass.xlsx"),
        Path("data") / "Tabela-cClass.xlsx",
    ]
    caminho = None
    for c in candidatos:
        if c.exists():
            caminho = c
            break
    if caminho is None:
        # sem excel: devolve lista vazia (o site ainda roda)
        return []

    import openpyxl

    wb = openpyxl.load_workbook(caminho, data_only=True)
    ws = wb.active

    # tenta achar colunas pelo cabeçalho
    header = []
    for col in range(1, ws.max_column + 1):
        header.append(str(ws.cell(1, col).value or "").strip().lower())

    def find_col(*names: str) -> int | None:
        for n in names:
            n = n.lower()
            for i, h in enumerate(header, start=1):
                if h == n:
                    return i
        return None

    col_code = find_col("cclass", "codigo", "código", "code")
    col_desc = find_col("descricao", "descrição", "descricao do item", "descrição do item", "desc")

    # fallback: se não achar, assume 1 e 2
    if col_code is None:
        col_code = 1
    if col_desc is None:
        col_desc = 2

    lista: List[Dict[str, str]] = []
    for row in range(2, ws.max_row + 1):
        code = str(ws.cell(row, col_code).value or "").strip()
        desc = str(ws.cell(row, col_desc).value or "").strip()
        if code:
            lista.append({"code": code, "desc": desc})

    return lista


def cclass_desc_map(cclass_lista: List[Dict[str, str]]) -> Dict[str, str]:
    return {i["code"]: i.get("desc", "") for i in cclass_lista}


# =========================
# Regras (texto)
# =========================
def parse_regras_texto(txt: str) -> Dict[str, str]:
    """
    Formato por linha: cClass;CFOP
    Ex:
      060101;5102
      110201;5102
    """
    regras: Dict[str, str] = {}
    for line in (txt or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # aceita ; ou , ou :
        if ";" in line:
            parts = [p.strip() for p in line.split(";", 1)]
        elif "," in line:
            parts = [p.strip() for p in line.split(",", 1)]
        elif ":" in line:
            parts = [p.strip() for p in line.split(":", 1)]
        else:
            continue
        if len(parts) == 2 and parts[0] and parts[1]:
            regras[parts[0]] = parts[1]
    return regras


# =========================
# Aplicar regras no XML (lote)
# =========================
def aplicar_regras_xml_str(
    xml_str: str,
    regras: Dict[str, str],
    remover_desconto: bool = False,
    remover_outros: bool = False,
) -> str:
    novo = xml_str

    if remover_desconto:
        novo = re.sub(r"<vDesc>.*?</vDesc>", "", novo, flags=re.DOTALL)

    if remover_outros:
        novo = re.sub(r"<vOutro>.*?</vOutro>", "", novo, flags=re.DOTALL)

    # Insere CFOP logo após cClass quando não existir CFOP em seguida
    for cclass, cfop in regras.items():
        padrao = rf"(<cClass>{re.escape(cclass)}</cClass>)(?!\s*<CFOP>)"
        novo = re.sub(padrao, rf"\1<CFOP>{cfop}</CFOP>", novo)

    return novo


def processar_lote_zip(
    zip_bytes: bytes,
    regras: Dict[str, str],
    remover_desconto: bool = False,
    remover_outros: bool = False,
) -> bytes:
    """
    Entrada: ZIP com XML/TXT
    Saída: ZIP com XML editados (TXT é copiado)
    """
    in_files = _zip_iter_files(zip_bytes)
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name, data in in_files:
            low = name.lower()
            if low.endswith(".xml"):
                try:
                    s = data.decode("utf-8", errors="ignore")
                    s2 = aplicar_regras_xml_str(s, regras, remover_desconto, remover_outros)
                    zout.writestr(name, s2.encode("utf-8"))
                except Exception:
                    # se falhar, copia original
                    zout.writestr(name, data)
            else:
                zout.writestr(name, data)

    mem.seek(0)
    return mem.read()


# =========================
# Parse NFCom (para Nota/Resumo)
# =========================
@dataclass
class ItemNF:
    cclass: str
    vprod: float


def parse_nfcom(xml_bytes: bytes) -> List[ItemNF]:
    """
    Extrai itens com cClass e vProd.
    Remove namespaces e tenta associar vProd ao mesmo "bloco" do cClass.
    """
    root = ET.fromstring(xml_bytes)
    root = _strip_namespaces(root)

    itens: List[ItemNF] = []

    # pega todos cClass e tenta achar vProd no pai (ou no avô)
    for c_el in root.findall(".//cClass"):
        cclass = (c_el.text or "").strip()
        if not cclass:
            continue

        vprod = 0.0

        parent = _find_parent(root, c_el)
        if parent is not None:
            vp = parent.find(".//vProd")
            if vp is not None and (vp.text or "").strip():
                vprod = _to_float(vp.text)
            else:
                grand = _find_parent(root, parent)
                if grand is not None:
                    vp2 = grand.find(".//vProd")
                    if vp2 is not None and (vp2.text or "").strip():
                        vprod = _to_float(vp2.text)

        itens.append(ItemNF(cclass=cclass, vprod=vprod))

    return itens


def _find_parent(root: ET.Element, child: ET.Element) -> ET.Element | None:
    # ElementTree não guarda parent. Faz busca linear.
    for p in root.iter():
        for c in list(p):
            if c is child:
                return p
    return None


# =========================
# RESUMO (ZIP -> Totais por cClass)
# =========================
def gerar_resumo_de_zip(zip_bytes: bytes, desc_map: Dict[str, str] | None = None) -> dict:
    desc_map = desc_map or {}

    arquivos = [(n, b) for (n, b) in _zip_iter_files(zip_bytes) if n.lower().endswith(".xml")]
    total_arquivos = len(arquivos)

    por_cclass: Dict[str, Dict[str, float]] = {}
    total_geral = 0.0

    for _, xmlb in arquivos:
        try:
            itens = parse_nfcom(xmlb)
        except Exception:
            continue

        for it in itens:
            cc = it.cclass
            por_cclass.setdefault(cc, {"qtd_itens": 0.0, "v_total": 0.0})
            por_cclass[cc]["qtd_itens"] += 1
            por_cclass[cc]["v_total"] += float(it.vprod)
            total_geral += float(it.vprod)

    # monta linhas ordenadas
    linhas = []
    for cc, agg in por_cclass.items():
        v_total = float(agg["v_total"])
        qtd = int(agg["qtd_itens"])
        pct = (v_total / total_geral * 100.0) if total_geral > 0 else 0.0
        linhas.append(
            {
                "cClass": cc,
                "descricao": desc_map.get(cc, ""),
                "qtd_itens": qtd,
                "v_total": v_total,
                "v_total_br": _br_money(v_total),
                "pct": pct,
                "pct_br": f"{pct:.2f}".replace(".", ","),
            }
        )

    linhas.sort(key=lambda x: x["v_total"], reverse=True)

    # top 12 pro gráfico
    top = linhas[:12]
    labels = [f'{r["cClass"]}' for r in top]
    valores = [round(float(r["v_total"]), 2) for r in top]

    return {
        "total_arquivos": total_arquivos,
        "total_geral": total_geral,
        "total_geral_br": _br_money(total_geral),
        "linhas": linhas,
        "labels": labels,
        "valores": valores,
    }


# =========================
# CSV (ZIP -> CSV)
# =========================
def gerar_csv_de_zip(zip_bytes: bytes, mapping: List[Tuple[str, str]]) -> bytes:
    """
    mapping: [(cabecalho, tag_xml), ...]
    Gera 1 linha por XML.
    """
    arquivos = [(n, b) for (n, b) in _zip_iter_files(zip_bytes) if n.lower().endswith(".xml")]

    bio = io.StringIO()
    w = csv.writer(bio, delimiter=";")
    w.writerow([m[0] for m in mapping])

    for _, xmlb in arquivos:
        try:
            root = ET.fromstring(xmlb)
            root = _strip_namespaces(root)
        except Exception:
            w.writerow(["" for _ in mapping])
            continue

        row = []
        for _, campo in mapping:
            el = root.find(f".//{campo}")
            row.append((el.text or "").strip() if el is not None else "")
        w.writerow(row)

    return bio.getvalue().encode("utf-8-sig")
