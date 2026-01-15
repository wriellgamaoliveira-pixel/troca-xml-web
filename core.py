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
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def _br_num(v: float, casas: int = 2) -> str:
    s = f"{v:,.{casas}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def _to_float(s: str | None) -> float:
    if not s:
        return 0.0
    s = str(s).strip()
    if not s:
        return 0.0
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _strip_namespaces(root: ET.Element) -> ET.Element:
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    return root


def _zip_iter_files(zip_bytes: bytes) -> List[Tuple[str, bytes]]:
    out: List[Tuple[str, bytes]] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
        for name in z.namelist():
            if name.endswith("/"):
                continue
            out.append((name, z.read(name)))
    return out


def _find_parent(root: ET.Element, child: ET.Element) -> ET.Element | None:
    for p in root.iter():
        for c in list(p):
            if c is child:
                return p
    return None


def _findtext(root: ET.Element, *paths: str, default: str = "") -> str:
    """
    Tenta vários caminhos .//tag e devolve o primeiro texto encontrado.
    """
    for p in paths:
        el = root.find(p)
        if el is not None and (el.text or "").strip():
            return (el.text or "").strip()
    return default


# =========================
# cClass (Excel)
# =========================
def carregar_cclass_lista(xlsx_path: str = "data/Tabela-cClass.xlsx") -> List[Dict[str, str]]:
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
        return []

    import openpyxl

    wb = openpyxl.load_workbook(caminho, data_only=True)
    ws = wb.active

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

    col_code = find_col("cclass", "codigo", "código", "code", "grupo/código") or 1
    col_desc = find_col("descricao", "descrição", "desc") or 2

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
# Regras (texto) cClass;CFOP
# =========================
def parse_regras_texto(txt: str) -> Dict[str, str]:
    regras: Dict[str, str] = {}
    for line in (txt or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ";" in line:
            a, b = [x.strip() for x in line.split(";", 1)]
        elif "," in line:
            a, b = [x.strip() for x in line.split(",", 1)]
        elif ":" in line:
            a, b = [x.strip() for x in line.split(":", 1)]
        else:
            continue
        if a and b:
            regras[a] = b
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
                    zout.writestr(name, data)
            else:
                zout.writestr(name, data)

    mem.seek(0)
    return mem.read()


# =========================
# RESUMO - Itens (cClass, xProd, vProd)
# =========================
@dataclass
class ItemNF:
    cclass: str
    xprod: str
    vprod: float


def parse_nfcom_itens(xml_bytes: bytes) -> List[ItemNF]:
    root = ET.fromstring(xml_bytes)
    root = _strip_namespaces(root)

    itens: List[ItemNF] = []

    for c_el in root.findall(".//cClass"):
        cclass = (c_el.text or "").strip()
        if not cclass:
            continue

        vprod = 0.0
        xprod = ""

        parent = _find_parent(root, c_el)
        if parent is not None:
            xp = parent.find(".//xProd")
            if xp is not None and (xp.text or "").strip():
                xprod = (xp.text or "").strip()

            vp = parent.find(".//vProd")
            if vp is not None and (vp.text or "").strip():
                vprod = _to_float(vp.text)
            else:
                grand = _find_parent(root, parent)
                if grand is not None:
                    vp2 = grand.find(".//vProd")
                    if vp2 is not None and (vp2.text or "").strip():
                        vprod = _to_float(vp2.text)

        itens.append(ItemNF(cclass=cclass, xprod=xprod, vprod=vprod))

    return itens


def gerar_resumo_de_zip(zip_bytes: bytes, desc_map: Dict[str, str] | None = None) -> dict:
    desc_map = desc_map or {}

    arquivos = [(n, b) for (n, b) in _zip_iter_files(zip_bytes) if n.lower().endswith(".xml")]
    total_arquivos = len(arquivos)

    por_cclass: Dict[str, Dict[str, float]] = {}
    por_item: Dict[Tuple[str, str], Dict[str, float]] = {}
    total_geral = 0.0

    for _, xmlb in arquivos:
        try:
            itens = parse_nfcom_itens(xmlb)
        except Exception:
            continue

        for it in itens:
            cc = it.cclass
            v = float(it.vprod)
            total_geral += v

            por_cclass.setdefault(cc, {"qtd_itens": 0.0, "v_total": 0.0})
            por_cclass[cc]["qtd_itens"] += 1
            por_cclass[cc]["v_total"] += v

            item_nome = (it.xprod or "(sem descrição)").strip()
            key = (item_nome, cc)
            por_item.setdefault(key, {"qtd_itens": 0.0, "v_total": 0.0})
            por_item[key]["qtd_itens"] += 1
            por_item[key]["v_total"] += v

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

    itens_linhas = []
    for (xprod, cc), agg in por_item.items():
        v_total = float(agg["v_total"])
        qtd = int(agg["qtd_itens"])
        pct = (v_total / total_geral * 100.0) if total_geral > 0 else 0.0
        itens_linhas.append(
            {
                "item": xprod,
                "cClass": cc,
                "qtd_itens": qtd,
                "v_total": v_total,
                "v_total_br": _br_money(v_total),
                "pct_br": f"{pct:.2f}".replace(".", ","),
            }
        )
    itens_linhas.sort(key=lambda x: x["v_total"], reverse=True)

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
        "itens_linhas": itens_linhas,
    }


# =========================
# CSV (ZIP -> CSV)
# =========================
def gerar_csv_de_zip(zip_bytes: bytes, mapping: List[Tuple[str, str]]) -> bytes:
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


# =========================
# NOTA (gera o dict "d" usado no templates/resultado.html)
# =========================
def gerar_dados_nota_xml(xml_bytes: bytes) -> dict:
    """
    Monta o dicionário 'd' que o seu template resultado.html espera.
    Se alguma tag não existir, devolve vazio/— sem quebrar a página.
    """
    root = ET.fromstring(xml_bytes)
    root = _strip_namespaces(root)

    # -------- Emitente
    emit_nome = _findtext(root, ".//emit//xNome", default="")
    emit_fantasia = _findtext(root, ".//emit//xFant", default="")
    emit_cnpj = _findtext(root, ".//emit//CNPJ", ".//emit//CPF", default="")
    emit_ie = _findtext(root, ".//emit//IE", default="")
    emit_lgr = _findtext(root, ".//emit//enderEmit//xLgr", default="")
    emit_nro = _findtext(root, ".//emit//enderEmit//nro", default="")
    emit_bairro = _findtext(root, ".//emit//enderEmit//xBairro", default="")
    emit_mun = _findtext(root, ".//emit//enderEmit//xMun", default="")
    emit_uf = _findtext(root, ".//emit//enderEmit//UF", default="")
    emit_cep = _findtext(root, ".//emit//enderEmit//CEP", default="")

    emit_endereco_linha1 = " ".join([x for x in [emit_lgr, emit_nro] if x]).strip()
    emit_endereco_linha2 = " - ".join([x for x in [emit_bairro, emit_mun] if x]).strip()
    if emit_uf:
        emit_endereco_linha2 = (emit_endereco_linha2 + f" / {emit_uf}").strip()
    if emit_cep:
        emit_endereco_linha2 = (emit_endereco_linha2 + f" - CEP {emit_cep}").strip()

    # -------- Destinatário
    dest_nome = _findtext(root, ".//dest//xNome", default="")
    dest_doc = _findtext(root, ".//dest//CNPJ", ".//dest//CPF", default="")
    dest_lgr = _findtext(root, ".//dest//enderDest//xLgr", default="")
    dest_nro = _findtext(root, ".//dest//enderDest//nro", default="")
    dest_bairro = _findtext(root, ".//dest//enderDest//xBairro", default="")
    dest_mun = _findtext(root, ".//dest//enderDest//xMun", default="")
    dest_uf = _findtext(root, ".//dest//enderDest//UF", default="")
    dest_cep = _findtext(root, ".//dest//enderDest//CEP", default="")

    dest_endereco_linha1 = " ".join([x for x in [dest_lgr, dest_nro] if x]).strip()
    dest_endereco_linha2 = " - ".join([x for x in [dest_bairro, dest_mun] if x]).strip()
    if dest_uf:
        dest_endereco_linha2 = (dest_endereco_linha2 + f" / {dest_uf}").strip()
    if dest_cep:
        dest_endereco_linha2 = (dest_endereco_linha2 + f" - CEP {dest_cep}").strip()

    # -------- Dados NF
    nNF = _findtext(root, ".//ide//nNF", default="")
    serie = _findtext(root, ".//ide//serie", default="")
    dhEmi = _findtext(root, ".//ide//dhEmi", ".//ide//dEmi", default="")
    chNFCom = _findtext(root, ".//chNFCom", ".//infNFCom//@Id", default="")

    # Totais básicos
    vNF = _to_float(_findtext(root, ".//total//vNF", ".//total//ICMSTot//vNF", default="0"))
    vBC = _to_float(_findtext(root, ".//total//vBC", ".//total//ICMSTot//vBC", default="0"))
    vICMS = _to_float(_findtext(root, ".//total//vICMS", ".//total//ICMSTot//vICMS", default="0"))
    vIsento = _to_float(_findtext(root, ".//total//vIsento", default="0"))
    vOutro = _to_float(_findtext(root, ".//total//vOutro", default="0"))

    # Tributos (se existirem)
    vPIS = _to_float(_findtext(root, ".//total//vPIS", default="0"))
    vCOFINS = _to_float(_findtext(root, ".//total//vCOFINS", default="0"))
    vFUST = _to_float(_findtext(root, ".//total//vFUST", default="0"))
    vFUNTTEL = _to_float(_findtext(root, ".//total//vFUNTTEL", default="0"))

    # -------- Itens
    itens = []
    for det in root.findall(".//det"):
        cClass = _findtext(det, ".//cClass", default="")
        xProd = _findtext(det, ".//xProd", default="")
        un = _findtext(det, ".//uCom", ".//uUn", default="")
        qtd = _to_float(_findtext(det, ".//qCom", ".//qUn", ".//qtd", default="0"))
        vUnit = _to_float(_findtext(det, ".//vUnCom", ".//vUn", default="0"))
        vTotal = _to_float(_findtext(det, ".//vProd", ".//vItem", default="0"))

        # PIS/COFINS (bem genérico)
        pis = _to_float(_findtext(det, ".//PIS//vPIS", default="0"))
        cof = _to_float(_findtext(det, ".//COFINS//vCOFINS", default="0"))
        pis_cof = pis + cof

        bc_icms = _to_float(_findtext(det, ".//ICMS//vBC", default="0"))
        aliq_icms = _to_float(_findtext(det, ".//ICMS//pICMS", default="0"))
        v_icms = _to_float(_findtext(det, ".//ICMS//vICMS", default="0"))

        itens.append(
            {
                "cClass": cClass,
                "xProd": xProd,
                "un": un,
                "qtd": qtd,
                "qtd_fmt": _br_num(qtd, 2) if qtd else "",
                "vUnit": vUnit,
                "vUnit_fmt": _br_money(vUnit) if vUnit else "",
                "vTotal": vTotal,
                "vTotal_fmt": _br_money(vTotal) if vTotal else "",
                "pis_cofins": pis_cof,
                "pis_cofins_fmt": _br_money(pis_cof) if pis_cof else "",
                "bc_icms": bc_icms,
                "bc_icms_fmt": _br_money(bc_icms) if bc_icms else "",
                "aliq_icms": aliq_icms,
                "aliq_icms_fmt": _br_num(aliq_icms, 2) if aliq_icms else "",
                "v_icms": v_icms,
                "v_icms_fmt": _br_money(v_icms) if v_icms else "",
            }
        )

    d = {
        # Cabeçalho / emit
        "emit_fantasia": emit_fantasia,
        "emit_nome": emit_nome,
        "emit_cnpj": emit_cnpj,
        "emit_ie": emit_ie,
        "emit_endereco_linha1": emit_endereco_linha1,
        "emit_endereco_linha2": emit_endereco_linha2,
        # Dest
        "dest_nome": dest_nome,
        "dest_doc": dest_doc,
        "dest_endereco_linha1": dest_endereco_linha1,
        "dest_endereco_linha2": dest_endereco_linha2,
        # NF
        "nNF": nNF,
        "serie": serie,
        "dhEmi": dhEmi,
        "dhEmi_fmt": dhEmi,
        "chNFCom": chNFCom,
        "chNFCom_fmt": chNFCom,
        # Totais
        "total_fmt": _br_money(vNF) if vNF else "",
        "total_pagar_fmt": _br_money(vNF) if vNF else "",
        "bc_total_fmt": _br_money(vBC) if vBC else "",
        "icms_total_fmt": _br_money(vICMS) if vICMS else "",
        "isento_fmt": _br_money(vIsento) if vIsento else "",
        "outros_fmt": _br_money(vOutro) if vOutro else "",
        "pis_total_fmt": _br_money(vPIS) if vPIS else "",
        "cofins_total_fmt": _br_money(vCOFINS) if vCOFINS else "",
        "fust_total_fmt": _br_money(vFUST) if vFUST else "",
        "funttel_total_fmt": _br_money(vFUNTTEL) if vFUNTTEL else "",
        # Itens
        "itens": itens,
        # Campos que o template usa, mas podem ficar vazios sem quebrar:
        "qrcode_url": "",
        "prot_num": "",
        "prot_data": "",
        "prot_data_fmt": "",
        "cod_assinante": "",
        "contrato": "",
        "telefone": "",
        "periodo": "",
        "referencia": "",
        "vencimento": "",
        "area_contribuinte": "",
        "reservado_fisco": "",
        "info_complementar": "",
        "debito_auto_id": "",
        "codigo_barras_vis": "",
        "linha_digitavel": "",
        "anatel_texto": "",
    }
    return d
