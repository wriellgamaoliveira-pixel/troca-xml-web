import io
import zipfile
from collections import defaultdict

from lxml import etree

from .parser import organizar_por_ncm, parse_nfe, parse_nfe_itens_flat


RELATORIO_CST_COLUMNS = [
    ("tipo", "Tipo"),
    ("cnpj", "Inscrição"),
    ("numero_nota", "Documento"),
    ("nome", "Nome Emit./Dest."),
    ("cfop", "CFOP"),
    ("produto", "Codigo Prod."),
    ("descricao", "Descrição item"),
    ("ncm", "NCM"),
    ("qtd", "Qtd."),
    ("v_unit", "Valor unitário"),
    ("v_desc", "Valor Desc."),
    ("v_prod", "Valor Cont."),
    ("bc_icms", "Base do ICMS"),
    ("aliq_icms", "Aliq. ICMS"),
    ("v_icms", "Valor do ICMS"),
    ("cst_pis", "CST PIS"),
    ("bc_pis", "Base do PIS"),
    ("aliq_pis", "Aliq. PIS"),
    ("v_pis", "Valor do PIS"),
    ("cst_cofins", "CST COFINS"),
    ("bc_cofins", "Base COFINS"),
    ("aliq_cofins", "Aliq. COFINS"),
    ("v_cofins", "Valor COFINS"),
    ("total", "Valor total"),
]

def formatar_valor_br(valor):
    if valor is None:
        return "0,00"
    try:
        return f"{float(str(valor).replace(',', '.')):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0,00"


def _num(v):
    try:
        return float(str(v or "0").replace(",", "."))
    except Exception:
        return 0.0


def _money(v):
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _nota_key(nota):
    return (nota.get("nNF") or "", nota.get("serie") or "", nota.get("cNF") or "")


def gerar_relatorio_cst(zip_file_storage, progress_cb=None):
    if zip_file_storage is None:
        return None, "Selecione um arquivo ZIP com XMLs de NF-e."

    raw = zip_file_storage.read()
    if not raw:
        return None, "Arquivo ZIP vazio."

    linhas = []
    total_arquivos = 0
    total_ok = 0

    try:
        with zipfile.ZipFile(io.BytesIO(raw), "r") as zf:
            nomes = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            total_arquivos = len(nomes)

            for idx, nome in enumerate(nomes):
                try:
                    root = etree.fromstring(zf.read(nome))
                    itens = parse_nfe_itens_flat(root)
                    if itens:
                        linhas.extend(itens)
                        total_ok += 1
                except Exception:
                    pass
                finally:
                    if progress_cb:
                        progresso = int(((idx + 1) / max(len(nomes), 1)) * 100)
                        progress_cb(progresso)

            if progress_cb and not nomes:
                progress_cb(100)

        campos_monetarios = {
            "v_unit",
            "v_desc",
            "v_prod",
            "bc_icms",
            "v_icms",
            "bc_pis",
            "v_pis",
            "bc_cofins",
            "v_cofins",
            "total",
        }

        linhas_formatadas = []
        for linha in linhas:
            nova = dict(linha)
            for campo in campos_monetarios:
                nova[campo] = formatar_valor_br(nova.get(campo))
            linhas_formatadas.append(nova)

        return {
            "linhas": linhas_formatadas,
            "total_arquivos": total_arquivos,
            "total_ok": total_ok,
            "total_itens": len(linhas_formatadas),
            "columns": RELATORIO_CST_COLUMNS,
        }, None
    except Exception as e:
        return None, f"Falha ao processar ZIP: {e}"


def gerar_relatorio_ncm(zip_file_storage):
    if zip_file_storage is None:
        return None, "Selecione um arquivo ZIP com XMLs de NF-e."

    raw = zip_file_storage.read()
    if not raw:
        return None, "Arquivo ZIP vazio."

    by_ncm = defaultdict(
        lambda: {
            "ncm": "",
            "qtd_itens": 0,
            "v_total": 0.0,
            "icms": 0.0,
            "pis": 0.0,
            "cofins": 0.0,
            "desconto": 0.0,
            "outras": 0.0,
            "notas": [],
            "_nota_keys": set(),
        }
    )
    by_item = defaultdict(
        lambda: {
            "item": "",
            "desc": "",
            "ncm": "",
            "qtd_itens": 0,
            "v_total": 0.0,
            "icms": 0.0,
            "pis": 0.0,
            "cofins": 0.0,
            "desconto": 0.0,
            "outras": 0.0,
        }
    )

    total_arquivos = 0
    total_ok = 0
    total_geral = 0.0

    try:
        with zipfile.ZipFile(io.BytesIO(raw), "r") as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            total_arquivos = len(names)

            for name in names:
                try:
                    root = etree.fromstring(zf.read(name))
                    nota = parse_nfe(root)
                    agrupado = organizar_por_ncm(nota)
                    total_ok += 1

                    for ncm, bloco in (agrupado.get("ncm") or {}).items():
                        row = by_ncm[ncm]
                        row["ncm"] = ncm

                        for item in bloco.get("itens") or []:
                            vprod = _num(item.get("vProd"))
                            icms = _num((item.get("ICMS") or {}).get("vICMS"))
                            pis = _num((item.get("PIS") or {}).get("vPIS"))
                            cofins = _num((item.get("COFINS") or {}).get("vCOFINS"))
                            desc = _num(item.get("vDesc"))
                            outras = _num(item.get("vOutro"))

                            row["qtd_itens"] += 1
                            row["v_total"] += vprod
                            row["icms"] += icms
                            row["pis"] += pis
                            row["cofins"] += cofins
                            row["desconto"] += desc
                            row["outras"] += outras

                            item_key = (item.get("cProd") or "", item.get("xProd") or "", ncm)
                            ir = by_item[item_key]
                            ir["item"] = item.get("cProd") or ""
                            ir["desc"] = item.get("xProd") or ""
                            ir["ncm"] = ncm
                            ir["qtd_itens"] += 1
                            ir["v_total"] += vprod
                            ir["icms"] += icms
                            ir["pis"] += pis
                            ir["cofins"] += cofins
                            ir["desconto"] += desc
                            ir["outras"] += outras

                            total_geral += vprod

                        for nota_ref in bloco.get("notas") or []:
                            key = _nota_key(nota_ref)
                            if key not in row["_nota_keys"]:
                                row["_nota_keys"].add(key)
                                row["notas"].append(nota_ref)
                except Exception:
                    continue

        ncm_linhas = sorted(by_ncm.values(), key=lambda x: x["v_total"], reverse=True)
        item_linhas = sorted(by_item.values(), key=lambda x: x["v_total"], reverse=True)

        for row in ncm_linhas:
            row.pop("_nota_keys", None)
            row["v_total_br"] = _money(row["v_total"])
            row["icms_br"] = _money(row["icms"])
            row["pis_br"] = _money(row["pis"])
            row["cofins_br"] = _money(row["cofins"])
            row["desconto_br"] = _money(row["desconto"])
            row["outras_br"] = _money(row["outras"])

        for row in item_linhas:
            row["v_total_br"] = _money(row["v_total"])
            row["icms_br"] = _money(row["icms"])
            row["pis_br"] = _money(row["pis"])
            row["cofins_br"] = _money(row["cofins"])
            row["desconto_br"] = _money(row["desconto"])
            row["outras_br"] = _money(row["outras"])

        top12 = ncm_linhas[:12]
        labels = [r["ncm"] for r in top12]
        valores = [r["v_total"] for r in top12]

        return {
            "total_arquivos": total_arquivos,
            "total_ok": total_ok,
            "total_geral": total_geral,
            "total_geral_br": _money(total_geral),
            "labels": labels,
            "valores": valores,
            "ncm_linhas": ncm_linhas,
            "item_linhas": item_linhas,
        }, None
    except Exception as e:
        return None, f"Falha ao processar ZIP: {e}"
