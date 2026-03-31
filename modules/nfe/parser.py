from __future__ import annotations

from collections import defaultdict
from typing import Any

from lxml import etree


NS = {"nfe": "http://www.portalfiscal.inf.br/nfe"}


def _find(node: etree._Element | None, path: str):
    if node is None:
        return None
    return node.find(path, NS)


def _findall(node: etree._Element | None, path: str):
    if node is None:
        return []
    return node.findall(path, NS)


def _text(node: etree._Element | None, path: str, default: str = "") -> str:
    found = _find(node, path)
    if found is None or found.text is None:
        return default
    return found.text.strip()


def _num(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace(",", "."))
    except Exception:
        return 0.0


def _money(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _extract_icms(imposto: etree._Element | None) -> dict[str, str]:
    icms = _find(imposto, "nfe:ICMS")
    icms_node = None
    icms_tipo = ""

    for child in list(icms) if icms is not None else []:
        local = etree.QName(child).localname
        if local.startswith("ICMS"):
            icms_node = child
            icms_tipo = local
            break

    return {
        "tipo": icms_tipo,
        "CST": _text(icms_node, "nfe:CST"),
        "CSOSN": _text(icms_node, "nfe:CSOSN"),
        "vBC": _text(icms_node, "nfe:vBC"),
        "pICMS": _text(icms_node, "nfe:pICMS"),
        "vICMS": _text(icms_node, "nfe:vICMS"),
    }


def _extract_pis(imposto: etree._Element | None) -> dict[str, str]:
    pis = _find(imposto, "nfe:PIS")
    pis_node = list(pis)[0] if pis is not None and len(list(pis)) else None
    return {
        "tipo": etree.QName(pis_node).localname if pis_node is not None else "",
        "CST": _text(pis_node, "nfe:CST"),
        "vBC": _text(pis_node, "nfe:vBC"),
        "pPIS": _text(pis_node, "nfe:pPIS"),
        "vPIS": _text(pis_node, "nfe:vPIS"),
    }


def _extract_cofins(imposto: etree._Element | None) -> dict[str, str]:
    cofins = _find(imposto, "nfe:COFINS")
    cofins_node = list(cofins)[0] if cofins is not None and len(list(cofins)) else None
    return {
        "tipo": etree.QName(cofins_node).localname if cofins_node is not None else "",
        "CST": _text(cofins_node, "nfe:CST"),
        "vBC": _text(cofins_node, "nfe:vBC"),
        "pCOFINS": _text(cofins_node, "nfe:pCOFINS"),
        "vCOFINS": _text(cofins_node, "nfe:vCOFINS"),
    }


def _tipo_operacao(tp_nf: str) -> str:
    return "Entrada" if (tp_nf or "").strip() == "0" else "Saída"


def parse_nfe_itens_flat(xml_root: etree._Element) -> list[dict[str, str]]:
    inf = _find(xml_root, ".//nfe:infNFe")
    if inf is None:
        raise ValueError("XML sem infNFe.")

    ide = _find(inf, "nfe:ide")
    emit = _find(inf, "nfe:emit")
    dest = _find(inf, "nfe:dest")

    mod = _text(ide, "nfe:mod")
    if mod != "55":
        raise ValueError(f"Modelo inválido para NF-e: {mod or 'vazio'}")

    tipo = _tipo_operacao(_text(ide, "nfe:tpNF"))
    cnpj = _text(emit, "nfe:CNPJ")
    numero_nota = _text(ide, "nfe:nNF")
    nome_emit = _text(emit, "nfe:xNome")
    nome_dest = _text(dest, "nfe:xNome")
    nome = nome_dest or nome_emit

    linhas = []
    for det in _findall(inf, "nfe:det"):
        prod = _find(det, "nfe:prod")
        imposto = _find(det, "nfe:imposto")

        icms = _extract_icms(imposto)
        pis = _extract_pis(imposto)
        cofins = _extract_cofins(imposto)

        v_prod = _text(prod, "nfe:vProd")

        linhas.append(
            {
                "tipo": tipo,
                "cnpj": cnpj,
                "numero_nota": numero_nota,
                "nome": nome,
                "cfop": _text(prod, "nfe:CFOP"),
                "produto": _text(prod, "nfe:cProd"),
                "descricao": _text(prod, "nfe:xProd"),
                "ncm": _text(prod, "nfe:NCM"),
                "qtd": _text(prod, "nfe:qCom"),
                "v_unit": _text(prod, "nfe:vUnCom"),
                "v_desc": _text(prod, "nfe:vDesc"),
                "v_prod": v_prod,
                "bc_icms": icms.get("vBC") or "",
                "aliq_icms": icms.get("pICMS") or "",
                "v_icms": icms.get("vICMS") or "",
                "cst_pis": pis.get("CST") or "",
                "bc_pis": pis.get("vBC") or "",
                "aliq_pis": pis.get("pPIS") or "",
                "v_pis": pis.get("vPIS") or "",
                "cst_cofins": cofins.get("CST") or "",
                "bc_cofins": cofins.get("vBC") or "",
                "aliq_cofins": cofins.get("pCOFINS") or "",
                "v_cofins": cofins.get("vCOFINS") or "",
                "total": v_prod,
            }
        )

    return linhas


def parse_nfe(xml_root: etree._Element) -> dict[str, Any]:
    inf = _find(xml_root, ".//nfe:infNFe")
    if inf is None:
        raise ValueError("XML sem infNFe.")

    ide = _find(inf, "nfe:ide")
    emit = _find(inf, "nfe:emit")
    dest = _find(inf, "nfe:dest")
    total = _find(inf, "nfe:total/nfe:ICMSTot")

    identificacao = {
        "mod": _text(ide, "nfe:mod"),
        "serie": _text(ide, "nfe:serie"),
        "nNF": _text(ide, "nfe:nNF"),
        "dhEmi": _text(ide, "nfe:dhEmi"),
        "dhSaiEnt": _text(ide, "nfe:dhSaiEnt"),
        "natOp": _text(ide, "nfe:natOp"),
        "tpNF": _text(ide, "nfe:tpNF"),
        "idDest": _text(ide, "nfe:idDest"),
        "cMunFG": _text(ide, "nfe:cMunFG"),
        "tpImp": _text(ide, "nfe:tpImp"),
        "tpEmis": _text(ide, "nfe:tpEmis"),
        "cDV": _text(ide, "nfe:cDV"),
        "tpAmb": _text(ide, "nfe:tpAmb"),
        "finNFe": _text(ide, "nfe:finNFe"),
        "indFinal": _text(ide, "nfe:indFinal"),
        "indPres": _text(ide, "nfe:indPres"),
        "procEmi": _text(ide, "nfe:procEmi"),
        "verProc": _text(ide, "nfe:verProc"),
        "cNF": _text(ide, "nfe:cNF"),
    }

    if identificacao["mod"] != "55":
        raise ValueError(f"Modelo inválido para NF-e: {identificacao['mod'] or 'vazio'}")

    emitente = {
        "CNPJ": _text(emit, "nfe:CNPJ"),
        "xNome": _text(emit, "nfe:xNome"),
        "xFant": _text(emit, "nfe:xFant"),
        "IE": _text(emit, "nfe:IE"),
        "CRT": _text(emit, "nfe:CRT"),
        "enderEmit": {
            "xLgr": _text(emit, "nfe:enderEmit/nfe:xLgr"),
            "nro": _text(emit, "nfe:enderEmit/nfe:nro"),
            "xCpl": _text(emit, "nfe:enderEmit/nfe:xCpl"),
            "xBairro": _text(emit, "nfe:enderEmit/nfe:xBairro"),
            "cMun": _text(emit, "nfe:enderEmit/nfe:cMun"),
            "xMun": _text(emit, "nfe:enderEmit/nfe:xMun"),
            "UF": _text(emit, "nfe:enderEmit/nfe:UF"),
            "CEP": _text(emit, "nfe:enderEmit/nfe:CEP"),
            "cPais": _text(emit, "nfe:enderEmit/nfe:cPais"),
            "xPais": _text(emit, "nfe:enderEmit/nfe:xPais"),
            "fone": _text(emit, "nfe:enderEmit/nfe:fone"),
        },
    }

    destinatario = {
        "CNPJ": _text(dest, "nfe:CNPJ"),
        "CPF": _text(dest, "nfe:CPF"),
        "xNome": _text(dest, "nfe:xNome"),
        "IE": _text(dest, "nfe:IE"),
        "indIEDest": _text(dest, "nfe:indIEDest"),
        "enderDest": {
            "xLgr": _text(dest, "nfe:enderDest/nfe:xLgr"),
            "nro": _text(dest, "nfe:enderDest/nfe:nro"),
            "xCpl": _text(dest, "nfe:enderDest/nfe:xCpl"),
            "xBairro": _text(dest, "nfe:enderDest/nfe:xBairro"),
            "cMun": _text(dest, "nfe:enderDest/nfe:cMun"),
            "xMun": _text(dest, "nfe:enderDest/nfe:xMun"),
            "UF": _text(dest, "nfe:enderDest/nfe:UF"),
            "CEP": _text(dest, "nfe:enderDest/nfe:CEP"),
            "cPais": _text(dest, "nfe:enderDest/nfe:cPais"),
            "xPais": _text(dest, "nfe:enderDest/nfe:xPais"),
            "fone": _text(dest, "nfe:enderDest/nfe:fone"),
        },
    }

    itens = []
    for det in _findall(inf, "nfe:det"):
        prod = _find(det, "nfe:prod")
        imposto = _find(det, "nfe:imposto")
        icms = _extract_icms(imposto)
        pis = _extract_pis(imposto)
        cofins = _extract_cofins(imposto)

        itens.append(
            {
                "nItem": det.get("nItem") or "",
                "cProd": _text(prod, "nfe:cProd"),
                "xProd": _text(prod, "nfe:xProd"),
                "NCM": _text(prod, "nfe:NCM"),
                "CFOP": _text(prod, "nfe:CFOP"),
                "uCom": _text(prod, "nfe:uCom"),
                "qCom": _text(prod, "nfe:qCom"),
                "vUnCom": _text(prod, "nfe:vUnCom"),
                "vProd": _text(prod, "nfe:vProd"),
                "cEAN": _text(prod, "nfe:cEAN"),
                "cBarra": _text(prod, "nfe:cBarra"),
                "uTrib": _text(prod, "nfe:uTrib"),
                "qTrib": _text(prod, "nfe:qTrib"),
                "vUnTrib": _text(prod, "nfe:vUnTrib"),
                "vFrete": _text(prod, "nfe:vFrete"),
                "vSeg": _text(prod, "nfe:vSeg"),
                "vDesc": _text(prod, "nfe:vDesc"),
                "vOutro": _text(prod, "nfe:vOutro"),
                "ICMS": icms,
                "PIS": pis,
                "COFINS": cofins,
            }
        )

    totais = {
        "vBC": _text(total, "nfe:vBC"),
        "vICMS": _text(total, "nfe:vICMS"),
        "vST": _text(total, "nfe:vST"),
        "vProd": _text(total, "nfe:vProd"),
        "vPIS": _text(total, "nfe:vPIS"),
        "vCOFINS": _text(total, "nfe:vCOFINS"),
        "vNF": _text(total, "nfe:vNF"),
    }

    return {
        "identificacao": identificacao,
        "emitente": emitente,
        "destinatario": destinatario,
        "itens": itens,
        "totais": totais,
    }


def organizar_por_ncm(nota_data: dict[str, Any]) -> dict[str, Any]:
    ide = nota_data.get("identificacao") or {}
    nota_ref = {
        "nNF": ide.get("nNF") or "",
        "serie": ide.get("serie") or "",
        "dhEmi": ide.get("dhEmi") or "",
        "cNF": ide.get("cNF") or "",
        "emitente": ((nota_data.get("emitente") or {}).get("xNome") or ""),
        "destinatario": ((nota_data.get("destinatario") or {}).get("xNome") or ""),
        "valor": _num((nota_data.get("totais") or {}).get("vNF")),
    }
    nota_ref["valor_br"] = _money(nota_ref["valor"])

    ncm_map = defaultdict(
        lambda: {
            "ncm": "",
            "notas": [],
            "itens": [],
            "totais": {
                "vProd": 0.0,
                "vICMS": 0.0,
                "vPIS": 0.0,
                "vCOFINS": 0.0,
                "vDesc": 0.0,
                "vOutro": 0.0,
            },
        }
    )

    for item in nota_data.get("itens") or []:
        ncm = (item.get("NCM") or "").strip() or "SEM NCM"
        group = ncm_map[ncm]
        group["ncm"] = ncm
        if not group["notas"]:
            group["notas"].append(nota_ref)
        elif all((n.get("nNF"), n.get("serie"), n.get("cNF")) != (nota_ref["nNF"], nota_ref["serie"], nota_ref["cNF"]) for n in group["notas"]):
            group["notas"].append(nota_ref)

        group["itens"].append(item)
        group["totais"]["vProd"] += _num(item.get("vProd"))
        group["totais"]["vICMS"] += _num((item.get("ICMS") or {}).get("vICMS"))
        group["totais"]["vPIS"] += _num((item.get("PIS") or {}).get("vPIS"))
        group["totais"]["vCOFINS"] += _num((item.get("COFINS") or {}).get("vCOFINS"))
        group["totais"]["vDesc"] += _num(item.get("vDesc"))
        group["totais"]["vOutro"] += _num(item.get("vOutro"))

    return {"ncm": ncm_map, "nota": nota_ref}
