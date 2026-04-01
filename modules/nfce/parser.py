from lxml import etree


NS = {"nfe": "http://www.portalfiscal.inf.br/nfe"}


def _find(node, path):
    if node is None:
        return None
    return node.find(path, NS)


def _findall(node, path):
    if node is None:
        return []
    return node.findall(path, NS)


def _text(node, path, default=""):
    el = _find(node, path)
    if el is None or el.text is None:
        return default
    return el.text.strip()


def _icms_node(imposto):
    icms_wrap = _find(imposto, "nfe:ICMS")
    if icms_wrap is None:
        return None
    for child in list(icms_wrap):
        if etree.QName(child).localname.startswith("ICMS"):
            return child
    return None


def _pis_node(imposto):
    pis_wrap = _find(imposto, "nfe:PIS")
    if pis_wrap is None:
        return None
    children = list(pis_wrap)
    return children[0] if children else None


def _cofins_node(imposto):
    cofins_wrap = _find(imposto, "nfe:COFINS")
    if cofins_wrap is None:
        return None
    children = list(cofins_wrap)
    return children[0] if children else None


def _tipo(tp_nf):
    return "Entrada" if (tp_nf or "").strip() == "0" else "Saída"


def parse_nfce_relatorio_cst(xml_bytes: bytes):
    root = etree.fromstring(xml_bytes)
    inf = _find(root, ".//nfe:infNFe")
    if inf is None:
        return []

    ide = _find(inf, "nfe:ide")
    mod = _text(ide, "nfe:mod")
    if mod != "65":
        return []

    emit = _find(inf, "nfe:emit")
    dest = _find(inf, "nfe:dest")

    tipo = _tipo(_text(ide, "nfe:tpNF"))
    inscricao = _text(emit, "nfe:IE")
    documento = _text(ide, "nfe:nNF")
    nome = _text(dest, "nfe:xNome") or _text(emit, "nfe:xNome")

    linhas = []
    for det in _findall(inf, "nfe:det"):
        prod = _find(det, "nfe:prod")
        imposto = _find(det, "nfe:imposto")

        icms = _icms_node(imposto)
        pis = _pis_node(imposto)
        cofins = _cofins_node(imposto)

        linhas.append(
            {
                "tipo": tipo,
                "inscricao": inscricao,
                "documento": documento,
                "nome": nome,
                "cfop": _text(prod, "nfe:CFOP"),
                "produto": _text(prod, "nfe:cProd"),
                "descricao": _text(prod, "nfe:xProd"),
                "ncm": _text(prod, "nfe:NCM"),
                "qtd": _text(prod, "nfe:qCom"),
                "v_unit": _text(prod, "nfe:vUnCom", "0"),
                "v_desc": _text(prod, "nfe:vDesc", "0"),
                "v_prod": _text(prod, "nfe:vProd", "0"),
                "bc_icms": _text(icms, "nfe:vBC", "0"),
                "aliq_icms": _text(icms, "nfe:pICMS", "0"),
                "v_icms": _text(icms, "nfe:vICMS", "0"),
                "cst_pis": _text(pis, "nfe:CST"),
                "bc_pis": _text(pis, "nfe:vBC", "0"),
                "aliq_pis": _text(pis, "nfe:pPIS") or _text(pis, "nfe:vAliqProd", "0"),
                "v_pis": _text(pis, "nfe:vPIS", "0"),
                "cst_cofins": _text(cofins, "nfe:CST"),
                "bc_cofins": _text(cofins, "nfe:vBC", "0"),
                "aliq_cofins": _text(cofins, "nfe:pCOFINS") or _text(cofins, "nfe:vAliqProd", "0"),
                "v_cofins": _text(cofins, "nfe:vCOFINS", "0"),
                "total": _text(prod, "nfe:vProd", "0"),
            }
        )

    return linhas
