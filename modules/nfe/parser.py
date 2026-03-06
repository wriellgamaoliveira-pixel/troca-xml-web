from lxml import etree


def _tag_name(tag: str) -> str:
    return tag.split('}', 1)[-1] if '}' in tag else tag


def _find_first(node, path_with_ns: str, ns: dict):
    if node is None:
        return None
    found = node.find(path_with_ns, ns) if ns else node.find(path_with_ns.replace('nfe:', ''))
    if found is not None:
        return found
    # fallback namespace-agnostic by local-name
    parts = [p for p in path_with_ns.split('/') if p and p != '.']
    cur = node
    for p in parts:
        local = p.split(':', 1)[-1]
        nxt = None
        for ch in list(cur):
            if _tag_name(ch.tag) == local:
                nxt = ch
                break
        if nxt is None:
            return None
        cur = nxt
    return cur


def _find_text(node, path_with_ns: str, ns: dict, default: str = '') -> str:
    el = _find_first(node, path_with_ns, ns)
    if el is None or el.text is None:
        return default
    return (el.text or '').strip()


def _to_float(v):
    if v in (None, ''):
        return 0.0
    try:
        return float(str(v).replace(',', '.'))
    except Exception:
        return 0.0


def parse_nfe(xml_root):
    ns_uri = etree.QName(xml_root).namespace or ''
    ns = {'nfe': ns_uri} if ns_uri else {}

    inf = _find_first(xml_root, './/nfe:infNFe', ns)
    if inf is None:
        inf = xml_root

    ide = _find_first(inf, 'nfe:ide', ns)
    emit = _find_first(inf, 'nfe:emit', ns)
    dest = _find_first(inf, 'nfe:dest', ns)
    total = _find_first(inf, 'nfe:total/nfe:ICMSTot', ns)

    identificacao = {
        'nNF': _find_text(ide, 'nfe:nNF', ns),
        'serie': _find_text(ide, 'nfe:serie', ns),
        'dhEmi': _find_text(ide, 'nfe:dhEmi', ns),
        'natOp': _find_text(ide, 'nfe:natOp', ns),
        'mod': _find_text(ide, 'nfe:mod', ns),
        'tpNF': _find_text(ide, 'nfe:tpNF', ns),
        'finNFe': _find_text(ide, 'nfe:finNFe', ns),
    }

    emit_end = _find_first(emit, 'nfe:enderEmit', ns)
    emitente = {
        'CNPJ': _find_text(emit, 'nfe:CNPJ', ns),
        'xNome': _find_text(emit, 'nfe:xNome', ns),
        'xFant': _find_text(emit, 'nfe:xFant', ns),
        'IE': _find_text(emit, 'nfe:IE', ns),
        'CRT': _find_text(emit, 'nfe:CRT', ns),
        'enderEmit': {
            'xLgr': _find_text(emit_end, 'nfe:xLgr', ns),
            'nro': _find_text(emit_end, 'nfe:nro', ns),
            'xBairro': _find_text(emit_end, 'nfe:xBairro', ns),
            'cMun': _find_text(emit_end, 'nfe:cMun', ns),
            'xMun': _find_text(emit_end, 'nfe:xMun', ns),
            'UF': _find_text(emit_end, 'nfe:UF', ns),
            'CEP': _find_text(emit_end, 'nfe:CEP', ns),
            'cPais': _find_text(emit_end, 'nfe:cPais', ns),
            'xPais': _find_text(emit_end, 'nfe:xPais', ns),
            'fone': _find_text(emit_end, 'nfe:fone', ns),
        },
    }

    dest_end = _find_first(dest, 'nfe:enderDest', ns)
    destinatario = {
        'CNPJ': _find_text(dest, 'nfe:CNPJ', ns),
        'CPF': _find_text(dest, 'nfe:CPF', ns),
        'xNome': _find_text(dest, 'nfe:xNome', ns),
        'IE': _find_text(dest, 'nfe:IE', ns),
        'enderDest': {
            'xLgr': _find_text(dest_end, 'nfe:xLgr', ns),
            'nro': _find_text(dest_end, 'nfe:nro', ns),
            'xBairro': _find_text(dest_end, 'nfe:xBairro', ns),
            'cMun': _find_text(dest_end, 'nfe:cMun', ns),
            'xMun': _find_text(dest_end, 'nfe:xMun', ns),
            'UF': _find_text(dest_end, 'nfe:UF', ns),
            'CEP': _find_text(dest_end, 'nfe:CEP', ns),
            'cPais': _find_text(dest_end, 'nfe:cPais', ns),
            'xPais': _find_text(dest_end, 'nfe:xPais', ns),
            'fone': _find_text(dest_end, 'nfe:fone', ns),
        },
    }

    dets = inf.findall('nfe:det', ns) if ns else inf.findall('det')
    if not dets:
        dets = [el for el in inf.iter() if _tag_name(el.tag) == 'det']

    itens = []
    for det in dets:
        prod = _find_first(det, 'nfe:prod', ns)
        imposto = _find_first(det, 'nfe:imposto', ns)

        # ICMS dinâmico (ICMS00, ICMS10, ICMS60, ...)
        icms_tipo = ''
        icms_no = None
        icms_wrap = _find_first(imposto, 'nfe:ICMS', ns)
        if icms_wrap is not None:
            for child in list(icms_wrap):
                name = _tag_name(child.tag)
                if name.startswith('ICMS'):
                    icms_tipo = name
                    icms_no = child
                    break
        if icms_no is None and imposto is not None:
            for child in list(imposto):
                name = _tag_name(child.tag)
                if name.startswith('ICMS'):
                    icms_tipo = name
                    icms_no = child
                    break

        pis_tipo = ''
        pis_no = None
        pis_wrap = _find_first(imposto, 'nfe:PIS', ns)
        if pis_wrap is not None:
            for child in list(pis_wrap):
                pis_tipo = _tag_name(child.tag)
                pis_no = child
                break

        cofins_tipo = ''
        cofins_no = None
        cofins_wrap = _find_first(imposto, 'nfe:COFINS', ns)
        if cofins_wrap is not None:
            for child in list(cofins_wrap):
                cofins_tipo = _tag_name(child.tag)
                cofins_no = child
                break

        item = {
            'cProd': _find_text(prod, 'nfe:cProd', ns),
            'xProd': _find_text(prod, 'nfe:xProd', ns),
            'NCM': _find_text(prod, 'nfe:NCM', ns),
            'CFOP': _find_text(prod, 'nfe:CFOP', ns),
            'uCom': _find_text(prod, 'nfe:uCom', ns),
            'qCom': _find_text(prod, 'nfe:qCom', ns),
            'vUnCom': _find_text(prod, 'nfe:vUnCom', ns),
            'vProd': _find_text(prod, 'nfe:vProd', ns),
            'ICMS': {
                'tipo': icms_tipo,
                'CST': _find_text(icms_no, 'nfe:CST', ns),
                'orig': _find_text(icms_no, 'nfe:orig', ns),
                'vBC': _find_text(icms_no, 'nfe:vBC', ns),
                'pICMS': _find_text(icms_no, 'nfe:pICMS', ns),
                'vICMS': _find_text(icms_no, 'nfe:vICMS', ns),
            },
            'PIS': {
                'tipo': pis_tipo,
                'CST': _find_text(pis_no, 'nfe:CST', ns),
                'vBC': _find_text(pis_no, 'nfe:vBC', ns),
                'pPIS': _find_text(pis_no, 'nfe:pPIS', ns),
                'vPIS': _find_text(pis_no, 'nfe:vPIS', ns),
            },
            'COFINS': {
                'tipo': cofins_tipo,
                'CST': _find_text(cofins_no, 'nfe:CST', ns),
                'vBC': _find_text(cofins_no, 'nfe:vBC', ns),
                'pCOFINS': _find_text(cofins_no, 'nfe:pCOFINS', ns),
                'vCOFINS': _find_text(cofins_no, 'nfe:vCOFINS', ns),
            },
        }
        itens.append(item)

    totais = {
        'vBC': _find_text(total, 'nfe:vBC', ns),
        'vICMS': _find_text(total, 'nfe:vICMS', ns),
        'vProd': _find_text(total, 'nfe:vProd', ns),
        'vPIS': _find_text(total, 'nfe:vPIS', ns),
        'vCOFINS': _find_text(total, 'nfe:vCOFINS', ns),
        'vNF': _find_text(total, 'nfe:vNF', ns),
        # numéricos auxiliares
        'vBC_num': _to_float(_find_text(total, 'nfe:vBC', ns)),
        'vICMS_num': _to_float(_find_text(total, 'nfe:vICMS', ns)),
        'vProd_num': _to_float(_find_text(total, 'nfe:vProd', ns)),
        'vPIS_num': _to_float(_find_text(total, 'nfe:vPIS', ns)),
        'vCOFINS_num': _to_float(_find_text(total, 'nfe:vCOFINS', ns)),
        'vNF_num': _to_float(_find_text(total, 'nfe:vNF', ns)),
    }

    return {
        'identificacao': identificacao,
        'emitente': emitente,
        'destinatario': destinatario,
        'itens': itens,
        'totais': totais,
    }


def processar_xml(xml_bytes: bytes):
    root = etree.fromstring(xml_bytes)
    return parse_nfe(root)
