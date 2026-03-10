from lxml import etree


NS = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}


def _tag_name(tag: str) -> str:
    return tag.split('}', 1)[-1] if '}' in tag else tag


def _find_first(node, path_with_ns: str, ns: dict):
    if node is None:
        return None
    found = node.find(path_with_ns, ns) if ns else node.find(path_with_ns.replace('nfe:', ''))
    if found is not None:
        return found
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
    ns_uri = etree.QName(xml_root).namespace or NS['nfe']
    ns = {'nfe': ns_uri}

    inf = _find_first(xml_root, './/nfe:infNFe', ns)
    if inf is None:
        inf = xml_root

    ide = _find_first(inf, 'nfe:ide', ns)
    emit = _find_first(inf, 'nfe:emit', ns)
    dest = _find_first(inf, 'nfe:dest', ns)
    total = _find_first(inf, 'nfe:total/nfe:ICMSTot', ns)

    identificacao = {
        'cUF': _find_text(ide, 'nfe:cUF', ns),
        'cNF': _find_text(ide, 'nfe:cNF', ns),
        'natOp': _find_text(ide, 'nfe:natOp', ns),
        'mod': _find_text(ide, 'nfe:mod', ns),
        'serie': _find_text(ide, 'nfe:serie', ns),
        'nNF': _find_text(ide, 'nfe:nNF', ns),
        'dhEmi': _find_text(ide, 'nfe:dhEmi', ns),
        'dhSaiEnt': _find_text(ide, 'nfe:dhSaiEnt', ns),
        'tpNF': _find_text(ide, 'nfe:tpNF', ns),
        'idDest': _find_text(ide, 'nfe:idDest', ns),
        'cMunFG': _find_text(ide, 'nfe:cMunFG', ns),
        'tpImp': _find_text(ide, 'nfe:tpImp', ns),
        'tpEmis': _find_text(ide, 'nfe:tpEmis', ns),
        'cDV': _find_text(ide, 'nfe:cDV', ns),
        'tpAmb': _find_text(ide, 'nfe:tpAmb', ns),
        'finNFe': _find_text(ide, 'nfe:finNFe', ns),
        'indFinal': _find_text(ide, 'nfe:indFinal', ns),
        'indPres': _find_text(ide, 'nfe:indPres', ns),
        'procEmi': _find_text(ide, 'nfe:procEmi', ns),
        'verProc': _find_text(ide, 'nfe:verProc', ns),
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
            'xCpl': _find_text(emit_end, 'nfe:xCpl', ns),
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
        'indIEDest': _find_text(dest, 'nfe:indIEDest', ns),
        'enderDest': {
            'xLgr': _find_text(dest_end, 'nfe:xLgr', ns),
            'nro': _find_text(dest_end, 'nfe:nro', ns),
            'xCpl': _find_text(dest_end, 'nfe:xCpl', ns),
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

    dets = inf.findall('nfe:det', ns)
    if not dets:
        dets = [el for el in inf.iter() if _tag_name(el.tag) == 'det']

    itens = []
    for det in dets:
        prod = _find_first(det, 'nfe:prod', ns)
        imposto = _find_first(det, 'nfe:imposto', ns)

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
            'nItem': det.get('nItem') or '',
            'cProd': _find_text(prod, 'nfe:cProd', ns),
            'xProd': _find_text(prod, 'nfe:xProd', ns),
            'NCM': _find_text(prod, 'nfe:NCM', ns),
            'CFOP': _find_text(prod, 'nfe:CFOP', ns),
            'uCom': _find_text(prod, 'nfe:uCom', ns),
            'qCom': _find_text(prod, 'nfe:qCom', ns),
            'vUnCom': _find_text(prod, 'nfe:vUnCom', ns),
            'vProd': _find_text(prod, 'nfe:vProd', ns),
            'cEAN': _find_text(prod, 'nfe:cEAN', ns),
            'cBarra': _find_text(prod, 'nfe:cBarra', ns),
            'uTrib': _find_text(prod, 'nfe:uTrib', ns),
            'qTrib': _find_text(prod, 'nfe:qTrib', ns),
            'vUnTrib': _find_text(prod, 'nfe:vUnTrib', ns),
            'vFrete': _find_text(prod, 'nfe:vFrete', ns),
            'vSeg': _find_text(prod, 'nfe:vSeg', ns),
            'vDesc': _find_text(prod, 'nfe:vDesc', ns),
            'vOutro': _find_text(prod, 'nfe:vOutro', ns),
            'ICMS': {
                'tipo': icms_tipo,
                'CST': _find_text(icms_no, 'nfe:CST', ns),
                'CSOSN': _find_text(icms_no, 'nfe:CSOSN', ns),
                'vBC': _find_text(icms_no, 'nfe:vBC', ns),
                'pICMS': _find_text(icms_no, 'nfe:pICMS', ns),
                'vICMS': _find_text(icms_no, 'nfe:vICMS', ns),
                'vBCST': _find_text(icms_no, 'nfe:vBCST', ns),
                'vICMSST': _find_text(icms_no, 'nfe:vICMSST', ns),
                'vFCP': _find_text(icms_no, 'nfe:vFCP', ns),
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
        'vST': _find_text(total, 'nfe:vST', ns),
        'vProd': _find_text(total, 'nfe:vProd', ns),
        'vPIS': _find_text(total, 'nfe:vPIS', ns),
        'vCOFINS': _find_text(total, 'nfe:vCOFINS', ns),
        'vNF': _find_text(total, 'nfe:vNF', ns),
        'vFrete': _find_text(total, 'nfe:vFrete', ns),
        'vSeg': _find_text(total, 'nfe:vSeg', ns),
        'vDesc': _find_text(total, 'nfe:vDesc', ns),
        'vBC_num': _to_float(_find_text(total, 'nfe:vBC', ns)),
        'vICMS_num': _to_float(_find_text(total, 'nfe:vICMS', ns)),
        'vST_num': _to_float(_find_text(total, 'nfe:vST', ns)),
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
