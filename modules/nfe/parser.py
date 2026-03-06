def processar_xml(xml_bytes: bytes):
    from app import parse_nfe_xml
    return parse_nfe_xml(xml_bytes)
