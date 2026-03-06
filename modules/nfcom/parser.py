def processar_xml(xml_bytes: bytes):
    from app import parse_nfcom_xml
    return parse_nfcom_xml(xml_bytes)
