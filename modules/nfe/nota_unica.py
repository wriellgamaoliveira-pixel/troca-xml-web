from lxml import etree

from .parser import parse_nfe


def processar_nota_unica_nfe(file_storage):
    if file_storage is None:
        return None, "Selecione um arquivo XML de NF-e."

    try:
        xml_bytes = file_storage.read()
        if not xml_bytes:
            return None, "Arquivo XML vazio."

        root = etree.fromstring(xml_bytes)
        data = parse_nfe(root)

        mod = (data.get("identificacao") or {}).get("mod", "").strip()
        if mod != "55":
            return None, f"Modelo inválido para NF-e. Esperado 55 e recebido {mod or 'vazio'}."

        return data, None
    except Exception as e:
        return None, f"Falha ao processar XML de NF-e: {e}"
