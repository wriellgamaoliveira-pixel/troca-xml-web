import io
import zipfile
from collections import defaultdict

from lxml import etree

from .parser import parse_nfe


def _num(v):
    try:
        return float(str(v or '0').replace(',', '.'))
    except Exception:
        return 0.0


def _money(v):
    return f"R$ {v:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def gerar_relatorio_ncm(zip_file_storage):
    if zip_file_storage is None:
        return None, 'Selecione um arquivo ZIP com XMLs de NF-e.'

    raw = zip_file_storage.read()
    if not raw:
        return None, 'Arquivo ZIP vazio.'

    by_ncm = defaultdict(lambda: {
        'ncm': '', 'qtd_itens': 0, 'qtd_notas': 0,
        'v_total': 0.0, 'icms': 0.0, 'pis': 0.0, 'cofins': 0.0,
        'fust': 0.0, 'funttel': 0.0, 'ibs': 0.0, 'cbs': 0.0,
        'desconto': 0.0, 'outras': 0.0,
        'notas': []
    })
    by_item = defaultdict(lambda: {
        'item': '', 'desc': '', 'ncm': '', 'qtd_itens': 0,
        'v_total': 0.0, 'icms': 0.0, 'pis': 0.0, 'cofins': 0.0,
        'fust': 0.0, 'funttel': 0.0, 'ibs': 0.0, 'cbs': 0.0,
        'desconto': 0.0, 'outras': 0.0,
    })

    total_arquivos = 0
    total_ok = 0
    total_geral = 0.0

    try:
        with zipfile.ZipFile(io.BytesIO(raw), 'r') as zf:
            names = [n for n in zf.namelist() if n.lower().endswith('.xml')]
            total_arquivos = len(names)

            for name in names:
                try:
                    root = etree.fromstring(zf.read(name))
                    data = parse_nfe(root)
                    ide = data.get('identificacao') or {}
                    if (ide.get('mod') or '').strip() != '55':
                        continue

                    total_ok += 1
                    nota_ref = {
                        'nNF': ide.get('nNF') or '',
                        'serie': ide.get('serie') or '',
                        'dhEmi': ide.get('dhEmi') or '',
                        'cNF': ide.get('cNF') or '',
                        'emitente': (data.get('emitente') or {}).get('xNome') or '',
                        'destinatario': (data.get('destinatario') or {}).get('xNome') or '',
                    }

                    for it in data.get('itens') or []:
                        ncm = (it.get('NCM') or '').strip() or 'SEM NCM'
                        vprod = _num(it.get('vProd'))
                        icms = _num((it.get('ICMS') or {}).get('vICMS'))
                        pis = _num((it.get('PIS') or {}).get('vPIS'))
                        cofins = _num((it.get('COFINS') or {}).get('vCOFINS'))
                        desc = _num(it.get('vDesc'))
                        outras = _num(it.get('vOutro'))

                        rec = by_ncm[ncm]
                        rec['ncm'] = ncm
                        rec['qtd_itens'] += 1
                        rec['qtd_notas'] += 1
                        rec['v_total'] += vprod
                        rec['icms'] += icms
                        rec['pis'] += pis
                        rec['cofins'] += cofins
                        rec['desconto'] += desc
                        rec['outras'] += outras
                        if len(rec['notas']) < 200:
                            rec['notas'].append({**nota_ref, 'valor': vprod, 'valor_br': _money(vprod)})

                        item_key = (it.get('cProd') or '', it.get('xProd') or '', ncm)
                        ir = by_item[item_key]
                        ir['item'] = it.get('cProd') or ''
                        ir['desc'] = it.get('xProd') or ''
                        ir['ncm'] = ncm
                        ir['qtd_itens'] += 1
                        ir['v_total'] += vprod
                        ir['icms'] += icms
                        ir['pis'] += pis
                        ir['cofins'] += cofins
                        ir['desconto'] += desc
                        ir['outras'] += outras

                        total_geral += vprod
                except Exception:
                    continue

        ncm_linhas = sorted(by_ncm.values(), key=lambda x: x['v_total'], reverse=True)
        item_linhas = sorted(by_item.values(), key=lambda x: x['v_total'], reverse=True)

        for row in ncm_linhas:
            row['v_total_br'] = _money(row['v_total'])
            row['icms_br'] = _money(row['icms'])
            row['pis_br'] = _money(row['pis'])
            row['cofins_br'] = _money(row['cofins'])
            row['desconto_br'] = _money(row['desconto'])
            row['outras_br'] = _money(row['outras'])

        for row in item_linhas:
            row['v_total_br'] = _money(row['v_total'])
            row['icms_br'] = _money(row['icms'])
            row['pis_br'] = _money(row['pis'])
            row['cofins_br'] = _money(row['cofins'])
            row['desconto_br'] = _money(row['desconto'])
            row['outras_br'] = _money(row['outras'])

        top12 = ncm_linhas[:12]
        labels = [r['ncm'] for r in top12]
        valores = [r['v_total'] for r in top12]

        return {
            'total_arquivos': total_arquivos,
            'total_ok': total_ok,
            'total_geral': total_geral,
            'total_geral_br': _money(total_geral),
            'labels': labels,
            'valores': valores,
            'ncm_linhas': ncm_linhas,
            'item_linhas': item_linhas,
        }, None
    except Exception as e:
        return None, f'Falha ao processar ZIP: {e}'
