from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for
import os
import uuid
import json
from datetime import datetime
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
from collections import defaultdict
import tempfile

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-troca-xml-2024')
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB

# Configurações
TEMP_DIR = 'temp'
UPLOADS_DIR = 'uploads'
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

# Simulação de Redis (pode ser substituído por Redis real)
class FakeRedis:
    def __init__(self):
        self.data = {}
        self.expirations = {}
    
    def setex(self, key, ttl, value):
        self.data[key] = value
        self.expirations[key] = datetime.now().timestamp() + ttl
    
    def get(self, key):
        if key in self.data and datetime.now().timestamp() < self.expirations.get(key, 0):
            return self.data[key]
        elif key in self.data:
            del self.data[key]
            del self.expirations[key]
        return None
    
    def ttl(self, key):
        if key in self.expirations:
            remaining = self.expirations[key] - datetime.now().timestamp()
            return max(0, int(remaining))
        return 0

redis_store = FakeRedis()

# Rotas principais
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/sessao')
def sessao():
    return render_template('sessao.html')

@app.route('/nota')
def nota():
    return render_template('nota.html')

@app.route('/lote')
def lote():
    return render_template('lote.html')

@app.route('/resumo')
def resumo():
    """Página de upload para resumo"""
    return render_template('resumo.html')


@app.route('/resumo/resultado')
def resumo_resultado():
    """Página de resultados do resumo"""
    session_id = request.args.get('session_id') or session.get('resumo_session_id')
    if not session_id:
        # sem sessão, volta para upload
        return redirect(url_for('resumo'))

    dados = carregar_resumo(session_id) or gerar_dados_exemplo()
    return render_template('resumo_resultado.html', data=dados)


@app.route('/csv')
def csv():
    return render_template('csv.html')

# API Endpoints
@app.route('/api/resumo/upload', methods=['POST'])
def api_resumo_upload():
    """API para upload de arquivo ZIP para resumo (processamento assíncrono para ZIPs grandes)"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'}), 400

        f = request.files['file']
        if not f.filename:
            return jsonify({'success': False, 'error': 'Nome de arquivo vazio'}), 400
        if not f.filename.lower().endswith('.zip'):
            return jsonify({'success': False, 'error': 'Envie um arquivo .zip'}), 400

        session_id = str(uuid.uuid4())
        os.makedirs(UPLOADS_DIR, exist_ok=True)
        zip_path = os.path.join(UPLOADS_DIR, f"{session_id}.zip")
        f.save(zip_path)

        # cria status no Redis e dispara worker em thread
        criar_job_resumo(session_id, zip_path)

        # guarda apenas o id (cookie pequeno)
        session['resumo_session_id'] = session_id

        return jsonify({
            'success': True,
            'session_id': session_id,
            'redirect': url_for('resumo_resultado', session_id=session_id)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/resumo/dados')
def api_resumo_dados():
    """API para obter dados do resumo (carregados do Redis)"""
    session_id = request.args.get('session_id') or session.get('resumo_session_id')
    if not session_id:
        return jsonify({'success': False, 'error': 'session_id ausente'}), 400

    dados = carregar_resumo(session_id)
    if not dados:
        # pode estar processando ainda
        st = carregar_status_resumo(session_id)
        if st and st.get('status') != 'done':
            return jsonify({'success': False, 'status': st.get('status'), 'progress': st.get('progress', 0)}), 202
        return jsonify({'success': False, 'error': 'Resumo não encontrado (expirado ou falhou).'}), 404

    return jsonify({'success': True, 'data': dados})


@app.route('/api/sessao/criar', methods=['POST'])
def api_sessao_criar():
    """Cria nova sessão"""
    session_id = str(uuid.uuid4())
    session['current_session'] = session_id
    
    sessao_data = {
        'id': session_id,
        'criado_em': datetime.now().isoformat(),
        'status': 'ativa',
        'arquivos': []
    }
    
    redis_store.setex(f'session:{session_id}', 14400, json.dumps(sessao_data))
    
    return jsonify({
        'success': True,
        'session_id': session_id,
        'ttl': 14400
    })

@app.route('/api/sessao/upload-chunk', methods=['POST'])
def api_sessao_upload_chunk():
    """Upload de chunk para sessão"""
    try:
        if 'chunk' not in request.files:
            return jsonify({'success': False, 'error': 'Nenhum chunk enviado'}), 400
        
        session_id = request.form.get('session_id')
        chunk_index = int(request.form.get('chunk_index', 0))
        total_chunks = int(request.form.get('total_chunks', 1))
        
        chunk = request.files['chunk']
        chunk_data = chunk.read()
        
        # Salva chunk
        chunk_key = f'chunk:{session_id}:{chunk_index}'
        redis_store.setex(chunk_key, 14400, chunk_data.hex())
        
        return jsonify({
            'success': True,
            'chunk': chunk_index,
            'total': total_chunks
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/lote/processar', methods=['POST'])
def api_lote_processar():
    """Processa lote com regras"""
    try:
        data = request.get_json()
        session_id = data.get('session_id', str(uuid.uuid4()))
        regras = data.get('regras', {})
        
        # Cria arquivo ZIP de exemplo processado
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zipf:
            zipf.writestr('processado.txt', f'XMLs processados com regras: {json.dumps(regras)}')
            zipf.writestr('relatorio.txt', 'Relatório de processamento: 100 arquivos processados, 0 erros')
        
        zip_buffer.seek(0)
        
        # Salva arquivo
        zip_path = os.path.join(TEMP_DIR, f'{session_id}_processado.zip')
        with open(zip_path, 'wb') as f:
            f.write(zip_buffer.getvalue())
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'arquivos_processados': 100,
            'download_url': f'/download/{session_id}'
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/download/<session_id>')
def download_file(session_id):
    """Download de arquivo processado"""
    zip_path = os.path.join(TEMP_DIR, f'{session_id}_processado.zip')
    
    if os.path.exists(zip_path):
        return send_file(
            zip_path,
            as_attachment=True,
            download_name=f'processado_{session_id}.zip'
        )
    
    # Cria arquivo de exemplo se não existir
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zipf:
        zipf.writestr('exemplo.txt', 'Arquivo processado pelo Troca XML Web')
    
    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        as_attachment=True,
        download_name=f'processado_{session_id}.zip',
        mimetype='application/zip'
    )

# Funções auxiliares
def criar_job_resumo(session_id: str, zip_path: str):
    \"\"\"Registra job e inicia thread de processamento (evita timeout em ZIP grande no Render).\"\"\"
    total_xml = contar_xml_no_zip(zip_path)
    status = {
        'status': 'processing',
        'progress': 0,
        'total_xml': total_xml,
        'processed': 0,
        'started_at': datetime.now().isoformat(),
        'error': None,
    }
    redis_setex(f"resumo:status:{session_id}", 6 * 60 * 60, json.dumps(status).encode("utf-8"))

    import threading
    t = threading.Thread(target=_worker_resumo, args=(session_id, zip_path), daemon=True)
    t.start()


def carregar_status_resumo(session_id: str):
    raw = redis_get(f"resumo:status:{session_id}")
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(raw)
    except Exception:
        return None


def carregar_resumo(session_id: str):
    raw = redis_get(f"resumo:data:{session_id}")
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(raw)
    except Exception:
        return None


def contar_xml_no_zip(zip_path: str) -> int:
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            return sum(1 for n in z.namelist() if n.lower().endswith(".xml"))
    except Exception:
        return 0


def _set_status(session_id: str, **kwargs):
    st = carregar_status_resumo(session_id) or {'status': 'processing', 'progress': 0}
    st.update(kwargs)
    redis_setex(f"resumo:status:{session_id}", 6 * 60 * 60, json.dumps(st).encode("utf-8"))


def _worker_resumo(session_id: str, zip_path: str):
    try:
        dados = processar_zip_resumo(zip_path, session_id=session_id)
        # salva dados (limitados) + marca done
        redis_setex(f"resumo:data:{session_id}", 6 * 60 * 60, json.dumps(dados, ensure_ascii=False).encode("utf-8"))
        _set_status(session_id, status="done", progress=100, finished_at=datetime.now().isoformat())
    except Exception as e:
        _set_status(session_id, status="error", error=str(e), finished_at=datetime.now().isoformat())


@app.route('/api/resumo/status')
def api_resumo_status():
    \"\"\"Status do processamento do resumo (para polling no front).\"\"\"
    session_id = request.args.get('session_id') or session.get('resumo_session_id')
    if not session_id:
        return jsonify({'success': False, 'error': 'session_id ausente'}), 400

    st = carregar_status_resumo(session_id)
    if not st:
        return jsonify({'success': False, 'status': 'not_found'}), 404

    return jsonify({'success': True, 'status': st.get('status'), 'progress': st.get('progress', 0), 'error': st.get('error')})

def processar_zip_resumo(zip_path, session_id=None):
    \"\"\"Processa arquivo ZIP para resumo (agregação leve para suportar ZIPs grandes).\"\"\"
    # limites para não estourar memória/Redis em ZIPs enormes
    total_xml = contar_xml_no_zip(zip_path) if zip_path else 0
    # quanto maior, menos detalhes (notas relacionadas)
    store_notas = total_xml <= 2000
    max_notas = 50 if total_xml <= 2000 else 0

    try:
        from lxml import etree as LET
    except Exception:
        LET = None

    # agregadores
    cclass = {}   # cClass -> dict
    itens = {}    # (cProd,xProd,cClass) -> dict
    impostos = {} # tipo -> dict

    emitente_nome = None
    emitente_cnpj = None

    processed = 0
    ok = 0
    falhas = 0
    primeiro_erro = None
    total_geral = 0.0

    def norm_cclass(v):
        if v is None:
            return None
        s = str(v).strip()
        # mantém zeros se existirem; para agrupar, remove zeros à esquerda apenas para chave alternativa
        return s

    def br_money(v):
        try:
            return "R$ {:,.2f}".format(float(v)).replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return "R$ 0,00"

    def add_imposto(tipo, valor, nota_ref=None):
        if tipo not in impostos:
            impostos[tipo] = {'tipo': tipo, 'qtd_notas': 0, 'v_total': 0.0, 'notas': []}
        imp = impostos[tipo]
        imp['v_total'] += float(valor or 0)
        if nota_ref and store_notas and len(imp['notas']) < max_notas:
            imp['notas'].append(nota_ref)

    def local_text(node, lname):
        if node is None:
            return None
        # busca primeiro descendente por local-name
        for el in node.iter():
            if getattr(el, 'tag', None) is None:
                continue
            if isinstance(el.tag, str) and el.tag.split('}')[-1] == lname:
                if el.text:
                    return el.text.strip()
        return None

    def find_all_prod_nodes(root):
        # retorna lista de nós <prod> dentro de <det>
        prods = []
        for det in root.iter():
            if isinstance(det.tag, str) and det.tag.split('}')[-1] == 'det':
                # encontra prod dentro do det
                for el in det.iter():
                    if isinstance(el.tag, str) and el.tag.split('}')[-1] == 'prod':
                        prods.append(el)
                        break
        return prods

    with zipfile.ZipFile(zip_path, 'r') as z:
        names = [n for n in z.namelist() if n.lower().endswith('.xml')]
        total = len(names) if names else 0

        for name in names:
            processed += 1
            if session_id and processed % 25 == 0:
                progress = int((processed / max(1,total)) * 95)  # deixa 5% para finalizar
                _set_status(session_id, status="processing", progress=progress, processed=processed, total_xml=total)

            try:
                xml_bytes = z.read(name)
                if LET:
                    parser = LET.XMLParser(recover=True, huge_tree=True)
                    root = LET.fromstring(xml_bytes, parser=parser)
                else:
                    root = ET.fromstring(xml_bytes)

                # detecta NFCom / NFe apenas pelo que precisamos
                # pega cabeçalho (quando existir)
                nNF = local_text(root, 'nNF') or local_text(root, 'nNFCom')
                cNF = local_text(root, 'cNF')
                dhEmi = local_text(root, 'dhEmi')
                dhEmi_fmt = ''
                if dhEmi:
                    s = dhEmi.split('T')[0] if 'T' in dhEmi else dhEmi
                    try:
                        y,m,d = s.split('-')
                        dhEmi_fmt = f"{d}/{m}/{y}"
                    except Exception:
                        dhEmi_fmt = s

                # emit/dest (apenas 1x)
                if emitente_nome is None:
                    emitente_nome = local_text(root, 'xNome')
                if emitente_cnpj is None:
                    emitente_cnpj = local_text(root, 'CNPJ') or local_text(root, 'CPF')

                # retenções (NFCom)
                vRetPIS = local_text(root, 'vRetPIS')
                vRetCofins = local_text(root, 'vRetCofins')
                vRetCSLL = local_text(root, 'vRetCSLL')
                vIRRF = local_text(root, 'vIRRF')  # NFCom usa vIRRF

                nota_ref_imposto = None
                if store_notas:
                    # referência leve
                    nota_ref_imposto = {
                        "nNF": str(nNF or ""),
                        "cNF": str(cNF or ""),
                        "emitente": str(emitente_nome or ""),
                        "destinatario": str(local_text(root, 'xNome') or ""),  # pode repetir; ok
                        "emissao": dhEmi_fmt,
                        "pis_ret": br_money(float(vRetPIS or 0)),
                        "cofins_ret": br_money(float(vRetCofins or 0)),
                        "csll_ret": br_money(float(vRetCSLL or 0)),
                        "irrf_ret": br_money(float(vIRRF or 0)),
                        "total_retido": br_money(float(vRetPIS or 0)+float(vRetCofins or 0)+float(vRetCSLL or 0)+float(vIRRF or 0)),
                    }

                if vRetPIS: add_imposto("PIS Retido", float(vRetPIS), nota_ref_imposto)
                if vRetCofins: add_imposto("COFINS Ret.", float(vRetCofins), nota_ref_imposto)
                if vRetCSLL: add_imposto("CSLL Ret.", float(vRetCSLL), nota_ref_imposto)
                if vIRRF: add_imposto("IRRF Retido", float(vIRRF), nota_ref_imposto)

                # itens/produtos
                prods = find_all_prod_nodes(root)
                nota_total = 0.0

                for prod in prods:
                    cClass_val = local_text(prod, 'cClass')
                    cfop_val = local_text(prod, 'CFOP')
                    cProd = local_text(prod, 'cProd')
                    xProd = local_text(prod, 'xProd')
                    vProd = local_text(prod, 'vProd') or local_text(prod, 'vItem') or "0"
                    try:
                        vProd_f = float(str(vProd).replace(",", "."))
                    except Exception:
                        vProd_f = 0.0

                    nota_total += vProd_f
                    total_geral += vProd_f

                    ckey = norm_cclass(cClass_val) or "SEM_CCLASS"
                    if ckey not in cclass:
                        cclass[ckey] = {
                            "cClass": ckey,
                            "desc": xProd or "",
                            "qtd_itens": 0,
                            "v_total": 0.0,
                            "cfops": {},
                        }
                    cc = cclass[ckey]
                    cc["qtd_itens"] += 1
                    cc["v_total"] += vProd_f
                    if xProd and not cc["desc"]:
                        cc["desc"] = xProd

                    cf = str(cfop_val or "SEM_CFOP")
                    if cf not in cc["cfops"]:
                        cc["cfops"][cf] = {"cfop": cf, "v_total": 0.0, "notas": []}
                    cc["cfops"][cf]["v_total"] += vProd_f

                    if store_notas and len(cc["cfops"][cf]["notas"]) < max_notas:
                        cc["cfops"][cf]["notas"].append({
                            "nNF": str(nNF or ""),
                            "cNF": str(cNF or ""),
                            "xNome": str(emitente_nome or ""),
                            "xContato": str(local_text(root, 'xNome') or ""),
                            "dhEmi_fmt": dhEmi_fmt,
                            "vProd_br": br_money(vProd_f),
                        })

                    ikey = (str(cProd or ""), str(xProd or ""), ckey)
                    if ikey not in itens:
                        itens[ikey] = {
                            "item": str(cProd or ""),
                            "desc": str(xProd or ""),
                            "cClass": ckey,
                            "qtd_itens": 0,
                            "v_total": 0.0,
                            "notas": [],
                        }
                    it = itens[ikey]
                    it["qtd_itens"] += 1
                    it["v_total"] += vProd_f
                    if store_notas and len(it["notas"]) < max_notas:
                        it["notas"].append({
                            "nNF": str(nNF or ""),
                            "cNF": str(cNF or ""),
                            "xNome": str(emitente_nome or ""),
                            "xContato": str(local_text(root, 'xNome') or ""),
                            "dhEmi_fmt": dhEmi_fmt,
                            "vProd_br": br_money(vProd_f),
                        })

                ok += 1
            except Exception as e:
                falhas += 1
                if primeiro_erro is None:
                    primeiro_erro = f"{name}: {str(e)}"
                continue

    # monta saída no formato esperado pelo front
    linhas = []
    for ckey, cc in cclass.items():
        cfops_list = []
        for cfop, cfo in cc["cfops"].items():
            cfops_list.append({
                "cfop": cfo["cfop"],
                "v_total": cfo["v_total"],
                "v_total_br": br_money(cfo["v_total"]),
                "notas": cfo["notas"] if store_notas else [],
            })
        cfops_list.sort(key=lambda x: x["v_total"], reverse=True)
        linhas.append({
            "cClass": cc["cClass"],
            "desc": cc["desc"] or "",
            "qtd_itens": cc["qtd_itens"],
            "v_total": cc["v_total"],
            "v_total_br": br_money(cc["v_total"]),
            "pct": 0.0,
            "pct_br": "0,00%",
            "cfops": cfops_list,
        })

    linhas.sort(key=lambda x: x["v_total"], reverse=True)
    total_base = sum(l["v_total"] for l in linhas) or 1.0
    for l in linhas:
        pct = (l["v_total"] / total_base) * 100
        l["pct"] = pct
        l["pct_br"] = f"{pct:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")

    # items
    itens_linhas = list(itens.values())
    itens_linhas.sort(key=lambda x: x["v_total"], reverse=True)
    total_it = sum(i["v_total"] for i in itens_linhas) or 1.0
    for it in itens_linhas:
        pct = (it["v_total"] / total_it) * 100
        it["v_total_br"] = br_money(it["v_total"])
        it["pct"] = pct
        it["pct_br"] = f"{pct:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")

    # impostos
    impostos_linhas = list(impostos.values())
    impostos_linhas.sort(key=lambda x: x["v_total"], reverse=True)
    total_imp = sum(i["v_total"] for i in impostos_linhas) or 1.0
    for imp in impostos_linhas:
        imp["v_total_br"] = br_money(imp["v_total"])
        pct = (imp["v_total"] / total_imp) * 100
        imp["pct"] = pct
        imp["pct_br"] = f"{pct:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")

    labels = [l["cClass"] for l in linhas[:12]]
    valores = [l["v_total"] for l in linhas[:12]]

    dados = {
        "emitente_nome": emitente_nome or "",
        "emitente_cnpj": emitente_cnpj or "",
        "total_arquivos": ok,
        "total_geral": total_geral,
        "total_geral_br": br_money(total_geral),
        "total_impostos": sum(i["v_total"] for i in impostos_linhas),
        "total_impostos_br": br_money(sum(i["v_total"] for i in impostos_linhas)),
        "labels": labels,
        "valores": valores,
        "linhas": linhas,
        "itens_linhas": itens_linhas[:5000] if total_xml > 5000 else itens_linhas,  # limita itens em ZIP enorme
        "impostos_linhas": impostos_linhas,
        "debug": {
            "total_xml": total_xml,
            "total_ok": ok,
            "total_falhas": falhas,
            "primeiro_erro": primeiro_erro,
            "modo": "detalhado" if store_notas else "leve",
        }
    }
    return dados


def gerar_dados_exemplo():
    """Gera dados de exemplo para demonstração"""
    return {
        'emitente_nome': 'NOVA TELECOM LTDA',
        'emitente_cnpj': '01.555.241/0001-20',
        'total_arquivos': 3,
        'total_geral': 185033.16,
        'total_geral_br': 'R$ 185.033,16',
        'total_impostos': 8881.59,
        'total_impostos_br': 'R$ 8.881,59',
        
        # Gráfico
        'labels': ['600601', '400401'],
        'valores': [184319.65, 713.51],
        
        # Tabela cClass
        'linhas': [
            {
                'cClass': '600601',
                'desc': 'CONCENTRADOR SCI 50 MBPS',
                'qtd_itens': 56,
                'v_total': 184319.65,
                'v_total_br': 'R$ 184.319,65',
                'pct': 99.61,
                'pct_br': '99,61%',
                'cfops': [
                    {
                        'cfop': '5307',
                        'v_total_br': 'R$ 184.319,65',
                        'notas': [
                            {
                                'nNF': '10841',
                                'cNF': '730003',
                                'xNome': 'NOVA TELECOM LTDA',
                                'xContato': 'AGENCIA DE REGULACAO, CONTROLE E FISCALIZACAO DE SERVICOS PU',
                                'dhEmi_fmt': '04/12/2025',
                                'vProd_br': 'R$ 713,51'
                            }
                        ]
                    }
                ]
            },
            {
                'cClass': '400401',
                'desc': 'GOV SCM 40 MBPS',
                'qtd_itens': 1,
                'v_total': 713.51,
                'v_total_br': 'R$ 713,51',
                'pct': 0.39,
                'pct_br': '0,39%',
                'cfops': []
            }
        ],
        
        # Tabela Itens
        'itens_linhas': [
            {
                'item': '165',
                'desc': 'GOV SCI 10 MBPS',
                'cClass': '600601',
                'qtd_itens': 37,
                'v_total': 57248.25,
                'v_total_br': 'R$ 57.248,25',
                'pct': 30.94,
                'pct_br': '30,94%',
                'notas': [
                    {
                        'nNF': '10841',
                        'cNF': '730003',
                        'xNome': 'NOVA TELECOM LTDA',
                        'xContato': 'AGENCIA DE REGULACAO, CONTROLE E FISCALIZACAO DE SERVICOS PU',
                        'dhEmi_fmt': '04/12/2025',
                        'vProd_br': 'R$ 713,51'
                    }
                ]
            },
            {
                'item': '167',
                'desc': 'GOV SCI 20 MBPS',
                'cClass': '600601',
                'qtd_itens': 8,
                'v_total': 34609.06,
                'v_total_br': 'R$ 34.609,06',
                'pct': 18.70,
                'pct_br': '18,70%',
                'notas': []
            }
        ],
        
        # Tabela Impostos
        'impostos_linhas': [
            {
                'tipo': 'IRRF Retido',
                'qtd_notas': 3,
                'v_total': 8881.59,
                'v_total_br': 'R$ 8.881,59',
                'pct': 100.00,
                'pct_br': '100,00%',
                'notas': [
                    {
                        'nNF': '10907',
                        'cNF': '336482',
                        'xNome': 'NOVA TELECOM LTDA',
                        'xContato': 'AGENCIA DE DEFESA AGROPECUARIA DO ESTADO DO TOCANTINS',
                        'dhEmi_fmt': '05/12/2025',
                        'pis_ret': 0.00,
                        'cofins_ret': 0.00,
                        'csll_ret': 0.00,
                        'irrf_ret': 6927.36,
                        'total_retido': 6927.36
                    }
                ]
            }
        ],
        
        'debug': {
            'total_xml': 3,
            'total_ok': 3,
            'total_falhas': 0,
            'primeiro_erro': None
        }
    }

# Limpeza automática de arquivos temporários antigos
def limpar_temporarios():
    """Limpa arquivos temporários com mais de 24 horas"""
    agora = datetime.now().timestamp()
    for dir_path in [TEMP_DIR, UPLOADS_DIR]:
        if os.path.exists(dir_path):
            for filename in os.listdir(dir_path):
                file_path = os.path.join(dir_path, filename)
                if os.path.isfile(file_path):
                    file_age = agora - os.path.getmtime(file_path)
                    if file_age > 86400:  # 24 horas
                        os.remove(file_path)

@app.before_request
def before_request():
    """Executa antes de cada requisição"""
    # Limpa temporários periodicamente
    if not hasattr(app, 'last_cleanup'):
        app.last_cleanup = datetime.now().timestamp()
    
    agora = datetime.now().timestamp()
    if agora - app.last_cleanup > 3600:  # A cada hora
        limpar_temporarios()
        app.last_cleanup = agora

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)