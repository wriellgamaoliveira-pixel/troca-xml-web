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
    # Em produção, pegaria dados da sessão ou Redis
    dados = gerar_dados_exemplo()
    return render_template('resumo_resultado.html', data=dados)

@app.route('/csv')
def csv():
    return render_template('csv.html')

# API Endpoints
@app.route('/api/resumo/upload', methods=['POST'])
def api_resumo_upload():
    """API para upload de arquivo ZIP para resumo"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'Nome de arquivo vazio'}), 400
        
        if not file.filename.lower().endswith('.zip'):
            return jsonify({'success': False, 'error': 'Apenas arquivos ZIP são aceitos'}), 400
        
        # Salva arquivo temporariamente
        session_id = str(uuid.uuid4())
        zip_path = os.path.join(UPLOADS_DIR, f'{session_id}.zip')
        file.save(zip_path)
        
        # Processa o arquivo (simulação)
        processar_resultados = processar_zip_resumo(zip_path)
        
        # Salva resultados na sessão
        session['resumo_session_id'] = session_id
        session['resumo_data'] = processar_resultados
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'redirect': url_for('resumo_resultado')
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/resumo/dados')
def api_resumo_dados():
    """API para obter dados do resumo"""
    session_id = request.args.get('session_id')
    
    if session_id and session_id == session.get('resumo_session_id'):
        dados = session.get('resumo_data', gerar_dados_exemplo())
    else:
        dados = gerar_dados_exemplo()
    
    return jsonify(dados)

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
def processar_zip_resumo(zip_path):
    """Processa arquivo ZIP para resumo (simulação)"""
    # Em produção, processaria os XMLs reais
    return gerar_dados_exemplo()

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