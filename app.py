from flask import Flask, render_template, request, jsonify, send_file, session
import os
import uuid
import json
from datetime import datetime
import zipfile
import io
import xml.etree.ElementTree as ET

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-12345')
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB

# Diretórios temporários
TEMP_DIR = 'temp'
SESSOES_DIR = 'sessoes'

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(SESSOES_DIR, exist_ok=True)

# Simulação de Redis para desenvolvimento
class MemoryStorage:
    def __init__(self):
        self.data = {}
        self.chunks = {}
    
    def setex(self, key, ttl, value):
        self.data[key] = {
            'value': value,
            'expires': datetime.now().timestamp() + ttl
        }
    
    def get(self, key):
        if key in self.data:
            item = self.data[key]
            if datetime.now().timestamp() < item['expires']:
                return item['value']
            else:
                del self.data[key]
        return None
    
    def ttl(self, key):
        if key in self.data:
            item = self.data[key]
            remaining = item['expires'] - datetime.now().timestamp()
            return max(0, int(remaining))
        return 0

# Inicializa armazenamento
storage = MemoryStorage()

@app.route('/')
def index():
    """Página inicial"""
    return render_template('index.html')

@app.route('/sessao')
def sessao():
    """Página de sessão"""
    return render_template('sessao.html')

@app.route('/nota')
def nota():
    """Página de nota única"""
    return render_template('nota.html')

@app.route('/lote')
def lote():
    """Página de lote"""
    return render_template('lote.html')

@app.route('/resumo')
def resumo():
    """Página de resumo"""
    return render_template('resumo.html')

@app.route('/csv')
def csv():
    """Página de exportação CSV"""
    return render_template('csv.html')

# API Routes
@app.route('/api/sessao/criar', methods=['POST'])
def criar_sessao():
    """Cria uma nova sessão"""
    session_id = str(uuid.uuid4())
    session['session_id'] = session_id
    
    sessao_data = {
        'id': session_id,
        'criado_em': datetime.now().isoformat(),
        'status': 'ativa',
        'arquivos': [],
        'chunks_recebidos': 0
    }
    
    storage.setex(f'session:{session_id}', 14400, json.dumps(sessao_data))
    
    return jsonify({
        'success': True,
        'session_id': session_id,
        'ttl': 14400
    })

@app.route('/api/sessao/upload-chunk', methods=['POST'])
def upload_chunk():
    """Upload de chunk de arquivo"""
    try:
        if 'chunk' not in request.files:
            return jsonify({'error': 'Nenhum chunk enviado'}), 400
        
        session_id = request.form.get('session_id')
        chunk_index = int(request.form.get('chunk_index', 0))
        total_chunks = int(request.form.get('total_chunks', 1))
        file_name = request.form.get('file_name', 'arquivo.zip')
        
        chunk = request.files['chunk']
        chunk_data = chunk.read()
        
        # Salva chunk em arquivo temporário
        chunk_path = os.path.join(TEMP_DIR, f'{session_id}_chunk_{chunk_index}')
        with open(chunk_path, 'wb') as f:
            f.write(chunk_data)
        
        # Atualiza sessão
        sessao_data = storage.get(f'session:{session_id}')
        if sessao_data:
            sessao = json.loads(sessao_data)
            sessao['chunks_recebidos'] += 1
            if 'file_name' not in sessao:
                sessao['file_name'] = file_name
            storage.setex(f'session:{session_id}', 14400, json.dumps(sessao))
        
        return jsonify({
            'success': True,
            'chunk': chunk_index,
            'total': total_chunks,
            'chunkSize': f"{len(chunk_data) / 1024 / 1024:.2f} MB"
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sessao/processar', methods=['POST'])
def processar_sessao():
    """Processa XMLs da sessão"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        
        # Monta arquivo a partir dos chunks
        chunks = []
        for i in range(100):  # Assume no máximo 100 chunks
            chunk_path = os.path.join(TEMP_DIR, f'{session_id}_chunk_{i}')
            if os.path.exists(chunk_path):
                with open(chunk_path, 'rb') as f:
                    chunks.append(f.read())
            else:
                break
        
        # Combina chunks
        file_data = b''.join(chunks)
        
        # Salva arquivo completo
        file_path = os.path.join(SESSOES_DIR, f'{session_id}.zip')
        with open(file_path, 'wb') as f:
            f.write(file_data)
        
        # Processa o arquivo ZIP
        resultados = processar_zip(file_path)
        
        return jsonify({
            'success': True,
            'total_xmls': resultados.get('total', 0),
            'dados_agregados': resultados.get('dados', {}),
            'status': 'processado'
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/resumo/gerar', methods=['POST'])
def gerar_resumo():
    """Gera resumo consolidado"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        
        # Gera dados de exemplo
        resumo = {
            'emitente_nome': 'NOVA TELECOM LTDA',
            'emitente_cnpj': '01.555.241/0001-20',
            'total_arquivos': 2707,
            'total_geral': 292091.83,
            'total_geral_br': 'R$ 292.091,83',
            'linhas': [
                {
                    'cClass': '600601',
                    'desc': 'Serviços de Telecomunicações',
                    'qtd_itens': 56,
                    'v_total': 184319.65,
                    'v_total_br': 'R$ 184.319,65',
                    'pct': 99.61,
                    'pct_br': '99,61%'
                }
            ],
            'labels': ['600601', '400401'],
            'valores': [184319.65, 713.51],
            'itens_linhas': [
                {
                    'item': 'SERV-TELECOM',
                    'desc': 'Serviços de Telecomunicações',
                    'cClass': '600601',
                    'qtd_itens': 56,
                    'v_total': 184319.65,
                    'v_total_br': 'R$ 184.319,65',
                    'pct': 99.61,
                    'pct_br': '99,61%'
                }
            ],
            'impostos_linhas': [
                {
                    'tipo': 'PIS Retido',
                    'qtd_notas': 12,
                    'v_total': 1200.00,
                    'v_total_br': 'R$ 1.200,00',
                    'pct': 10.0,
                    'pct_br': '10,00%'
                }
            ],
            'total_impostos': 12000.00,
            'total_impostos_br': 'R$ 12.000,00'
        }
        
        return jsonify(resumo)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/lote/processar', methods=['POST'])
def processar_lote():
    """Processa lote com regras"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        regras = data.get('regras', {})
        
        # Cria um arquivo ZIP de exemplo
        zip_path = os.path.join(TEMP_DIR, f'{session_id}_processado.zip')
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            # Adiciona um arquivo de exemplo
            zipf.writestr('exemplo.txt', 'Arquivo processado com sucesso!')
        
        return jsonify({
            'success': True,
            'arquivos_processados': 1,
            'regras_aplicadas': regras,
            'download_url': f'/download/{session_id}'
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download/<session_id>')
def download_arquivo(session_id):
    """Download de arquivo processado"""
    try:
        zip_path = os.path.join(TEMP_DIR, f'{session_id}_processado.zip')
        
        if not os.path.exists(zip_path):
            # Cria um arquivo de exemplo se não existir
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                zipf.writestr('exemplo.txt', 'Este é um arquivo de exemplo processado pelo sistema.')
        
        return send_file(
            zip_path,
            as_attachment=True,
            download_name=f'processado_{session_id}.zip'
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sessao/status')
def status_sessao():
    """Retorna status da sessão"""
    session_id = request.args.get('session_id')
    
    if not session_id:
        return jsonify({'error': 'session_id não fornecido'}), 400
    
    sessao_data = storage.get(f'session:{session_id}')
    
    if not sessao_data:
        return jsonify({'status': 'nao_encontrada'})
    
    sessao = json.loads(sessao_data)
    ttl = storage.ttl(f'session:{session_id}')
    
    return jsonify({
        'status': sessao.get('status', 'desconhecido'),
        'chunks_recebidos': sessao.get('chunks_recebidos', 0),
        'ttl_restante': ttl,
        'criado_em': sessao.get('criado_em')
    })

def processar_zip(zip_path):
    """Processa arquivo ZIP contendo XMLs"""
    resultados = {
        'total': 0,
        'dados': {},
        'erros': []
    }
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zipf:
            for file_name in zipf.namelist():
                if file_name.lower().endswith('.xml'):
                    try:
                        with zipf.open(file_name) as xml_file:
                            xml_content = xml_file.read()
                            # Processa XML
                            dados = parse_xml(xml_content)
                            if dados:
                                resultados['total'] += 1
                                # Aqui você pode agregar os dados
                    except Exception as e:
                        resultados['erros'].append(f"Erro no arquivo {file_name}: {str(e)}")
    
    except Exception as e:
        resultados['erros'].append(f"Erro ao processar ZIP: {str(e)}")
    
    return resultados

def parse_xml(xml_content):
    """Parse básico de XML"""
    try:
        root = ET.fromstring(xml_content)
        
        # Extrai informações básicas
        dados = {
            'tipo': root.tag.split('}')[-1] if '}' in root.tag else root.tag,
            'elementos': len(list(root.iter()))
        }
        
        return dados
    
    except Exception as e:
        return None

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
