from flask import Flask, render_template, request, jsonify, send_file, session
from core import XMLProcessor, SessionManager
import os
import uuid
import json
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key')
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB

# Inicializa processadores
xml_processor = XMLProcessor()
session_manager = SessionManager()

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

# APIs
@app.route('/api/sessao/criar', methods=['POST'])
def criar_sessao():
    """Cria uma nova sessão"""
    session_id = str(uuid.uuid4())
    session['session_id'] = session_id
    session_manager.criar_sessao(session_id)
    return jsonify({'session_id': session_id, 'ttl': 14400})

@app.route('/api/sessao/upload-chunk', methods=['POST'])
def upload_chunk():
    """Upload de chunk de arquivo"""
    if 'chunk' not in request.files:
        return jsonify({'error': 'Nenhum chunk enviado'}), 400
    
    session_id = request.form.get('session_id')
    chunk_index = int(request.form.get('chunk_index', 0))
    total_chunks = int(request.form.get('total_chunks', 1))
    
    chunk = request.files['chunk']
    
    # Salva chunk temporariamente
    result = session_manager.salvar_chunk(session_id, chunk_index, chunk)
    
    return jsonify({
        'success': True,
        'chunk': chunk_index,
        'total': total_chunks,
        'chunkSize': f"{len(chunk.read()) / 1024 / 1024:.2f}"
    })

@app.route('/api/sessao/processar', methods=['POST'])
def processar_sessao():
    """Processa XMLs da sessão"""
    session_id = request.json.get('session_id')
    opcoes = request.json.get('opcoes', {})
    
    resultado = xml_processor.processar_sessao(session_id, opcoes)
    
    return jsonify(resultado)

@app.route('/api/resumo/gerar', methods=['POST'])
def gerar_resumo():
    """Gera resumo consolidado"""
    session_id = request.json.get('session_id')
    resultado = xml_processor.gerar_resumo(session_id)
    
    return jsonify(resultado)

@app.route('/api/lote/processar', methods=['POST'])
def processar_lote():
    """Processa lote com regras"""
    session_id = request.json.get('session_id')
    regras = request.json.get('regras', [])
    
    resultado = xml_processor.processar_lote(session_id, regras)
    
    # Gera arquivo ZIP para download
    zip_path = f"temp/{session_id}_processado.zip"
    resultado['download_url'] = f"/download/{session_id}"
    
    return jsonify(resultado)

@app.route('/download/<session_id>')
def download_arquivo(session_id):
    """Download de arquivo processado"""
    zip_path = f"temp/{session_id}_processado.zip"
    return send_file(zip_path, as_attachment=True)

@app.route('/api/sessao/status')
def status_sessao():
    """Retorna status da sessão"""
    session_id = request.args.get('session_id')
    status = session_manager.get_status(session_id)
    return jsonify(status)

if __name__ == '__main__':
    os.makedirs('temp', exist_ok=True)
    os.makedirs('sessoes', exist_ok=True)
    app.run(debug=True, port=5000)