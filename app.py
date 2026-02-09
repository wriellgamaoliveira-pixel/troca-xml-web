from flask import Flask, render_template, request, jsonify, send_file, session
import os
import uuid
import json
from datetime import datetime
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
from collections import defaultdict

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-12345')
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB

# Diretórios temporários
TEMP_DIR = 'temp'
SESSOES_DIR = 'sessoes'

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(SESSOES_DIR, exist_ok=True)

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

storage = MemoryStorage()

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
    """Página de resumo - mostra formulário de upload"""
    return render_template('resumo.html')

@app.route('/resumo/resultado')
def resumo_resultado():
    """Página de resultado do resumo (após processamento)"""
    # Em produção, esses dados viriam do banco/Redis
    dados_resumo = gerar_dados_exemplo()
    return render_template('resumo_resultado.html', data=dados_resumo)

@app.route('/csv')
def csv():
    return render_template('csv.html')

# API Routes
@app.route('/api/resumo/upload', methods=['POST'])
def upload_resumo():
    """Processa upload para resumo"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'Nenhum arquivo enviado'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Nenhum arquivo selecionado'}), 400
        
        # Salva arquivo temporariamente
        session_id = str(uuid.uuid4())
        file_path = os.path.join(TEMP_DIR, f'{session_id}.zip')
        file.save(file_path)
        
        # Simula processamento
        dados = processar_arquivo_resumo(file_path)
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'data': dados
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/resumo/dados')
def get_dados_resumo():
    """Retorna dados para o resumo"""
    session_id = request.args.get('session_id')
    
    if not session_id:
        # Retorna dados de exemplo
        dados = gerar_dados_exemplo()
        return jsonify(dados)
    
    # Aqui você recuperaria dados reais do armazenamento
    dados = gerar_dados_exemplo()
    return jsonify(dados)

def processar_arquivo_resumo(zip_path):
    """Processa arquivo ZIP e retorna dados estruturados"""
    # Esta é uma implementação simplificada
    # Em produção, você processaria os XMLs reais
    
    return gerar_dados_exemplo()

def gerar_dados_exemplo():
    """Gera dados de exemplo conforme as imagens fornecidas"""
    return {
        'emitente_nome': 'NOVA TELECOM LTDA',
        'emitente_cnpj': '01.555.241/0001-20',
        'total_arquivos': 3,
        'total_geral': 185033.16,
        'total_geral_br': 'R$ 185.033,16',
        'total_impostos': 8881.59,
        'total_impostos_br': 'R$ 8.881,59',
        
        # Dados para gráfico
        'labels': ['600601', '400401'],
        'valores': [184319.65, 713.51],
        
        # Linhas por cClass
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
                        'v_total': 184319.65,
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
        
        # Itens
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
            },
            {
                'item': '770',
                'desc': 'GOV SCI 4000MBPS',
                'cClass': '600601',
                'qtd_itens': 1,
                'v_total': 33200.00,
                'v_total_br': 'R$ 33.200,00',
                'pct': 17.94,
                'pct_br': '17,94%',
                'notas': []
            },
            {
                'item': '690',
                'desc': 'CONCENTRADOR SCI 1000 MBPS',
                'cClass': '600601',
                'qtd_itens': 1,
                'v_total': 30000.00,
                'v_total_br': 'R$ 30.000,00',
                'pct': 16.21,
                'pct_br': '16,21%',
                'notas': []
            },
            {
                'item': '168',
                'desc': 'GOV SCI 50 MBPS',
                'cClass': '600601',
                'qtd_itens': 2,
                'v_total': 11280.00,
                'v_total_br': 'R$ 11.280,00',
                'pct': 6.10,
                'pct_br': '6,10%',
                'notas': []
            },
            {
                'item': '163',
                'desc': 'GOV SCI 300 MBPS',
                'cClass': '600601',
                'qtd_itens': 1,
                'v_total': 7200.00,
                'v_total_br': 'R$ 7.200,00',
                'pct': 3.89,
                'pct_br': '3,89%',
                'notas': []
            },
            {
                'item': '577',
                'desc': 'GOV SCI 10MBPS',
                'cClass': '600601',
                'qtd_itens': 3,
                'v_total': 4641.75,
                'v_total_br': 'R$ 4.641,75',
                'pct': 2.51,
                'pct_br': '2,51%',
                'notas': []
            },
            {
                'item': '158',
                'desc': 'PROVIMENTO DE ACESSO A INTERNET - SCI',
                'cClass': '600601',
                'qtd_itens': 1,
                'v_total': 2854.04,
                'v_total_br': 'R$ 2.854,04',
                'pct': 1.54,
                'pct_br': '1,54%',
                'notas': []
            },
            {
                'item': '451',
                'desc': 'CONCENTRADOR SCI 50 MBPS',
                'cClass': '600601',
                'qtd_itens': 1,
                'v_total': 2027.87,
                'v_total_br': 'R$ 2.027,87',
                'pct': 1.10,
                'pct_br': '1,10%',
                'notas': []
            },
            {
                'item': '175',
                'desc': 'GOV SCI 30 MBPS',
                'cClass': '600601',
                'qtd_itens': 1,
                'v_total': 1258.68,
                'v_total_br': 'R$ 1.258,68',
                'pct': 0.68,
                'pct_br': '0,68%',
                'notas': []
            },
            {
                'item': '158',
                'desc': 'GOV SCM 40 MBPS',
                'cClass': '400401',
                'qtd_itens': 1,
                'v_total': 713.51,
                'v_total_br': 'R$ 713,51',
                'pct': 0.39,
                'pct_br': '0,39%',
                'notas': []
            }
        ],
        
        # Impostos
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
                        'vProd_br': 'R$ 0,00',
                        'pis_ret': 0.00,
                        'cofins_ret': 0.00,
                        'csll_ret': 0.00,
                        'irrf_ret': 6927.36,
                        'total_retido': 6927.36
                    },
                    {
                        'nNF': '10896',
                        'cNF': '212182',
                        'xNome': 'NOVA TELECOM LTDA',
                        'xContato': 'AGENCIA DE TECNOLOGIA DA INFORMACAO',
                        'dhEmi_fmt': '05/12/2025',
                        'vProd_br': 'R$ 0,00',
                        'pis_ret': 0.00,
                        'cofins_ret': 0.00,
                        'csll_ret': 0.00,
                        'irrf_ret': 1593.60,
                        'total_retido': 1593.60
                    },
                    {
                        'nNF': '10841',
                        'cNF': '730003',
                        'xNome': 'NOVA TELECOM LTDA',
                        'xContato': 'AGENCIA DE REGULACAO, CONTROLE E FISCALIZACAO DE SERVICOS PU',
                        'dhEmi_fmt': '04/12/2025',
                        'vProd_br': 'R$ 0,00',
                        'pis_ret': 0.00,
                        'cofins_ret': 0.00,
                        'csll_ret': 0.00,
                        'irrf_ret': 360.63,
                        'total_retido': 360.63
                    }
                ]
            }
        ],
        
        # Debug info
        'debug': {
            'total_xml': 3,
            'total_ok': 3,
            'total_falhas': 0,
            'primeiro_erro': None
        }
    }

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)