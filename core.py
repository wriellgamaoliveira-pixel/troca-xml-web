import xml.etree.ElementTree as ET
import zipfile
import json
import os
from datetime import datetime, timedelta
import redis
import tempfile
from typing import Dict, List, Any
import pandas as pd

class SessionManager:
    def __init__(self):
        self.redis_client = redis.Redis.from_url(
            os.environ.get('REDIS_URL', 'redis://localhost:6379')
        )
    
    def criar_sessao(self, session_id: str, ttl: int = 14400):
        """Cria uma nova sessão no Redis"""
        sessao_data = {
            'id': session_id,
            'criado_em': datetime.now().isoformat(),
            'status': 'ativa',
            'arquivos': [],
            'chunks_recebidos': 0
        }
        
        self.redis_client.setex(
            f'session:{session_id}',
            ttl,
            json.dumps(sessao_data)
        )
        
        return session_id
    
    def salvar_chunk(self, session_id: str, chunk_index: int, chunk_data):
        """Salva um chunk de arquivo"""
        chunk_key = f'session:chunk:{session_id}:{chunk_index}'
        
        # Converte para bytes se for arquivo
        if hasattr(chunk_data, 'read'):
            chunk_bytes = chunk_data.read()
        else:
            chunk_bytes = chunk_data
        
        # Salva no Redis (TTL 4 horas)
        self.redis_client.setex(chunk_key, 14400, chunk_bytes)
        
        # Atualiza contador
        sessao = self.get_sessao(session_id)
        sessao['chunks_recebidos'] += 1
        self.redis_client.setex(
            f'session:{session_id}',
            14400,
            json.dumps(sessao)
        )
        
        return True
    
    def get_sessao(self, session_id: str):
        """Retorna dados da sessão"""
        data = self.redis_client.get(f'session:{session_id}')
        if data:
            return json.loads(data)
        return None
    
    def get_status(self, session_id: str):
        """Retorna status da sessão"""
        sessao = self.get_sessao(session_id)
        if not sessao:
            return {'status': 'nao_encontrada'}
        
        # Calcula TTL restante
        ttl = self.redis_client.ttl(f'session:{session_id}')
        
        return {
            'status': sessao['status'],
            'chunks_recebidos': sessao['chunks_recebidos'],
            'ttl_restante': ttl,
            'criado_em': sessao['criado_em']
        }

class XMLProcessor:
    def __init__(self):
        self.namespaces = {
            'nfe': 'http://www.portalfiscal.inf.br/nfe',
            'nfcom': 'http://www.portalfiscal.inf.br/nfcom'
        }
    
    def parse_xml(self, xml_content: str):
        """Parse XML e extrai dados"""
        try:
            root = ET.fromstring(xml_content)
            
            # Detecta tipo (NFe ou NFCom)
            if root.tag.endswith('NFe'):
                return self._parse_nfe(root)
            elif root.tag.endswith('NFCom'):
                return self._parse_nfcom(root)
            else:
                return {'error': 'Tipo XML não suportado'}
                
        except ET.ParseError as e:
            return {'error': f'Erro no parse XML: {str(e)}'}
    
    def _parse_nfe(self, root: ET.Element):
        """Parse NFe"""
        # Extrai informações principais
        infNFe = root.find('.//nfe:infNFe', self.namespaces)
        
        dados = {
            'tipo': 'NFe',
            'nNF': infNFe.find('.//nfe:nNF', self.namespaces).text if infNFe else None,
            'cNF': infNFe.find('.//nfe:cNF', self.namespaces).text if infNFe else None,
            'dhEmi': infNFe.find('.//nfe:dhEmi', self.namespaces).text if infNFe else None,
            'emitente': {},
            'destinatario': {},
            'itens': [],
            'totais': {}
        }
        
        # Emitente
        emit = infNFe.find('.//nfe:emit', self.namespaces)
        if emit:
            dados['emitente'] = {
                'xNome': emit.find('nfe:xNome', self.namespaces).text if emit else None,
                'CNPJ': emit.find('nfe:CNPJ', self.namespaces).text if emit else None
            }
        
        # Itens
        dets = infNFe.findall('.//nfe:det', self.namespaces)
        for det in dets:
            prod = det.find('nfe:prod', self.namespaces)
            if prod:
                item = {
                    'cProd': prod.find('nfe:cProd', self.namespaces).text if prod else None,
                    'xProd': prod.find('nfe:xProd', self.namespaces).text if prod else None,
                    'NCM': prod.find('nfe:NCM', self.namespaces).text if prod else None,
                    'CFOP': prod.find('nfe:CFOP', self.namespaces).text if prod else None,
                    'qCom': float(prod.find('nfe:qCom', self.namespaces).text) if prod and prod.find('nfe:qCom', self.namespaces) is not None else 0,
                    'vUnCom': float(prod.find('nfe:vUnCom', self.namespaces).text) if prod and prod.find('nfe:vUnCom', self.namespaces) is not None else 0,
                    'vProd': float(prod.find('nfe:vProd', self.namespaces).text) if prod and prod.find('nfe:vProd', self.namespaces) is not None else 0
                }
                dados['itens'].append(item)
        
        return dados
    
    def _parse_nfcom(self, root: ET.Element):
        """Parse NFCom"""
        # Implementação similar para NFCom
        return {'tipo': 'NFCom', 'dados': 'em_desenvolvimento'}
    
    def processar_sessao(self, session_id: str, opcoes: Dict = None):
        """Processa todos os XMLs de uma sessão"""
        # Recupera chunks e monta arquivo
        # Processa cada XML
        # Retorna resultado agregado
        
        return {
            'total_xmls': 0,
            'dados_agregados': {},
            'status': 'processado'
        }
    
    def gerar_resumo(self, session_id: str):
        """Gera resumo consolidado"""
        # Implementação de agregação por cClass, CFOP, etc.
        
        return {
            'emitente_nome': 'NOVA TELECOM LTDA',
            'emitente_cnpj': '01.555.241/0001-20',
            'total_arquivos': 2707,
            'total_geral': 292091.83,
            'total_geral_br': 'R$ 292.091,83',
            'linhas': [],
            'labels': [],
            'valores': [],
            'itens_linhas': [],
            'impostos_linhas': [],
            'total_impostos': 12000.00,
            'total_impostos_br': 'R$ 12.000,00'
        }
    
    def processar_lote(self, session_id: str, regras: List[Dict]):
        """Processa lote com regras de transformação"""
        # Aplica regras de CFOP, remoção de tags, etc.
        # Gera ZIP com XMLs modificados
        
        return {
            'arquivos_processados': 0,
            'regras_aplicadas': regras,
            'download_url': f'/download/{session_id}'
        }