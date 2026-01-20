from __future__ import annotations

import io

import os
import time
import uuid
import threading
import tempfile
from flask import jsonify, redirect, url_for
from flask import Flask, render_template, request, send_file, flash

from core import (
    carregar_cclass_lista,
    cclass_desc_map,
    parse_regras_texto,
    processar_lote_zip,
    processar_lote_zip_path,  # <<< LOTE por arquivo + progresso

    gerar_csv_de_zip,
    gerar_resumo_de_zip,
    gerar_resumo_de_zip_path,  # <<< RESUMO por arquivo + progresso

    gerar_dados_nota_xml,  # <<< NOTA
)

app = Flask(__name__)
app.secret_key = "troca-xml"

_CCLASS_LISTA = None
_DESC_MAP = None


# =========================
# Jobs em memória (resumo)
# =========================
_JOBS = {}
_JOBS_LOCK = threading.Lock()


def _job_set(job_id: str, **kwargs):
    with _JOBS_LOCK:
        _JOBS.setdefault(job_id, {})
        _JOBS[job_id].update(kwargs)


def _job_get(job_id: str):
    with _JOBS_LOCK:
        return dict(_JOBS.get(job_id, {}))


def _processar_resumo_job(job_id: str, zip_path: str, desc_map: dict):
    try:
        _job_set(job_id, status="running", processed=0, total=0, started_at=time.time())

        def on_prog(p, t):
            _job_set(job_id, processed=p, total=t)

        resumo_data = gerar_resumo_de_zip_path(zip_path, desc_map=desc_map, on_progress=on_prog)
        _job_set(job_id, status="done", result=resumo_data, finished_at=time.time())
    except Exception as e:
        _job_set(job_id, status="error", error=str(e), finished_at=time.time())
    finally:
        try:
            os.remove(zip_path)
        except Exception:
            pass


def _load_cclass():
    global _CCLASS_LISTA, _DESC_MAP
    if _CCLASS_LISTA is None:
        _CCLASS_LISTA = carregar_cclass_lista()
        _DESC_MAP = cclass_desc_map(_CCLASS_LISTA)
    return _CCLASS_LISTA, _DESC_MAP


@app.get("/")
def index():
    return render_template("index.html")


# =========================
# LOTE (assíncrono com progresso)
# =========================
@app.get("/lote")
def lote():
    cclass_lista, _ = _load_cclass()
    return render_template("lote.html", cclass_lista=cclass_lista)


@app.post("/lote/processar")
def lote_processar():
    cclass_lista, _ = _load_cclass()

    fzip = request.files.get("zip_xmls")
    if not fzip:
        flash("Envie um arquivo .zip")
        return render_template("lote.html", cclass_lista=cclass_lista)

    remover_desconto = bool(request.form.get("remover_desconto"))
    remover_outros = bool(request.form.get("remover_outros"))

    # FIX: o textarea no lote.html chama regras_cclass_cfop
    regras_txt = request.form.get("regras_cclass_cfop", "") or ""
    regras = parse_regras_texto(regras_txt)

    job_id = uuid.uuid4().hex[:12]
    tmp_dir = tempfile.gettempdir()
    zip_path = os.path.join(tmp_dir, f"nfcom_lote_{job_id}.zip")
    out_path = os.path.join(tmp_dir, f"nfcom_lote_out_{job_id}.zip")
    fzip.save(zip_path)

    _job_set(job_id, status="queued", processed=0, total=0, kind="lote")

    th = threading.Thread(
        target=_processar_lote_job,
        args=(job_id, zip_path, out_path, regras, remover_desconto, remover_outros),
        daemon=True,
    )
    th.start()

    return render_template("lote_loading.html", job_id=job_id)


@app.get("/lote/status/<job_id>")
def lote_status(job_id: str):
    j = _job_get(job_id)
    if not j:
        return jsonify({"ok": False, "status": "not_found"}), 404

    processed = int(j.get("processed", 0) or 0)
    total = int(j.get("total", 0) or 0)
    status = j.get("status", "queued")

    pct = 0
    if total > 0:
        pct = int((processed / total) * 100)

    return jsonify({
        "ok": True,
        "status": status,
        "processed": processed,
        "total": total,
        "pct": pct,
        "error": j.get("error", ""),
        "done": status == "done",
    })


@app.get("/lote/baixar/<job_id>")
def lote_baixar(job_id: str):
    j = _job_get(job_id)
    if not j:
        flash("Job não encontrado ou expirou.")
        return redirect(url_for("lote"))

    if j.get("status") == "error":
        flash(f"Erro ao processar: {j.get('error', 'desconhecido')}")
        return redirect(url_for("lote"))

    if j.get("status") != "done":
        return render_template("lote_loading.html", job_id=job_id)

    out_path = j.get("out_path")
    if not out_path:
        flash("Arquivo de saída não encontrado.")
        return redirect(url_for("lote"))

    return send_file(
        out_path,
        as_attachment=True,
        download_name="resultado.zip",
        mimetype="application/zip",
    )


# =========================
# NOTA ÚNICA (VISUALIZAÇÃO)
# =========================
@app.get("/nota")
def nota():
    # Página com upload do XML (template nota.html)
    return render_template("nota.html")


@app.post("/nota/visualizar")
def nota_visualizar():
    fxml = request.files.get("xml_nota")
    if not fxml:
        flash("Envie um XML.")
        return render_template("nota.html")

    try:
        xml_bytes = fxml.read()
        d = gerar_dados_nota_xml(xml_bytes)
        # Renderiza exatamente o seu template de DANFECom/resultado
        return render_template("resultado.html", d=d)
    except Exception as e:
        flash(f"Erro ao processar XML: {e}")
        return render_template("nota.html")


# =========================
# CSV
# =========================
@app.get("/csv")
def csv_page():
    return render_template("csv.html")


@app.post("/csv/gerar")
def csv_gerar():
    fzip = request.files.get("zip_xmls")
    if not fzip:
        flash("Envie um arquivo .zip")
        return render_template("csv.html")

    mapping_txt = request.form.get("mapping_txt", "")
    mapping = []
    for l in (mapping_txt or "").splitlines():
        l = l.strip()
        if not l or l.startswith("#"):
            continue
        if ";" in l:
            a, b = [x.strip() for x in l.split(";", 1)]
            if a and b:
                mapping.append((a, b))

    if not mapping:
        flash("Informe o mapeamento no formato CABEÇALHO;TAG")
        return render_template("csv.html")

    out = gerar_csv_de_zip(fzip.read(), mapping)

    return send_file(
        io.BytesIO(out),
        as_attachment=True,
        download_name="relatorio.csv",
        mimetype="text/csv",
    )




def _processar_lote_job(job_id: str, zip_path: str, out_path: str, regras: dict, remover_desc: bool, remover_outros: bool):
    try:
        _job_set(job_id, status="running", processed=0, total=0, started_at=time.time())
        def on_prog(p, t):
            _job_set(job_id, processed=p, total=t)

        processar_lote_zip_path(
            zip_path,
            out_path,
            regras=regras,
            remover_desc=remover_desc,
            remover_outros=remover_outros,
            on_progress=on_prog,
        )

        _job_set(job_id, status="done", out_path=out_path, finished_at=time.time())
    except Exception as e:
        _job_set(job_id, status="error", error=str(e), finished_at=time.time())
        # tenta limpar saída parcial
        try:
            import os
            if out_path and os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
    finally:
        # limpa zip de entrada
        try:
            import os
            if zip_path and os.path.exists(zip_path):
                os.remove(zip_path)
        except Exception:
            pass


# =========================
# RESUMO
# =========================
@app.get("/resumo")
def resumo():
    return render_template("resumo.html", resumo=None)


@app.post("/resumo/gerar")
def resumo_gerar():
    _, desc_map = _load_cclass()

    fzip = request.files.get("zip_xmls")
    if not fzip:
        flash("Envie um arquivo .zip")
        return render_template("resumo.html", resumo=None)

    # salva zip no disco (evita RAM) e processa em background
    job_id = uuid.uuid4().hex[:12]
    tmp_dir = tempfile.gettempdir()
    zip_path = os.path.join(tmp_dir, f"nfcom_{job_id}.zip")
    fzip.save(zip_path)

    _job_set(job_id, status="queued", processed=0, total=0)

    th = threading.Thread(target=_processar_resumo_job, args=(job_id, zip_path, desc_map), daemon=True)
    th.start()

    return render_template("resumo_loading.html", job_id=job_id)


@app.get("/resumo/status/<job_id>")
def resumo_status(job_id: str):
    j = _job_get(job_id)
    if not j:
        return jsonify({"ok": False, "status": "not_found"}), 404

    processed = int(j.get("processed", 0) or 0)
    total = int(j.get("total", 0) or 0)
    status = j.get("status", "queued")

    pct = 0
    if total > 0:
        pct = int((processed / total) * 100)

    return jsonify({
        "ok": True,
        "status": status,
        "processed": processed,
        "total": total,
        "pct": pct,
        "error": j.get("error", ""),
        "done": status == "done",
    })


@app.get("/resumo/resultado/<job_id>")
def resumo_resultado(job_id: str):
    j = _job_get(job_id)
    if not j:
        flash("Job não encontrado ou expirou.")
        return redirect(url_for("resumo"))

    if j.get("status") == "error":
        flash(f"Erro ao processar: {j.get('error', 'desconhecido')}")
        return redirect(url_for("resumo"))

    if j.get("status") != "done":
        return render_template("resumo_loading.html", job_id=job_id)

    return render_template("resumo.html", resumo=j.get("result"))


if __name__ == "__main__":
    app.run(debug=True)