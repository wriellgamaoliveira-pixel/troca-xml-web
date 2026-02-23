/* Resumo Resultado (vanilla JS)
   - Filtros + ordenação
   - Expandir/Recolher todos
   - Tabelas com sublinhas
*/
function moneyBR(v){
  try{
    return new Intl.NumberFormat("pt-BR",{style:"currency",currency:"BRL"}).format(Number(v||0));
  }catch(e){ return "R$ 0,00"; }
}

function byValorDesc(a,b){ return (b.v_total||0)-(a.v_total||0); }
function byValorAsc(a,b){ return (a.v_total||0)-(b.v_total||0); }
function byAz(a,b){
  const A = String(a.cClass||a.tipo||a.item||"").toLowerCase();
  const B = String(b.cClass||b.tipo||b.item||"").toLowerCase();
  return A.localeCompare(B, "pt-BR");
}

function contains(a, q){
  return String(a||"").toLowerCase().includes(String(q||"").toLowerCase());
}

function el(tag, attrs={}, children=[]){
  const n = document.createElement(tag);
  for(const [k,v] of Object.entries(attrs)){
    if(k==="class") n.className = v;
    else if(k==="html") n.innerHTML = v;
    else if(k.startsWith("on") && typeof v === "function") n.addEventListener(k.slice(2).toLowerCase(), v);
    else n.setAttribute(k, v);
  }
  for(const c of children){
    if(c===null || c===undefined) continue;
    n.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
  }
  return n;
}

function icon(name){
  const i = document.createElement("i");
  i.setAttribute("data-lucide", name);
  i.style.width = "18px";
  i.style.height = "18px";
  return i;
}

function renderResumo(DATA){
  // ---------------- Chart ----------------
  const ctx = document.getElementById("pieChart");
  if(ctx && window.Chart){
    const chartData = (DATA.labels||[]).map((label, idx) => ({label, value: (DATA.valores||[])[idx] || 0}));
    const labels = chartData.map(x=>x.label);
    const values = chartData.map(x=>x.value);

    new Chart(ctx, {
      type: "pie",
      data: { labels, datasets: [{ data: values }] },
      options: {
        responsive: true,
        plugins: {
          tooltip: {
            callbacks: {
              label: (c) => `${c.label}: ${moneyBR(c.raw)}`
            }
          },
          legend: { position: "bottom" }
        }
      }
    });
  }

  // ---------------- cClass Table ----------------
  const cclassBody = document.getElementById("cclassBody");
  const cclassFilter = document.getElementById("cclassFilter");
  const cclassSort = document.getElementById("cclassSort");
  const cclassExpand = document.getElementById("cclassExpand");
  const cclassCollapse = document.getElementById("cclassCollapse");

  let expandedC = new Set();
  let expandedCfop = new Set();

  function cclassKey(i){ return `c:${i}`; }
  function cfopKey(i, cfop){ return `cf:${i}:${cfop}`; }

  function expandAllC(){
    expandedC = new Set((DATA.linhas||[]).map((_,i)=>cclassKey(i)));
    redrawC();
  }
  function collapseAllC(){
    expandedC = new Set();
    expandedCfop = new Set();
    redrawC();
  }

  function filteredSortedLinhas(){
    const q = (cclassFilter?.value || "").trim().toLowerCase();
    const sort = (cclassSort?.value || "desc");
    let arr = (DATA.linhas||[]).filter(l =>
      !q ||
      contains(l.cClass, q) ||
      contains(l.desc, q)
    );
    if(sort==="desc") arr.sort(byValorDesc);
    else if(sort==="asc-valor") arr.sort(byValorAsc);
    else arr.sort(byAz);
    return arr;
  }

  function redrawC(){
    if(!cclassBody) return;
    cclassBody.innerHTML = "";

    const arr = filteredSortedLinhas();
    if(arr.length===0){
      cclassBody.appendChild(el("tr",{},[
        el("td",{colspan:"15", class:"center", html:"<div style='padding:22px;color:var(--muted)'>Nenhum resultado encontrado para o filtro.</div>"})
      ]));
      return;
    }

    arr.forEach((linha, idx) => {
      // idx refer to filtered index; use stable key based on original? keep filtered index for toggles.
      const key = cclassKey(idx);
      const isOpen = expandedC.has(key);

      const btn = el("button",{class:"chev-btn", onClick:(ev)=>{ ev.stopPropagation(); 
        if(expandedC.has(key)) expandedC.delete(key); else expandedC.add(key);
        redrawC();
      }},[ icon(isOpen ? "chevron-down" : "chevron-right") ]);

      const row = el("tr",{},[
        el("td",{class:"center"},[btn]),
        el("td",{},[String(linha.cClass||"")]),
        el("td",{},[String(linha.desc||"")]),
        el("td",{class:"right"},[String(linha.qtd_itens ?? "")]),
        el("td",{class:"right"},[String(linha.v_total_br || moneyBR(linha.v_total))]),
        el("td",{class:"right"},[String(linha.total_icms_br || moneyBR(linha.total_icms))]),
        el("td",{class:"right"},[String(linha.total_pis_br || moneyBR(linha.total_pis))]),
        el("td",{class:"right"},[String(linha.total_cofins_br || moneyBR(linha.total_cofins))]),
        el("td",{class:"right"},[String(linha.total_fust_br || moneyBR(linha.total_fust))]),
        el("td",{class:"right"},[String(linha.total_funttel_br || moneyBR(linha.total_funttel))]),
        el("td",{class:"right"},[String(linha.total_ibs_br || moneyBR(linha.total_ibs))]),
        el("td",{class:"right"},[String(linha.total_cbs_br || moneyBR(linha.total_cbs))]),
        el("td",{class:"right"},[String(linha.total_desc_br || moneyBR(linha.total_desc))]),
        el("td",{class:"right"},[String(linha.total_outro_br || moneyBR(linha.total_outro))]),
        el("td",{class:"right"},[String(linha.pct_br || "")]),
      ]);
      cclassBody.appendChild(row);

      if(isOpen){
        // subrow with CFOPs
        const cfops = linha.cfops || [];
        const wrap = el("div",{class:"subcard"},[
          el("div",{class:"subtitle"},["CFOPs desta cClass"]),
        ]);

        if(cfops.length===0){
          wrap.appendChild(el("div",{class:"pill"},["Sem CFOPs detalhados"]));
        }else{
          const t = el("table",{},[]);
          t.style.minWidth = "620px";
          t.appendChild(el("thead",{},[
            el("tr",{},[
              el("th",{class:"center", style:"width:44px"},[""]),
              el("th",{},["CFOP"]),
              el("th",{class:"right"},["Valor"]),
              el("th",{class:"right"},["ICMS"]),
              el("th",{class:"right"},["PIS"]),
              el("th",{class:"right"},["COFINS"]),
              el("th",{class:"right"},["FUST"]),
              el("th",{class:"right"},["FUNTTEL"]),
              el("th",{class:"right"},["IBS"]),
              el("th",{class:"right"},["CBS"]),
              el("th",{class:"right"},["Desconto"]),
              el("th",{class:"right"},["Outras"]),
            ])
          ]));
          const tb = el("tbody",{},[]);
          cfops.forEach((cfo) => {
            const cfKey = cfopKey(idx, cfo.cfop);
            const cfOpen = expandedCfop.has(cfKey);
            const cfBtn = el("button",{class:"chev-btn", onClick:(ev)=>{ ev.stopPropagation();
              if(expandedCfop.has(cfKey)) expandedCfop.delete(cfKey); else expandedCfop.add(cfKey);
              redrawC();
            }},[ icon(cfOpen ? "chevron-down":"chevron-right") ]);

            tb.appendChild(el("tr",{},[
              el("td",{class:"center"},[cfBtn]),
              el("td",{},[String(cfo.cfop||"")]),
              el("td",{class:"right"},[String(cfo.v_total_br||"")]),
              el("td",{class:"right"},[String(cfo.total_icms_br || moneyBR(cfo.total_icms))]),
              el("td",{class:"right"},[String(cfo.total_pis_br || moneyBR(cfo.total_pis))]),
              el("td",{class:"right"},[String(cfo.total_cofins_br || moneyBR(cfo.total_cofins))]),
              el("td",{class:"right"},[String(cfo.total_fust_br || moneyBR(cfo.total_fust))]),
              el("td",{class:"right"},[String(cfo.total_funttel_br || moneyBR(cfo.total_funttel))]),
              el("td",{class:"right"},[String(cfo.total_ibs_br || moneyBR(cfo.total_ibs))]),
              el("td",{class:"right"},[String(cfo.total_cbs_br || moneyBR(cfo.total_cbs))]),
              el("td",{class:"right"},[String(cfo.total_desc_br || moneyBR(cfo.total_desc))]),
              el("td",{class:"right"},[String(cfo.total_outro_br || moneyBR(cfo.total_outro))]),
            ]));

            if(cfOpen){
              const notas = cfo.notas || [];
              const notasWrap = el("div",{class:"subcard"},[
                el("div",{class:"subtitle"},["Notas fiscais relacionadas"])
              ]);
              const nt = el("table",{},[]);
              nt.style.minWidth = "980px";
              nt.appendChild(el("thead",{},[
                el("tr",{},[
                  el("th",{},["nNF"]),
                  el("th",{},["Contrato (cNF)"]),
                  el("th",{},["Emitente"]),
                  el("th",{},["Destinatário"]),
                  el("th",{},["Emissão"]),
                  el("th",{class:"right"},["Valor do item"]),
                  el("th",{class:"right"},["ICMS"]),
                  el("th",{class:"right"},["PIS"]),
                  el("th",{class:"right"},["COFINS"]),
                  el("th",{class:"right"},["FUST"]),
                  el("th",{class:"right"},["FUNTTEL"]),
                  el("th",{class:"right"},["IBS"]),
                  el("th",{class:"right"},["CBS"]),
                  el("th",{class:"right"},["Desconto"]),
                  el("th",{class:"right"},["Outras"]),
                ])
              ]));
              const ntb = el("tbody",{},[]);
              if(notas.length===0){
                ntb.appendChild(el("tr",{},[
                  el("td",{colspan:"15", class:"center", html:"<div style='padding:14px;color:var(--muted)'>Sem notas</div>"})
                ]));
              }else{
                notas.forEach(n => {
                  ntb.appendChild(el("tr",{},[
                    el("td",{},[String(n.nNF||"")]),
                    el("td",{},[String(n.cNF||"")]),
                    el("td",{},[String(n.xNome||"")]),
                    el("td",{},[String(n.xContato||"")]),
                    el("td",{},[String(n.dhEmi_fmt||"")]),
                    el("td",{class:"right"},[String(n.valor_br||n.vProd_br||"")]),
                    el("td",{class:"right"},[String(n.icms_br||moneyBR(n.icms))]),
                    el("td",{class:"right"},[String(n.pis_br||moneyBR(n.pis))]),
                    el("td",{class:"right"},[String(n.cofins_br||moneyBR(n.cofins))]),
                    el("td",{class:"right"},[String(n.fust_br||moneyBR(n.fust))]),
                    el("td",{class:"right"},[String(n.funttel_br||moneyBR(n.funttel))]),
                    el("td",{class:"right"},[String(n.ibs_br||moneyBR(n.ibs))]),
                    el("td",{class:"right"},[String(n.cbs_br||moneyBR(n.cbs))]),
                    el("td",{class:"right"},[String(n.vDesc_br||moneyBR(n.vDesc))]),
                    el("td",{class:"right"},[String(n.vOutro_br||moneyBR(n.vOutro))]),
                  ]));
                });
              }
              nt.appendChild(ntb);
              notasWrap.appendChild(el("div",{class:"table-wrap"},[nt]));
              tb.appendChild(el("tr",{},[
                el("td",{colspan:"12", class:"subrow"},[notasWrap])
              ]));
            }
          });

          t.appendChild(tb);
          wrap.appendChild(el("div",{class:"table-wrap"},[t]));
        }

        cclassBody.appendChild(el("tr",{},[
          el("td",{colspan:"15", class:"subrow"},[wrap])
        ]));
      }
    });

    if (window.lucide && typeof window.lucide.createIcons === "function") window.lucide.createIcons();
  }

  cclassFilter?.addEventListener("input", redrawC);
  cclassSort?.addEventListener("change", redrawC);
  cclassExpand?.addEventListener("click", expandAllC);
  cclassCollapse?.addEventListener("click", collapseAllC);

  // ---------------- Items Table ----------------
  const itemsBody = document.getElementById("itemsBody");
  const itemsFilter = document.getElementById("itemsFilter");
  const itemsSort = document.getElementById("itemsSort");
  const itemsExpand = document.getElementById("itemsExpand");
  const itemsCollapse = document.getElementById("itemsCollapse");

  let expandedI = new Set();
  function itemKey(i){ return `i:${i}`; }

  function filteredSortedItems(){
    const q = (itemsFilter?.value || "").trim().toLowerCase();
    const sort = (itemsSort?.value || "desc");
    let arr = (DATA.itens_linhas||[]).filter(it =>
      !q ||
      contains(it.item, q) ||
      contains(it.desc, q) ||
      contains(it.cClass, q)
    );
    if(sort==="desc") arr.sort(byValorDesc);
    else if(sort==="asc-valor") arr.sort(byValorAsc);
    else {
      arr.sort((a,b)=>String((a.desc||a.item)||"").localeCompare(String((b.desc||b.item)||""), "pt-BR"));
    }
    return arr;
  }

  function expandAllItems(){
    expandedI = new Set((DATA.itens_linhas||[]).map((_,i)=>itemKey(i)));
    redrawItems();
  }
  function collapseAllItems(){
    expandedI = new Set();
    redrawItems();
  }

  function redrawItems(){
    if(!itemsBody) return;
    itemsBody.innerHTML = "";
    const arr = filteredSortedItems();
    if(arr.length===0){
      itemsBody.appendChild(el("tr",{},[
        el("td",{colspan:"16", class:"center", html:"<div style='padding:22px;color:var(--muted)'>Nenhum resultado encontrado para o filtro.</div>"})
      ]));
      return;
    }

    arr.forEach((item, idx) => {
      const key = itemKey(idx);
      const open = expandedI.has(key);
      const btn = el("button",{class:"chev-btn", onClick:(ev)=>{ ev.stopPropagation();
        if(expandedI.has(key)) expandedI.delete(key); else expandedI.add(key);
        redrawItems();
      }},[ icon(open ? "chevron-down":"chevron-right") ]);

      itemsBody.appendChild(el("tr",{},[
        el("td",{class:"center"},[btn]),
        el("td",{},[String(item.item||"")]),
        el("td",{},[String(item.desc||"")]),
        el("td",{},[String(item.cClass||"")]),
        el("td",{class:"right"},[String(item.qtd_itens ?? "")]),
        el("td",{class:"right"},[String(item.v_total_br || moneyBR(item.v_total))]),
        el("td",{class:"right"},[String(item.total_icms_br || moneyBR(item.total_icms))]),
        el("td",{class:"right"},[String(item.total_pis_br || moneyBR(item.total_pis))]),
        el("td",{class:"right"},[String(item.total_cofins_br || moneyBR(item.total_cofins))]),
        el("td",{class:"right"},[String(item.total_fust_br || moneyBR(item.total_fust))]),
        el("td",{class:"right"},[String(item.total_funttel_br || moneyBR(item.total_funttel))]),
        el("td",{class:"right"},[String(item.total_ibs_br || moneyBR(item.total_ibs))]),
        el("td",{class:"right"},[String(item.total_cbs_br || moneyBR(item.total_cbs))]),
        el("td",{class:"right"},[String(item.total_desc_br || moneyBR(item.total_desc))]),
        el("td",{class:"right"},[String(item.total_outro_br || moneyBR(item.total_outro))]),
        el("td",{class:"right"},[String(item.pct_br || "")]),
      ]));

      if(open){
        const notas = item.notas || [];
        const wrap = el("div",{class:"subcard"},[
          el("div",{class:"subtitle"},["Notas fiscais relacionadas"])
        ]);
        const t = el("table",{},[]);
        t.style.minWidth = "980px";
        t.appendChild(el("thead",{},[
          el("tr",{},[
            el("th",{},["nNF"]),
            el("th",{},["Contrato (cNF)"]),
            el("th",{},["Emitente"]),
            el("th",{},["Destinatário"]),
            el("th",{},["Emissão"]),
            el("th",{class:"right"},["Valor do item"]),
            el("th",{class:"right"},["ICMS"]),
            el("th",{class:"right"},["PIS"]),
            el("th",{class:"right"},["COFINS"]),
            el("th",{class:"right"},["FUST"]),
            el("th",{class:"right"},["FUNTTEL"]),
            el("th",{class:"right"},["IBS"]),
            el("th",{class:"right"},["CBS"]),
            el("th",{class:"right"},["Desconto"]),
            el("th",{class:"right"},["Outras"]),
          ])
        ]));
        const tb = el("tbody",{},[]);
        if(notas.length===0){
          tb.appendChild(el("tr",{},[
            el("td",{colspan:"15", class:"center", html:"<div style='padding:14px;color:var(--muted)'>Sem notas</div>"})
          ]));
        }else{
          notas.forEach(n => {
            tb.appendChild(el("tr",{},[
              el("td",{},[String(n.nNF||"")]),
              el("td",{},[String(n.cNF||"")]),
              el("td",{},[String(n.xNome||"")]),
              el("td",{},[String(n.xContato||"")]),
              el("td",{},[String(n.dhEmi_fmt||"")]),
              el("td",{class:"right"},[String(n.valor_br||n.vProd_br||"")]),
              el("td",{class:"right"},[String(n.icms_br||moneyBR(n.icms))]),
              el("td",{class:"right"},[String(n.pis_br||moneyBR(n.pis))]),
              el("td",{class:"right"},[String(n.cofins_br||moneyBR(n.cofins))]),
              el("td",{class:"right"},[String(n.fust_br||moneyBR(n.fust))]),
              el("td",{class:"right"},[String(n.funttel_br||moneyBR(n.funttel))]),
              el("td",{class:"right"},[String(n.ibs_br||moneyBR(n.ibs))]),
              el("td",{class:"right"},[String(n.cbs_br||moneyBR(n.cbs))]),
              el("td",{class:"right"},[String(n.vDesc_br||moneyBR(n.vDesc))]),
              el("td",{class:"right"},[String(n.vOutro_br||moneyBR(n.vOutro))]),
            ]));
          });
        }
        t.appendChild(tb);
        wrap.appendChild(el("div",{class:"table-wrap"},[t]));

        itemsBody.appendChild(el("tr",{},[
          el("td",{colspan:"16", class:"subrow"},[wrap])
        ]));
      }
    });

    if (window.lucide && typeof window.lucide.createIcons === "function") window.lucide.createIcons();
  }

  itemsFilter?.addEventListener("input", redrawItems);
  itemsSort?.addEventListener("change", redrawItems);
  itemsExpand?.addEventListener("click", expandAllItems);
  itemsCollapse?.addEventListener("click", collapseAllItems);

  // ---------------- Impostos Table ----------------
  const impBody = document.getElementById("impBody");
  const impFilter = document.getElementById("impFilter");
  const impSort = document.getElementById("impSort");
  const impExpand = document.getElementById("impExpand");
  const impCollapse = document.getElementById("impCollapse");

  let expandedImp = new Set();
  function impKey(i){ return `t:${i}`; }

  function filteredSortedImpostos(){
    const q = (impFilter?.value || "").trim().toLowerCase();
    const sort = (impSort?.value || "desc");
    let arr = (DATA.impostos_linhas||[]).filter(it => !q || contains(it.tipo, q));
    if(sort==="desc") arr.sort(byValorDesc);
    else arr.sort((a,b)=>String(a.tipo||"").localeCompare(String(b.tipo||""), "pt-BR"));
    return arr;
  }

  function expandAllImp(){
    expandedImp = new Set((DATA.impostos_linhas||[]).map((_,i)=>impKey(i)));
    redrawImp();
  }
  function collapseAllImp(){
    expandedImp = new Set();
    redrawImp();
  }

  function redrawImp(){
    if(!impBody) return;
    impBody.innerHTML = "";

    const arr = filteredSortedImpostos();
    if(arr.length===0){
      impBody.appendChild(el("tr",{},[
        el("td",{colspan:"5", class:"center", html:"<div style='padding:22px;color:var(--muted)'>Nenhum resultado encontrado para o filtro.</div>"})
      ]));
      return;
    }

    arr.forEach((imp, idx) => {
      const key = impKey(idx);
      const open = expandedImp.has(key);
      const btn = el("button",{class:"chev-btn", onClick:(ev)=>{ ev.stopPropagation();
        if(expandedImp.has(key)) expandedImp.delete(key); else expandedImp.add(key);
        redrawImp();
      }},[ icon(open ? "chevron-down":"chevron-right") ]);

      impBody.appendChild(el("tr",{},[
        el("td",{class:"center"},[btn]),
        el("td",{},[String(imp.tipo||"")]),
        el("td",{class:"right"},[String(imp.qtd_notas ?? "")]),
        el("td",{class:"right"},[String(imp.v_total_br || moneyBR(imp.v_total))]),
        el("td",{class:"right"},[String(imp.pct_br || "")]),
      ]));

      if(open){
        const notas = imp.notas || [];
        const wrap = el("div",{class:"subcard"},[
          el("div",{class:"subtitle"},["Notas fiscais relacionadas"])
        ]);
        const t = el("table",{},[]);
        t.style.minWidth = "1100px";
        t.appendChild(el("thead",{},[
          el("tr",{},[
            el("th",{},["nNF"]),
            el("th",{},["Contrato (cNF)"]),
            el("th",{},["Emitente"]),
            el("th",{},["Destinatário"]),
            el("th",{},["Emissão"]),
            el("th",{class:"right"},["PIS Ret."]),
            el("th",{class:"right"},["COFINS Ret."]),
            el("th",{class:"right"},["CSLL Ret."]),
            el("th",{class:"right"},["IRRF Ret."]),
            el("th",{class:"right"},["Total Retido"]),
          ])
        ]));
        const tb = el("tbody",{},[]);
        if(notas.length===0){
          tb.appendChild(el("tr",{},[
            el("td",{colspan:"10", class:"center", html:"<div style='padding:14px;color:var(--muted)'>Sem notas</div>"})
          ]));
        }else{
          notas.forEach(n => {
            tb.appendChild(el("tr",{},[
              el("td",{},[String(n.nNF||"")]),
              el("td",{},[String(n.cNF||"")]),
              el("td",{},[String(n.emitente||"")]),
              el("td",{},[String(n.destinatario||"")]),
              el("td",{},[String(n.emissao||"")]),
              el("td",{class:"right"},[String(n.pis_ret||"")]),
              el("td",{class:"right"},[String(n.cofins_ret||"")]),
              el("td",{class:"right"},[String(n.csll_ret||"")]),
              el("td",{class:"right"},[String(n.irrf_ret||"")]),
              el("td",{class:"right"},[String(n.total_retido||"")]),
            ]));
          });
        }
        t.appendChild(tb);
        wrap.appendChild(el("div",{class:"table-wrap"},[t]));

        impBody.appendChild(el("tr",{},[
          el("td",{colspan:"5", class:"subrow"},[wrap])
        ]));
      }
    });

    if (window.lucide && typeof window.lucide.createIcons === "function") window.lucide.createIcons();
  }

  impFilter?.addEventListener("input", redrawImp);
  impSort?.addEventListener("change", redrawImp);
  impExpand?.addEventListener("click", expandAllImp);
  impCollapse?.addEventListener("click", collapseAllImp);

  // render initial
  redrawC();
  redrawItems();
  redrawImp();
}

document.addEventListener("DOMContentLoaded", () => {
  if (typeof window.__RESUMO_DATA__ !== "undefined") {
    renderResumo(window.__RESUMO_DATA__);
  }
});
