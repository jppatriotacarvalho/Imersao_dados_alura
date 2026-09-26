/* VoeBem — navegação e gráficos do site.
   Todos os números vêm de dados/painel.json (ou da cópia embutida no index.html). */
(function () {
  'use strict';

  // ---------- Cores (as mesmas do estilo.css) ----------
  const COR = {
    serie: '#2b9fdc',
    cinza: '#5d6778',
    alerta: '#ec835a',
    texto: '#e3e7f0',
    texto2: '#aeb7c4',
    muted: '#7d8796',
    grade: '#232a37',
    eixo: '#343c4b',
    painel: '#151a24',
    painel2: '#1b2130',
    linha: '#262d3b',
  };

  // Nome curto das companhias que aparecem nos gráficos (o nome completo vem do dado).
  const NOME_CURTO = {
    TAM: 'LATAM (TAM)', AZU: 'Azul', GLO: 'GOL', ACN: 'Azul Conecta', LAN: 'LATAM Airlines Group',
    TAP: 'TAP', CMP: 'Copa', ARG: 'Aerolíneas Argentinas', SKU: 'Sky Airline', AVA: 'Avianca',
    IBE: 'Iberia', QTR: 'Qatar Airways', ETH: 'Ethiopian', DWI: 'Arajet', ITY: 'ITA Airways',
    DLH: 'Lufthansa', AAL: 'American Airlines',
  };

  const MESES = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
  const DIAS = { 1: 'Dom', 2: 'Seg', 3: 'Ter', 4: 'Qua', 5: 'Qui', 6: 'Sex', 7: 'Sáb' };

  // ---------- Formatação ----------
  const fmtInt = (n) => Number(n).toLocaleString('pt-BR');
  const fmtDec = (n, d = 1) =>
    Number(n).toLocaleString('pt-BR', { minimumFractionDigits: d, maximumFractionDigits: d });
  const fmtPct = (n, d = 1) => fmtDec(n, d) + '%';
  const rotuloMes = (m) => MESES[Number(m.slice(5, 7)) - 1] + '/' + m.slice(2, 4);
  const rotuloDia = (d) => d.slice(8, 10) + '/' + d.slice(5, 7) + '/' + d.slice(0, 4);
  const nomeCia = (icao, nome) => NOME_CURTO[icao] || nome || icao;
  const el = (id) => document.getElementById(id);

  // ---------- Dados ----------
  async function carregarDados() {
    try {
      const r = await fetch('dados/painel.json', { cache: 'no-cache' });
      if (r.ok) return await r.json();
    } catch (e) { /* aberto com duplo clique: usa a cópia embutida */ }
    return JSON.parse(el('painel-embedded').textContent);
  }

  function calcular(d) {
    const meses = d.serie_mensal.slice(0, 12); // o JSON traz um "2026-08" com 17 voos; o período é ago/25 a jul/26
    const somaVoos = meses.reduce((s, m) => s + m.voos, 0);
    const cancReal = meses.reduce((s, m) => s + m.voos * parseFloat(m.cancelamento_pct), 0) / somaVoos;
    const cancBrutos = d.dias_atipicos.reduce((s, x) => s + x.cancelados, 0);
    const cancFantasma = d.voos_fantasma.reduce((s, x) => s + x.cancelados, 0);
    const r = d.resumo[0];
    return {
      meses,
      num: {
        registros: fmtInt(r.registros),
        elegiveis: fmtInt(r.voos_elegiveis),
        pont_partida: fmtPct(parseFloat(r.pontualidade_partida_pct), 2),
        pont_chegada: fmtPct(parseFloat(r.pontualidade_chegada_pct), 2),
        canc_real: fmtPct(cancReal, 2),
        pct_fantasma: fmtPct((100 * cancFantasma) / cancBrutos, 1),
        pct_fora_denominador: fmtPct((100 * (r.registros - (r.voos_elegiveis - r.linhas_fantasma))) / r.registros, 1),
        pares_fantasma: fmtInt(d.voos_fantasma.length),
        linhas_fantasma: fmtInt(r.linhas_fantasma),
        horarios_suspeitos: fmtInt(r.horarios_suspeitos),
        n_atipicos: fmtInt(d.dias_atipicos.filter((x) => x.dia_atipico).length),
        duplicatas: fmtInt(r.duplicatas_de_origem),
      },
    };
  }

  function preencherNumeros(num) {
    document.querySelectorAll('[data-num]').forEach((n) => {
      const v = num[n.getAttribute('data-num')];
      if (v !== undefined) n.textContent = v;
    });
  }

  // ---------- Tabelas ----------
  function montarTabela(cabecalho, linhas, numericas) {
    const t = document.createElement('table');
    const th = t.createTHead().insertRow();
    cabecalho.forEach((c, i) => {
      const x = document.createElement('th');
      x.textContent = c;
      if (numericas.includes(i)) x.className = 'num';
      th.appendChild(x);
    });
    const tb = t.createTBody();
    linhas.forEach((l) => {
      const tr = tb.insertRow();
      l.forEach((v, i) => {
        const td = tr.insertCell();
        td.textContent = v;
        if (numericas.includes(i)) td.className = 'num';
      });
    });
    return t;
  }
  function colocarTabela(chave, cabecalho, linhas, numericas) {
    document.querySelectorAll('details[data-tabela="' + chave + '"] .rolagem').forEach((c) => {
      c.textContent = '';
      c.appendChild(montarTabela(cabecalho, linhas, numericas));
    });
  }

  // ---------- Chart.js: padrões ----------
  function configurarChart() {
    Chart.defaults.font.family = 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif';
    Chart.defaults.font.size = 12;
    Chart.defaults.color = COR.muted;
    Chart.defaults.borderColor = COR.grade;
    Chart.defaults.maintainAspectRatio = false;
    Chart.defaults.animation.duration = 400;
    Object.assign(Chart.defaults.plugins.tooltip, {
      backgroundColor: COR.painel2,
      borderColor: COR.linha,
      borderWidth: 1,
      titleColor: COR.texto2,
      titleFont: { weight: '500' },
      bodyColor: COR.texto,
      bodyFont: { weight: '600', size: 13 },
      footerColor: COR.muted,
      footerFont: { weight: '400' },
      padding: 10,
      cornerRadius: 8,
      boxWidth: 10,
      boxHeight: 2,
      boxPadding: 6,
    });
    Chart.defaults.plugins.legend.display = false;

    // Linha vertical que acompanha o mouse nos gráficos de linha.
    Chart.register({
      id: 'miraVertical',
      afterDatasetsDraw(chart) {
        if (!chart.options.plugins.miraVertical) return;
        const ativos = chart.tooltip && chart.tooltip.getActiveElements();
        if (!ativos || !ativos.length) return;
        const x = ativos[0].element.x;
        const { top, bottom } = chart.chartArea;
        const ctx = chart.ctx;
        ctx.save();
        ctx.strokeStyle = COR.eixo;
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(x, top); ctx.lineTo(x, bottom); ctx.stroke();
        ctx.restore();
      },
    });

    // Linhas horizontais de referência (mediana, corte).
    Chart.register({
      id: 'linhasRef',
      afterDatasetsDraw(chart, _a, opts) {
        if (!opts || !opts.linhas) return;
        const { left, right } = chart.chartArea;
        const ctx = chart.ctx;
        opts.linhas.forEach((l) => {
          const y = chart.scales.y.getPixelForValue(l.valor);
          ctx.save();
          ctx.strokeStyle = l.cor;
          ctx.lineWidth = 1;
          ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(right, y); ctx.stroke();
          ctx.fillStyle = COR.texto2;
          ctx.font = '12px system-ui, sans-serif';
          ctx.textAlign = l.lado === 'esquerda' ? 'left' : 'right';
          ctx.fillText(l.rotulo, l.lado === 'esquerda' ? left + 4 : right - 4, y - 5);
          ctx.restore();
        });
      },
    });

    // Rótulo de valor na ponta de barras escolhidas.
    Chart.register({
      id: 'rotulosPonta',
      afterDatasetsDraw(chart, _a, opts) {
        if (!opts || !opts.ativo) return;
        const ctx = chart.ctx;
        const horizontal = chart.options.indexAxis === 'y';
        chart.data.datasets.forEach((ds, di) => {
          if (opts.dataset !== undefined && opts.dataset !== di) return;
          const meta = chart.getDatasetMeta(di);
          meta.data.forEach((barra, i) => {
            if (opts.indices && !opts.indices.includes(i)) return;
            const v = ds.data[i];
            if (v === null || v === undefined) return;
            ctx.save();
            ctx.fillStyle = COR.texto2;
            ctx.font = '600 12px system-ui, sans-serif';
            const txt = opts.formato(v, i);
            if (barra.base === undefined) {
              ctx.textAlign = opts.alinhar || 'left'; ctx.textBaseline = 'middle';
              ctx.fillText(txt, barra.x + (opts.alinhar === 'right' ? -10 : 10), barra.y);
            } else if (horizontal) {
              ctx.textAlign = 'left'; ctx.textBaseline = 'middle';
              ctx.fillText(txt, Math.max(barra.x, barra.base) + 6, barra.y);
            } else {
              ctx.textAlign = 'center'; ctx.textBaseline = 'bottom';
              ctx.fillText(txt, barra.x, Math.min(barra.y, barra.base) - 4);
            }
            ctx.restore();
          });
        });
      },
    });
  }

  const eixoY = (extra) => Object.assign({
    grid: { color: COR.grade, drawTicks: false },
    border: { display: false },
    ticks: { padding: 8 },
  }, extra || {});
  const eixoX = (extra) => Object.assign({
    grid: { display: false },
    border: { color: COR.eixo },
    ticks: { padding: 6 },
  }, extra || {});
  const barra = (cor) => ({
    backgroundColor: cor,
    hoverBackgroundColor: cor + 'cc',
    borderRadius: 4,
    borderSkipped: 'start',
    maxBarThickness: 24,
  });

  // ---------- Gráficos ----------
  const graficos = {};

  function graficoMeses(calc) {
    const m = calc.meses;
    const rot = m.map((x) => rotuloMes(x.mes));
    graficos.mesPont = new Chart(el('g-mes-pont'), {
      type: 'line',
      data: {
        labels: rot,
        datasets: [{
          data: m.map((x) => parseFloat(x.pontualidade_pct)),
          borderColor: COR.serie, borderWidth: 2, tension: 0,
          pointRadius: 0, pointHoverRadius: 5, pointHoverBorderWidth: 2,
          pointHoverBackgroundColor: COR.serie, pointHoverBorderColor: COR.painel,
          fill: true, backgroundColor: 'rgba(43,159,220,0.10)',
        }],
      },
      options: {
        interaction: { mode: 'index', intersect: false },
        plugins: {
          miraVertical: true,
          tooltip: { callbacks: {
            label: (c) => fmtPct(c.parsed.y, 2) + ' pontuais',
            footer: (it) => fmtInt(m[it[0].dataIndex].voos) + ' voos',
          } },
        },
        scales: {
          x: eixoX({ ticks: { padding: 6, maxRotation: 0, autoSkip: true } }),
          y: eixoY({ suggestedMin: 70, suggestedMax: 90, ticks: { padding: 8, callback: (v) => v + '%' } }),
        },
      },
    });

    graficos.mesCanc = new Chart(el('g-mes-canc'), {
      type: 'bar',
      data: { labels: rot, datasets: [Object.assign({ data: m.map((x) => parseFloat(x.cancelamento_pct)) }, barra(COR.serie))] },
      options: {
        plugins: { tooltip: { callbacks: {
          label: (c) => fmtPct(c.parsed.y, 2) + ' cancelados',
          footer: (it) => fmtInt(m[it[0].dataIndex].voos) + ' voos',
        } } },
        scales: { x: eixoX({ ticks: { padding: 6, maxRotation: 0, autoSkip: true } }), y: eixoY({ beginAtZero: true, ticks: { padding: 8, callback: (v) => fmtDec(v, 1) + '%' } }) },
      },
    });

    colocarTabela('mes', ['Mês', 'Voos', 'Pontualidade de chegada', 'Cancelamento (sem voos-fantasma)'],
      m.map((x) => [rotuloMes(x.mes), fmtInt(x.voos), fmtPct(parseFloat(x.pontualidade_pct), 2), fmtPct(parseFloat(x.cancelamento_pct), 2)]),
      [1, 2, 3]);
  }

  function graficoHora(d) {
    const h = d.cascata_por_hora;
    const atras = h.map((x) => +(100 - parseFloat(x.pontualidade_pct)).toFixed(1));
    const iMax = atras.indexOf(Math.max(...atras));
    const iMin = atras.indexOf(Math.min(...atras));
    graficos.hora = new Chart(el('g-hora'), {
      type: 'bar',
      data: {
        labels: h.map((x) => String(x.hora_brasilia).padStart(2, '0') + 'h'),
        datasets: [Object.assign({ data: atras }, barra(COR.serie))],
      },
      options: {
        layout: { padding: { top: 18 } },
        plugins: {
          rotulosPonta: { ativo: true, indices: [iMin, iMax], formato: (v) => fmtPct(v, 1) },
          tooltip: { callbacks: {
            title: (it) => 'Partidas das ' + it[0].label,
            label: (c) => fmtPct(c.parsed.y, 1) + ' atrasadas',
            footer: (it) => {
              const x = h[it[0].dataIndex];
              return fmtInt(x.voos) + ' voos programados · atraso médio ' + fmtDec(x.atraso_medio, 1) + ' min';
            },
          } },
        },
        scales: { x: eixoX(), y: eixoY({ beginAtZero: true, ticks: { padding: 8, callback: (v) => v + '%' } }) },
      },
    });
    colocarTabela('hora', ['Hora (Brasília)', 'Voos programados', '% atrasadas (> 15 min)', 'Atraso médio (min)'],
      h.map((x, i) => [String(x.hora_brasilia).padStart(2, '0') + 'h', fmtInt(x.voos), fmtPct(atras[i], 1), fmtDec(x.atraso_medio, 1)]),
      [1, 2, 3]);
  }

  // Mapa de calor: rampa de uma cor só (azul), do escuro (menos atrasos) ao claro (mais atrasos).
  const RAMPA_A = [22, 40, 59];   // #16283b
  const RAMPA_B = [108, 196, 245]; // #6cc4f5
  const corRampa = (t) => 'rgb(' + RAMPA_A.map((a, i) => Math.round(a + (RAMPA_B[i] - a) * t)).join(',') + ')';

  function mapaCalor(d) {
    const cel = d.heatmap_hora_dia.map((x) => ({
      dia: x.num_dia_semana, hora: x.hora_brasilia, voos: x.voos,
      atras: +(100 - parseFloat(x.pontualidade_pct)).toFixed(1),
    }));
    const vals = cel.map((c) => c.atras);
    const min = Math.min(...vals), max = Math.max(...vals);
    const grade = el('calor');
    const dica = el('dica');
    grade.textContent = '';
    grade.appendChild(document.createElement('div'));
    for (let h = 0; h < 24; h++) {
      const r = document.createElement('div');
      r.className = 'rot-h';
      r.textContent = h % 3 === 0 ? String(h).padStart(2, '0') : '';
      grade.appendChild(r);
    }
    const mostrar = (c, x, y) => {
      dica.textContent = '';
      const b = document.createElement('b');
      b.textContent = fmtPct(c.atras, 1) + ' atrasadas';
      dica.appendChild(b);
      dica.appendChild(document.createTextNode(DIAS[c.dia] + ', ' + String(c.hora).padStart(2, '0') + 'h · ' + fmtInt(c.voos) + ' voos programados'));
      dica.style.display = 'block';
      const w = dica.offsetWidth;
      dica.style.left = Math.min(x + 14, window.innerWidth - w - 8) + 'px';
      dica.style.top = (y + 14) + 'px';
    };
    for (let dia = 1; dia <= 7; dia++) {
      const r = document.createElement('div');
      r.className = 'rot';
      r.textContent = DIAS[dia];
      grade.appendChild(r);
      for (let h = 0; h < 24; h++) {
        const c = cel.find((z) => z.dia === dia && z.hora === h);
        const q = document.createElement('div');
        q.className = 'cel';
        q.tabIndex = 0;
        if (c) {
          q.style.background = corRampa((c.atras - min) / (max - min || 1));
          q.setAttribute('aria-label', DIAS[dia] + ' ' + h + 'h: ' + fmtPct(c.atras, 1) + ' atrasadas');
          q.addEventListener('pointermove', (e) => mostrar(c, e.clientX, e.clientY));
          q.addEventListener('pointerleave', () => { dica.style.display = 'none'; });
          q.addEventListener('focus', () => { const b = q.getBoundingClientRect(); mostrar(c, b.right, b.bottom); });
          q.addEventListener('blur', () => { dica.style.display = 'none'; });
        }
        grade.appendChild(q);
      }
    }
    el('escala-barra').style.background = 'linear-gradient(90deg,' + corRampa(0) + ',' + corRampa(1) + ')';
    el('escala-limites').textContent = '(' + fmtPct(min, 1) + ' a ' + fmtPct(max, 1) + ')';
    colocarTabela('calor', ['Dia', 'Hora', 'Voos programados', '% atrasadas'],
      cel.map((c) => [DIAS[c.dia], String(c.hora).padStart(2, '0') + 'h', fmtInt(c.voos), fmtPct(c.atras, 1)]),
      [2, 3]);
  }

  function graficosCompanhias(d) {
    const top = d.ranking_companhias.slice().sort((a, b) => b.registros - a.registros).slice(0, 10);
    graficos.ciaPont = new Chart(el('g-cia-pont'), {
      type: 'bar',
      data: {
        labels: top.map((c) => nomeCia(c.icao_empresa, c.nome_companhia)),
        datasets: [Object.assign({ data: top.map((c) => parseFloat(c.pontualidade_pct)) }, barra(COR.serie))],
      },
      options: {
        indexAxis: 'y',
        layout: { padding: { right: 56 } },
        plugins: {
          rotulosPonta: { ativo: true, formato: (v) => fmtPct(v, 1) },
          tooltip: { callbacks: {
            label: (c) => fmtPct(c.parsed.x, 2) + ' das partidas pontuais',
            footer: (it) => fmtInt(top[it[0].dataIndex].registros) + ' registros',
          } },
        },
        scales: {
          x: eixoY({ min: 0, max: 100, ticks: { padding: 6, callback: (v) => v + '%' } }),
          y: eixoX({ border: { color: COR.eixo }, ticks: { color: COR.texto2 } }),
        },
      },
    });
    colocarTabela('cia-pont', ['Companhia', 'Código ICAO', 'Registros', 'Pontualidade de partida'],
      top.map((c) => [c.nome_companhia, c.icao_empresa, fmtInt(c.registros), fmtPct(parseFloat(c.pontualidade_pct), 2)]),
      [2, 3]);

    const pares = {};
    d.voos_fantasma.forEach((f) => { pares[f.icao_empresa] = (pares[f.icao_empresa] || 0) + 1; });
    const comFantasma = d.ranking_companhias
      .filter((c) => pares[c.icao_empresa])
      .sort((a, b) => b.registros - a.registros);
    graficos.ciaCanc = new Chart(el('g-cia-canc'), {
      type: 'bar',
      data: {
        labels: comFantasma.map((c) => nomeCia(c.icao_empresa, c.nome_companhia)),
        datasets: [
          Object.assign({ label: 'Bruto', data: comFantasma.map((c) => parseFloat(c.cancelamento_bruto)) }, barra(COR.cinza), { maxBarThickness: 12 }),
          Object.assign({ label: 'Após R2', data: comFantasma.map((c) => parseFloat(c.cancelamento_real)) }, barra(COR.serie), { maxBarThickness: 12 }),
        ],
      },
      options: {
        indexAxis: 'y',
        datasets: { bar: { categoryPercentage: 0.7, barPercentage: 0.9 } },
        interaction: { mode: 'index', axis: 'y', intersect: false },
        plugins: { tooltip: { callbacks: {
          label: (c) => c.dataset.label + ': ' + fmtPct(c.parsed.x, 2),
          footer: (it) => {
            const c = comFantasma[it[0].dataIndex];
            return pares[c.icao_empresa] + (pares[c.icao_empresa] === 1 ? ' par' : ' pares') + ' de voos-fantasma · ' + fmtInt(c.registros) + ' registros';
          },
        } } },
        scales: {
          x: eixoY({ beginAtZero: true, ticks: { padding: 6, callback: (v) => v + '%' } }),
          y: eixoX({ border: { color: COR.eixo }, ticks: { color: COR.texto2 } }),
        },
      },
    });
    colocarTabela('cia-canc', ['Companhia', 'Pares de voos-fantasma', 'Cancelamento bruto', 'Cancelamento após R2'],
      comFantasma.map((c) => [c.nome_companhia, fmtInt(pares[c.icao_empresa]), fmtPct(parseFloat(c.cancelamento_bruto), 2), fmtPct(parseFloat(c.cancelamento_real), 2)]),
      [1, 2, 3]);
  }

  // ---------- Qualidade dos dados: funil do denominador do cancelamento ----------
  // As 6.813 linhas de voo-fantasma são todas DI 0 e nenhuma é duplicata de origem, então
  // todas estão entre os voos elegíveis da R1: sem fantasma = elegíveis − linhas_fantasma.
  function graficoFunil(d) {
    const r = d.resumo[0];
    const semFantasma = r.voos_elegiveis - r.linhas_fantasma;
    const etapas = [
      { rotulo: 'Registros na base', valor: r.registros, sai: null, oque: 'todos os registros do VRA no período' },
      { rotulo: 'Entram na conta (R1)', valor: r.voos_elegiveis, sai: r.registros - r.voos_elegiveis, oque: 'saem os DI não programados e as duplicatas de origem' },
      { rotulo: 'Sem voos-fantasma (R2)', valor: semFantasma, sai: r.voos_elegiveis - semFantasma, oque: 'saem as linhas de voo-fantasma' },
    ];
    graficos.funil = new Chart(el('g-funil'), {
      type: 'bar',
      data: {
        labels: etapas.map((e) => e.rotulo),
        datasets: [Object.assign({ data: etapas.map((e) => e.valor) }, barra(COR.serie), { maxBarThickness: 30 })],
      },
      options: {
        indexAxis: 'y',
        layout: { padding: { right: 150 } },
        plugins: {
          rotulosPonta: { ativo: true, formato: (v, i) => fmtInt(v) + (etapas[i].sai ? '  (−' + fmtInt(etapas[i].sai) + ')' : '') },
          tooltip: { callbacks: {
            label: (c) => fmtInt(c.parsed.x) + ' registros',
            footer: (it) => etapas[it[0].dataIndex].oque,
          } },
        },
        scales: {
          x: eixoY({ beginAtZero: true, ticks: { padding: 6, callback: (v) => fmtInt(v) } }),
          y: eixoX({ border: { color: COR.eixo }, ticks: { color: COR.texto2 } }),
        },
      },
    });
    colocarTabela('funil', ['Etapa', 'Registros', 'Saem nesta etapa', 'O que sai'],
      etapas.map((e) => [e.rotulo, fmtInt(e.valor), e.sai ? fmtInt(e.sai) : '—', e.oque]),
      [1, 2]);
  }

  // ---------- Rotas ----------
  function rotas(d) {
    const aero = {};
    d.aeroportos.forEach((a) => { aero[a.icao_aeroporto] = a; });
    const cias = {};
    d.ranking_companhias.forEach((c) => { cias[c.icao_empresa] = c; });
    const lugar = (icao) => {
      const a = aero[icao];
      if (!a || !a.praca_aeroporto) return icao; // aeroporto estrangeiro sem cidade no cadastro
      return a.praca_aeroporto + (a.uf_aeroporto ? ' (' + a.uf_aeroporto + ')' : '');
    };
    const lista = d.rotas.slice().sort((a, b) => b.voos - a.voos);
    const rotulo = (r) => r.rota_icao.replace(' - ', ' → ') + ' · ' + r.icao_empresa;
    const porRotulo = {};
    const dl = el('rota-lista');
    lista.forEach((r) => {
      porRotulo[rotulo(r)] = r;
      const o = document.createElement('option');
      o.value = rotulo(r);
      o.label = fmtInt(r.voos) + ' voos';
      dl.appendChild(o);
    });

    function selecionar(r) {
      const [o, dd] = r.rota_icao.split(' - ');
      el('rota-titulo').textContent = o + ' → ' + dd;
      const c = cias[r.icao_empresa];
      el('rota-sub').textContent = lugar(o) + ' → ' + lugar(dd) + ' · ' + (c ? nomeCia(r.icao_empresa, c.nome_companhia) : r.icao_empresa);
      el('rota-voos').textContent = fmtInt(r.voos);
      el('rota-indice').textContent = r.indice !== null ? fmtDec(parseFloat(r.indice), 1) : '—';
      el('rota-pont').textContent = r.pontualidade_pct !== null ? fmtPct(parseFloat(r.pontualidade_pct), 1) : '—';
      el('rota-km').textContent = r.km !== null ? fmtInt(Math.round(r.km)) + ' km' : '—';
      el('rota-busca').value = rotulo(r);
      if (graficos.dispersao) {
        graficos.dispersao.data.datasets[2].data = (r.atraso_medio !== null && r.atraso_p90 !== null)
          ? [{ x: r.atraso_medio, y: r.atraso_p90, r }] : [];
        graficos.dispersao.update('none');
      }
      const valores = [r.atraso_medio, r.atraso_p50, r.atraso_p90];
      if (graficos.rota) graficos.rota.destroy();
      graficos.rota = new Chart(el('g-rota'), {
        type: 'bar',
        data: { labels: ['Média', 'P50 (mediana)', 'P90'], datasets: [Object.assign({ data: valores }, barra(COR.serie), { maxBarThickness: 44 })] },
        options: {
          layout: { padding: { top: 20 } },
          plugins: {
            rotulosPonta: { ativo: true, formato: (v) => fmtDec(v, 1) + ' min' },
            tooltip: { callbacks: { label: (x) => fmtDec(x.parsed.y, 1) + ' min' } },
          },
          scales: { x: eixoX({ ticks: { color: COR.texto2 } }), y: eixoY({ ticks: { padding: 8, callback: (v) => v + ' min' } }) },
        },
      });
    }

    el('rota-busca').addEventListener('change', (e) => {
      const r = porRotulo[e.target.value.trim()];
      if (r) selecionar(r);
    });

    // Dispersão: atraso médio × P90 (R6 — a média esconde a cauda)
    const pontos = d.rotas.filter((r) => r.voos >= 500 && r.atraso_p90 !== null && r.atraso_medio !== null);
    const mediana = (arr) => {
      const s = arr.slice().sort((a, b) => a - b);
      const m = s.length / 2;
      return s.length % 2 ? s[Math.floor(m)] : (s[m - 1] + s[m]) / 2;
    };
    el('disp-n').textContent = fmtInt(pontos.length);
    el('disp-med-media').textContent = fmtDec(mediana(pontos.map((r) => r.atraso_medio)), 1) + ' min';
    el('disp-med-p90').textContent = fmtDec(mediana(pontos.map((r) => r.atraso_p90)), 1) + ' min';
    const xs = pontos.map((r) => r.atraso_medio);
    const lo = Math.floor(Math.min(...xs) / 10) * 10;
    const hi = Math.ceil(Math.max(...xs) / 10) * 10;
    graficos.dispersao = new Chart(el('g-dispersao'), {
      type: 'scatter',
      data: { datasets: [
        { data: [{ x: lo, y: lo }, { x: hi, y: hi }], showLine: true, borderColor: COR.muted, borderWidth: 1,
          borderDash: [4, 4], pointRadius: 0, pointHoverRadius: 0, pointHitRadius: 0 },
        { data: pontos.map((r) => ({ x: r.atraso_medio, y: r.atraso_p90, r })),
          backgroundColor: 'rgba(43,159,220,0.55)', borderWidth: 0, pointRadius: 3, pointHoverRadius: 5,
          pointHoverBackgroundColor: COR.serie },
        { data: [], backgroundColor: COR.alerta, borderColor: COR.painel, borderWidth: 2, pointRadius: 6, pointHoverRadius: 7 },
      ] },
      options: {
        interaction: { mode: 'nearest', intersect: true },
        plugins: { tooltip: {
          filter: (it) => it.datasetIndex > 0,
          callbacks: {
            title: (it) => (it.length ? rotulo(it[0].raw.r) : ''),
            label: (c) => 'média ' + fmtDec(c.raw.x, 1) + ' min · P90 ' + fmtDec(c.raw.y, 1) + ' min',
            footer: (it) => (it.length ? fmtInt(it[0].raw.r.voos) + ' voos' : ''),
          },
        } },
        onHover: (ev, els) => {
          ev.native.target.style.cursor = els.some((x) => x.datasetIndex > 0) ? 'pointer' : 'default';
        },
        onClick: (ev, els) => {
          const e = els.find((x) => x.datasetIndex > 0);
          if (!e) return;
          selecionar(graficos.dispersao.data.datasets[e.datasetIndex].data[e.index].r);
          el('dashboard-rotas').scrollIntoView({ behavior: 'smooth' });
        },
        scales: {
          x: eixoX({ min: lo, max: hi, grid: { color: COR.grade, drawTicks: false }, title: { display: true, text: 'Atraso médio de chegada (min)', color: COR.muted },
            ticks: { padding: 6, callback: (v) => v + ' min' } }),
          y: eixoY({ min: Math.floor(lo / 50) * 50, title: { display: true, text: 'P90 do atraso de chegada (min)', color: COR.muted },
            ticks: { padding: 8, callback: (v) => v + ' min' } }),
        },
      },
    });

    // Tabela: onde a média mais se afasta do P90
    const top = d.rotas.filter((r) => r.voos >= 500 && r.atraso_p90 !== null && r.atraso_medio !== null)
      .map((r) => Object.assign({ dif: r.atraso_p90 - r.atraso_medio }, r))
      .sort((a, b) => b.dif - a.dif).slice(0, 10);
    const t = el('t-rotas');
    t.textContent = '';
    const cab = ['Rota', 'Companhia', 'Voos', 'Média', 'P50', 'P90', 'P90 − média'];
    const hr = t.createTHead().insertRow();
    cab.forEach((c, i) => { const th = document.createElement('th'); th.textContent = c; if (i >= 2) th.className = 'num'; hr.appendChild(th); });
    const tb = t.createTBody();
    top.forEach((r) => {
      const tr = tb.insertRow();
      tr.style.cursor = 'pointer';
      tr.tabIndex = 0;
      const vals = [r.rota_icao.replace(' - ', ' → '), r.icao_empresa, fmtInt(r.voos),
        fmtDec(r.atraso_medio, 1) + ' min', fmtDec(r.atraso_p50, 1) + ' min', fmtDec(r.atraso_p90, 1) + ' min', fmtDec(r.dif, 1) + ' min'];
      vals.forEach((v, i) => {
        const td = tr.insertCell();
        if (i === 0 || i === 6) { const s = document.createElement('strong'); s.textContent = v; td.appendChild(s); } else td.textContent = v;
        if (i >= 2) td.className = 'num';
      });
      const abrir = () => { selecionar(r); el('dashboard-rotas').scrollIntoView({ behavior: 'smooth' }); };
      tr.addEventListener('click', abrir);
      tr.addEventListener('keydown', (e) => { if (e.key === 'Enter') abrir(); });
    });

    selecionar(d.rotas.find((r) => r.rota_icao === 'SBNF - SBGR' && r.icao_empresa === 'TAM') || lista[0]);
  }

  // ---------- Aeroportos ----------
  function aeroportos(d) {
    const top = d.aeroportos.slice().sort((a, b) => b.voos - a.voos).slice(0, 15);
    const max = top[0].voos;
    const t = el('t-aeroportos');
    t.textContent = '';
    const cab = ['#', 'Aeroporto', 'Cidade', 'Registros de partida', 'Pontualidade de partida'];
    const hr = t.createTHead().insertRow();
    cab.forEach((c, i) => { const th = document.createElement('th'); th.textContent = c; if (i === 0 || i === 4) th.className = 'num'; hr.appendChild(th); });
    const tb = t.createTBody();
    top.forEach((a, i) => {
      const tr = tb.insertRow();
      const c0 = tr.insertCell(); c0.textContent = i + 1; c0.className = 'num';
      const c1 = tr.insertCell();
      const s = document.createElement('strong'); s.textContent = a.icao_aeroporto; c1.appendChild(s);
      c1.appendChild(document.createTextNode(' ' + a.nome_aeroporto));
      const c2 = tr.insertCell(); c2.textContent = a.praca_aeroporto + (a.uf_aeroporto ? ' · ' + a.uf_aeroporto : '');
      const c3 = tr.insertCell();
      const wrap = document.createElement('div'); wrap.className = 'barra-cel';
      const bar = document.createElement('i'); bar.style.width = Math.max(2, (120 * a.voos) / max) + 'px';
      const num = document.createElement('span'); num.textContent = fmtInt(a.voos);
      wrap.appendChild(bar); wrap.appendChild(num); c3.appendChild(wrap);
      const c4 = tr.insertCell(); c4.className = 'num';
      c4.textContent = a.pontualidade_pct !== null ? fmtPct(parseFloat(a.pontualidade_pct), 1) : '—';
    });
  }

  // ---------- Dias atípicos ----------
  function graficoDias(d) {
    const dias = d.dias_atipicos;
    // mediana calculada com a taxa exata de cada dia (cancelados ÷ voos), como no 03_gold
    const taxas = dias.map((x) => (100 * x.cancelados) / x.voos).sort((a, b) => a - b);
    const meio = taxas.length / 2;
    const mediana = taxas.length % 2 ? taxas[Math.floor(meio)] : (taxas[meio - 1] + taxas[meio]) / 2;
    const fator = d.regras.atipico_fator;
    const corte = mediana * fator;
    const atip = dias.map((x) => x.dia_atipico);
    const iPico = dias.reduce((m, x, i) => (x.taxa_pct > dias[m].taxa_pct ? i : m), 0);
    graficos.dias = new Chart(el('g-dias'), {
      type: 'line',
      data: {
        labels: dias.map((x) => x.dia),
        datasets: [{
          data: dias.map((x) => x.taxa_pct),
          borderColor: COR.serie, borderWidth: 1.5, tension: 0,
          pointRadius: (c) => (atip[c.dataIndex] ? 5 : 0),
          pointBackgroundColor: COR.alerta, pointBorderColor: COR.painel, pointBorderWidth: 2,
          pointHoverRadius: 5, pointHoverBackgroundColor: (c) => (atip[c.dataIndex] ? COR.alerta : COR.serie),
        }],
      },
      options: {
        interaction: { mode: 'index', intersect: false },
        layout: { padding: { top: 8 } },
        plugins: {
          miraVertical: true,
          rotulosPonta: { ativo: true, indices: [iPico], formato: (v, i) => rotuloDia(dias[i].dia) + ' · ' + fmtPct(v, 2) },
          linhasRef: { linhas: [
            { valor: mediana, cor: COR.muted, rotulo: 'mediana ' + fmtPct(mediana, 2), lado: 'esquerda' },
            { valor: corte, cor: COR.alerta, rotulo: 'corte R5 ' + fmtPct(corte, 2) },
          ] },
          tooltip: { callbacks: {
            title: (it) => rotuloDia(it[0].label),
            label: (c) => fmtPct(c.parsed.y, 2) + ' cancelados',
            footer: (it) => {
              const x = dias[it[0].dataIndex];
              return fmtInt(x.cancelados) + ' de ' + fmtInt(x.voos) + ' voos' + (x.dia_atipico ? ' · dia atípico' : '');
            },
          } },
        },
        scales: {
          x: eixoX({ ticks: {
            autoSkip: false, maxRotation: 0,
            callback: function (v, i) {
              const dia = this.getLabelForValue(v);
              return dia.slice(8, 10) === '01' ? rotuloMes(dia.slice(0, 7)) : null;
            },
          } }),
          y: eixoY({ beginAtZero: true, ticks: { padding: 8, callback: (v) => v + '%' } }),
        },
      },
    });
    const top = dias.slice().sort((a, b) => b.taxa_pct - a.taxa_pct).slice(0, 10);
    colocarTabela('dias', ['Dia', 'Voos', 'Cancelados', 'Taxa', 'R5'],
      top.map((x) => [rotuloDia(x.dia), fmtInt(x.voos), fmtInt(x.cancelados), fmtPct(x.taxa_pct, 2), x.dia_atipico ? 'acima do corte' : 'abaixo do corte']),
      [1, 2, 3]);
  }

  // ---------- Caixa de perguntas do agente ----------
  // Só manda o texto da pergunta para /api/perguntar (servidor do Docker).
  // As chaves ficam no servidor; nada aqui lê ou guarda credencial.
  // Tudo que vem do servidor entra na página como texto (textContent), nunca como HTML.
  function iniciarChat() {
    const form = el('chat-form');
    if (!form) return;
    const campo = el('chat-pergunta');
    const botao = el('chat-enviar');
    const status = el('chat-status');
    const statusTxt = el('chat-status-txt');
    const caixaRes = el('chat-resultado');
    const caixaErro = el('chat-erro');
    let disponivel = false;
    let enviando = false;

    const MSG_OFFLINE = 'Servidor do agente não encontrado. A caixa de perguntas só funciona com o site aberto pelo Docker ' +
      '(docker compose up -d --build web api, depois http://localhost:8080). O resto da página funciona normalmente.';

    // A linha de status só aparece quando há problema; com o agente pronto ela some.
    function estado(nome, texto) {
      status.dataset.estado = nome;
      statusTxt.textContent = texto;
      status.hidden = nome === 'pronto';
    }
    function mostrarErro(texto) {
      caixaErro.textContent = texto;
      caixaErro.hidden = false;
    }
    function atualizarBotao() {
      botao.disabled = !disponivel || enviando;
      botao.textContent = enviando ? 'Consultando…' : 'Perguntar';
    }

    async function verificar() {
      if (location.protocol === 'file:') {
        estado('offline', 'Agente indisponível: abra o site pelo Docker (docker compose up -d --build web api) em http://localhost:8080.');
        disponivel = false; atualizarBotao(); return;
      }
      try {
        const r = await fetch('api/saude', { cache: 'no-store' });
        if (!r.ok) throw new Error(String(r.status));
        const j = await r.json();
        if (j.configurado) {
          estado('pronto', '');
          disponivel = true;
        } else {
          estado('sem-chaves', 'Faltam as chaves em agente_ia/.env. Preencha e rode: docker compose up -d --force-recreate api');
          disponivel = false;
        }
      } catch (e) {
        estado('offline', 'Agente indisponível: abra o site pelo Docker (docker compose up -d --build web api) em http://localhost:8080.');
        disponivel = false;
      }
      atualizarBotao();
    }

    function mostrarResultado(j) {
      el('chat-pergunta-feita').textContent = 'Pergunta: ' + j.pergunta;
      el('chat-resposta').textContent = j.resposta || '(sem resposta)';
      caixaRes.hidden = false;
    }

    form.addEventListener('submit', async (ev) => {
      ev.preventDefault();
      const pergunta = campo.value.trim();
      caixaErro.hidden = true;
      if (pergunta.length < 3) { mostrarErro('Escreva uma pergunta.'); return; }
      if (!disponivel) { mostrarErro(MSG_OFFLINE); return; }
      enviando = true; atualizarBotao();
      try {
        const r = await fetch('api/perguntar', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pergunta }),
        });
        let j = null;
        try { j = await r.json(); } catch (e) { j = null; }
        if (!r.ok || !j) {
          mostrarErro((j && j.erro) || (r.status === 502 || r.status === 504
            ? 'O servidor do agente não respondeu. Veja se o contêiner "api" está rodando: docker compose ps'
            : 'Erro ' + r.status + ' ao consultar o agente.'));
        } else {
          mostrarResultado(j);
        }
      } catch (e) {
        mostrarErro(MSG_OFFLINE);
      } finally {
        enviando = false; atualizarBotao();
      }
    });

    campo.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' && !ev.shiftKey) { ev.preventDefault(); form.requestSubmit(); }
    });
    document.querySelectorAll('#chat-sugestoes .chip').forEach((c) => {
      c.addEventListener('click', () => { campo.value = c.textContent; campo.focus(); });
    });

    atualizarBotao();
    verificar();
  }

  // ---------- Navegação ----------
  let dados = null;
  let dashboardPronto = false;

  function montarDashboard() {
    if (dashboardPronto || !dados || typeof Chart === 'undefined') return;
    dashboardPronto = true;
    configurarChart();
    const calc = calcular(dados);
    graficoMeses(calc);
    graficoFunil(dados);
    graficoHora(dados);
    mapaCalor(dados);
    graficosCompanhias(dados);
    rotas(dados);
    aeroportos(dados);
    graficoDias(dados);
  }

  function navegar() {
    const partes = (location.hash.replace('#', '') || 'dashboard').split('/');
    const pagina = ['dashboard', 'agente'].includes(partes[0]) ? partes[0] : 'dashboard';
    document.querySelectorAll('.pagina').forEach((p) => p.classList.toggle('ativa', p.id === 'pagina-' + pagina));
    document.querySelectorAll('.menu-item').forEach((a) => a.classList.toggle('ativo', a.dataset.alvo === pagina));
    el('submenu-dashboard').classList.toggle('aberto', pagina === 'dashboard');
    document.title = { dashboard: 'VoeBem · Dashboard', agente: 'VoeBem · Agente de IA' }[pagina];
    if (pagina === 'dashboard') montarDashboard();
    const alvo = partes[1] && el('dashboard-' + partes[1]);
    if (alvo) alvo.scrollIntoView();
    else window.scrollTo(0, 0);
  }

  window.addEventListener('hashchange', navegar);
  iniciarChat();
  carregarDados().then((d) => {
    dados = d;
    preencherNumeros(calcular(d).num);
    navegar();
  });
})();
