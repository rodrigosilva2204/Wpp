/**
 * WPP Promoções — Servidor principal
 *
 * Painel web que conecta ao WhatsApp via QR Code, monitora grupos,
 * encaminha mensagens por regras e substitui links de afiliado do Mercado Livre.
 *
 * Porta padrão: 3080 (ou variável de ambiente PORT)
 * Config persistida em: data/config.json
 * Sessão do WhatsApp em: data/.wwebjs_auth/
 */

const express = require('express');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');
const qrcode = require('qrcode');
const { Client, LocalAuth } = require('whatsapp-web.js');

// ─── Constantes de caminho ────────────────────────────────────────────────────

const PORT = process.env.PORT || 3080;
const DATA_DIR = path.join(__dirname, 'data');
const CONFIG_FILE = path.join(DATA_DIR, 'config.json');
const STATS_FILE = path.join(DATA_DIR, 'stats.json');
const SESSION_DIR = path.join(DATA_DIR, '.wwebjs_auth');

fs.mkdirSync(DATA_DIR, { recursive: true });

// ─── Configuração padrão ──────────────────────────────────────────────────────

const defaultConfig = {
  rules: [],
  settings: {
    ignoreOwnMessages: true,       // Ignora mensagens enviadas pelo próprio número
    simulationMode: true,          // Modo simulação: loga sem enviar de verdade
    avoidDuplicatesWindowSec: 120, // Janela em segundos para bloquear mensagens duplicadas
    affiliateEnabled: false,       // Ativa substituição automática de links de afiliado
    affiliateMattD2id: '',         // Seu ID de afiliado do Mercado Livre (matt_d2id)
    affiliateMattTracingId: '',    // Seu tracing ID de afiliado (matt_tracing_id)
    affiliateApiToken: '',         // Token base64 do painel de afiliados ML (ssid)
    affiliateApiTag: '',           // Tag de afiliado (ex: promofantasma) para gerar meli.la
    affiliateMattTool: '',         // ID da ferramenta de afiliado (matt_tool, ex: 79579775)
    affiliateSocialId: '',         // ID do seu perfil social ML (ex: cy20260504185420)
    scheduleEnabled: false,        // Ativa fila de envio com agenda de horário
    scheduleStartTime: '08:00',   // Horário de início do envio (HH:MM)
    scheduleEndTime: '22:00',     // Horário de fim do envio (HH:MM)
    scheduleCheckIntervalMinutes: 1,     // A cada quantos minutos verifica a fila
    scheduleDispatchIntervalMinutes: 5, // Intervalo mínimo (minutos) entre disparos
    amazonEnabled: false,          // Ativa afiliado Amazon
    amazonTag: '',                 // Amazon Associates tag (ex: meunome-20)
    shopeeEnabled: false,          // Ativa afiliado Shopee
    shopeeAffiliateId: '',         // ID de afiliado Shopee
    shopeeSubSource: ''            // Sub-fonte Shopee (opcional, ex: nome da campanha)
  }
};

// ─── Persistência de configuração ────────────────────────────────────────────

/**
 * Carrega a configuração do disco. Se o arquivo não existir, cria com valores padrão.
 * Faz merge com defaultConfig para garantir que campos novos sempre existam.
 */
function loadConfig() {
  if (!fs.existsSync(CONFIG_FILE)) {
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(defaultConfig, null, 2));
    return structuredClone(defaultConfig);
  }
  try {
    const cfg = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
    return {
      ...defaultConfig,
      ...cfg,
      settings: { ...defaultConfig.settings, ...(cfg.settings || {}) },
      rules: Array.isArray(cfg.rules) ? cfg.rules : []
    };
  } catch {
    return structuredClone(defaultConfig);
  }
}

/** Salva a configuração atual no disco em formato JSON indentado. */
function saveConfig() {
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2));
}

// ─── Estado em memória ────────────────────────────────────────────────────────

let config = loadConfig();
let latestQr = null;          // Último QR Code gerado (base64 data URL)
let status = 'iniciando';     // Status atual da conexão WhatsApp
let me = null;                // ID do número conectado (ex: 5511999@c.us)
let lastEvents = [];          // Histórico dos últimos 200 eventos de log
let cachedGroups = [];        // Cache dos grupos do WhatsApp (atualizado ao conectar)
const recentHashes = new Map(); // Mapa hash → timestamp para deduplicação de mensagens

// ─── Fila de envio agendado ───────────────────────────────────────────────────
let messageQueue = [];        // Fila de mensagens pendentes ({ outgoing, media, rule, ... })
let scheduleTimer = null;     // Timer de disparo (envia da fila a cada dispatchInterval)
let pollTimer = null;         // Timer de polling (verifica grupos a cada checkInterval)
let lastPollTimes = {};       // { groupId: timestamp } — última vez que cada grupo foi varrido
let lastDispatchTime = 0;     // Timestamp do último disparo bem-sucedido
let processingQueue = false;  // Guard de concorrência para processQueue
let wasInWindow = null;       // Rastreia estado da janela para evitar spam de log
let lastByPlatform = {};      // { [ruleId]: { ml|amazon|shopee: { outgoing, at } } }
let runningSchedule = false;  // Controla se busca e envio estão ativos (Start/Pause)

// ─── Estatísticas de envio ────────────────────────────────────────────────────
let stats = { daily: {}, byRule: {}, total: 0 };

// ─── Reconexão automática ─────────────────────────────────────────────────────
let reconnectAttempts = 0;
let reconnectTimer = null;
const MAX_RECONNECT = 5;

// ─── Funções utilitárias ──────────────────────────────────────────────────────

/**
 * Cria e distribui um evento de log para todos os clientes conectados via Socket.io.
 * @param {'info'|'success'|'warn'|'error'|'simulate'|'forward'} type - Tipo visual do log
 * @param {string} text - Texto principal do evento
 * @param {Object} meta - Dados adicionais exibidos no log (ruleName, sourceName, etc.)
 */
function pushEvent(type, text, meta = {}) {
  const item = {
    id: Date.now() + '-' + Math.random().toString(36).slice(2, 8),
    at: new Date().toISOString(),
    type,
    text,
    meta
  };
  lastEvents.unshift(item);
  lastEvents = lastEvents.slice(0, 200);
  io.emit('event', item);
}

// ─── Agenda de envio ──────────────────────────────────────────────────────────

function isWithinScheduleWindow(settings) {
  const now = new Date();
  const [sh, sm] = (settings.scheduleStartTime || '00:00').split(':').map(Number);
  const [eh, em] = (settings.scheduleEndTime || '23:59').split(':').map(Number);
  const nowMin = now.getHours() * 60 + now.getMinutes();
  return nowMin >= sh * 60 + sm && nowMin < eh * 60 + em;
}

// Retorna array de IDs de destino de uma regra (suporta targetGroupIds array e targetGroupId legado).
function getTargetIds(rule) {
  if (Array.isArray(rule.targetGroupIds) && rule.targetGroupIds.length > 0) return rule.targetGroupIds;
  if (rule.targetGroupId) return [rule.targetGroupId];
  return [];
}

// Stats helpers
function loadStats() {
  try {
    if (fs.existsSync(STATS_FILE)) return JSON.parse(fs.readFileSync(STATS_FILE, 'utf8'));
  } catch {}
  return { daily: {}, byRule: {}, total: 0 };
}

function saveStats() {
  try { fs.writeFileSync(STATS_FILE, JSON.stringify(stats)); } catch {}
}

function recordForward(rule, platforms = []) {
  const today = new Date().toISOString().slice(0, 10);
  const hour = new Date().getHours();
  if (!stats.daily[today]) stats.daily[today] = { total: 0, ml: 0, amazon: 0, shopee: 0, byHour: {} };
  stats.daily[today].total++;
  stats.daily[today].byHour[hour] = (stats.daily[today].byHour[hour] || 0) + 1;
  platforms.forEach(p => { if (p in stats.daily[today]) stats.daily[today][p]++; });
  if (!stats.byRule[rule.id]) stats.byRule[rule.id] = { name: rule.name, total: 0, ml: 0, amazon: 0, shopee: 0, lastAt: null };
  stats.byRule[rule.id].name = rule.name;
  stats.byRule[rule.id].total++;
  stats.byRule[rule.id].lastAt = new Date().toISOString();
  platforms.forEach(p => { if (p in stats.byRule[rule.id]) stats.byRule[rule.id][p]++; });
  stats.total++;
  saveStats();
}

// Reconexão automática com backoff exponencial
function scheduleReconnect() {
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  if (reconnectAttempts >= MAX_RECONNECT) {
    pushEvent('error', `[Reconexão] Limite de ${MAX_RECONNECT} tentativas atingido. Use o botão Reiniciar.`);
    return;
  }
  const delay = Math.min(5000 * Math.pow(2, reconnectAttempts), 60000);
  reconnectAttempts++;
  pushEvent('warn', `[Reconexão] Tentativa ${reconnectAttempts}/${MAX_RECONNECT} em ${Math.round(delay / 1000)}s...`);
  reconnectTimer = setTimeout(async () => {
    try {
      status = 'reconectando';
      io.emit('status', getStatus());
      await client.initialize();
    } catch (err) {
      pushEvent('error', `[Reconexão] Falha na tentativa ${reconnectAttempts}: ${err.message}`);
      scheduleReconnect();
    }
  }, delay);
}

// Processa uma mensagem contra todas as regras ativas.
// Usado tanto pelo listener de eventos quanto pelo polling periódico.
async function processMessageAgainstRules(message, chat) {
  if (config.settings.ignoreOwnMessages && message.fromMe) return;
  cleanupHashes();

  const body = normalizeText(message.body || '');
  if (!body) return;

  const sourceId = chat.id._serialized;
  const sourceName = chat.name || sourceId;

  for (const rule of config.rules.filter(r => r.active)) {
    if (rule.sourceGroupId !== sourceId) continue;

    let matched = false;
    let reason = '';
    if (rule.matchType === 'contains') {
      matched = body.toLowerCase().includes(String(rule.pattern || '').toLowerCase());
      reason = `contains:${rule.pattern}`;
    } else if (rule.matchType === 'regex') {
      try {
        matched = new RegExp(rule.pattern, rule.flags || 'i').test(body);
      } catch { matched = false; }
      reason = `regex:${rule.pattern}`;
    } else if (rule.matchType === 'startsWith') {
      matched = body.toLowerCase().startsWith(String(rule.pattern || '').toLowerCase());
      reason = `startsWith:${rule.pattern}`;
    }
    if (!matched) continue;

    // Filtra por plataforma: a mensagem deve conter link da(s) plataforma(s) configuradas na regra
    const rulePlatforms = Array.isArray(rule.platforms) && rule.platforms.length > 0 ? rule.platforms : ['ml'];
    const hasML     = rulePlatforms.includes('ml')     && ML_URL_RE.test(body);
    const hasAmazon = rulePlatforms.includes('amazon') && AMAZON_URL_RE.test(body);
    const hasShopee = rulePlatforms.includes('shopee') && SHOPEE_URL_RE.test(body);
    ML_URL_RE.lastIndex = 0; AMAZON_URL_RE.lastIndex = 0; SHOPEE_URL_RE.lastIndex = 0;
    if (!hasML && !hasAmazon && !hasShopee) continue;

    // Hash baseado no corpo ORIGINAL (antes de processar links) para deduplicação robusta.
    // Usar o texto processado causaria duplicatas quando a API ML retorna URLs meli.la diferentes.
    const hash = makeHash([rule.id, sourceId, rule.targetGroupId, compactText(body)]);
    if (recentHashes.has(hash)) {
      pushEvent('warn', 'Mensagem duplicada ignorada.', { ruleName: rule.name, sourceName });
      continue;
    }
    recentHashes.set(hash, Date.now());

    // Processa apenas plataformas que têm links detectados nesta mensagem
    const detectedPlatforms = [
      ...(hasML     ? ['ml']     : []),
      ...(hasAmazon ? ['amazon'] : []),
      ...(hasShopee ? ['shopee'] : [])
    ];
    const { text: processedBody, swapped: affiliateSwapped } = await replaceAllAffiliateLinks(body, config.settings, detectedPlatforms);
    const outgoing = (rule.prefix ? `${rule.prefix}\n` : '') + processedBody + (rule.suffix ? `\n${rule.suffix}` : '');

    // Rastreia a última promoção capturada por plataforma para reenvio manual
    if (!lastByPlatform[rule.id]) lastByPlatform[rule.id] = {};
    if (hasML)     lastByPlatform[rule.id].ml     = { outgoing, at: Date.now() };
    if (hasAmazon) lastByPlatform[rule.id].amazon = { outgoing, at: Date.now() };
    if (hasShopee) lastByPlatform[rule.id].shopee = { outgoing, at: Date.now() };

    if (config.settings.simulationMode) {
      pushEvent('simulate', 'Regra acionada em modo simulação.', {
        ruleName: rule.name, sourceName,
        targetGroupId: rule.targetGroupId, reason, affiliateSwapped,
        hasMedia: message.hasMedia, preview: outgoing.slice(0, 400)
      });
      continue;
    }

    let media = null;
    if (message.hasMedia) {
      try { media = await message.downloadMedia(); }
      catch (err) { pushEvent('warn', 'Não foi possível baixar mídia.', { error: err.message }); }
    }

    if (config.settings.scheduleEnabled) {
      messageQueue.unshift({ outgoing, media, rule, sourceName, reason, affiliateSwapped, addedAt: Date.now(), platforms: detectedPlatforms });
      io.emit('queue', { count: messageQueue.length, lastDispatchTime });
      pushEvent('info', `[Agenda] Enfileirada (prioridade). Fila: ${messageQueue.length}.`, {
        ruleName: rule.name, sourceName, preview: outgoing.slice(0, 400)
      });
      processQueue();
      continue;
    }

    const targetIds = getTargetIds(rule);
    for (const targetId of targetIds) {
      try {
        const targetChat = await client.getChatById(targetId);
        if (media) {
          await targetChat.sendMessage(media, { caption: outgoing });
        } else {
          await targetChat.sendMessage(outgoing);
        }
        pushEvent('forward', 'Mensagem encaminhada.', {
          ruleName: rule.name, sourceName,
          targetName: targetChat.name || targetId,
          reason, affiliateSwapped, hasMedia: !!media, preview: outgoing.slice(0, 400)
        });
      } catch (err) {
        pushEvent('error', `Erro ao enviar para destino ${targetId}.`, { error: err.message });
      }
    }
    if (targetIds.length > 0) recordForward(rule, detectedPlatforms);
  }
}

// Dispara uma verificação em todos os grupos monitorados buscando mensagens novas.
async function pollSourceGroups() {
  if (!runningSchedule || status !== 'conectado') return;
  const activeRules = config.rules.filter(r => r.active);
  if (activeRules.length === 0) return;

  const sourceGroupIds = [...new Set(activeRules.map(r => r.sourceGroupId))];

  for (const groupId of sourceGroupIds) {
    try {
      const chat = await client.getChatById(groupId);
      const messages = await chat.fetchMessages({ limit: 50 });
      const now = Date.now();

      if (lastPollTimes[groupId] === undefined) {
        // Primeira vez: marca o tempo atual sem processar mensagens antigas
        lastPollTimes[groupId] = now;
        pushEvent('info', `[Polling] Grupo "${chat.name || groupId}" registrado. Próximo ciclo capturará mensagens novas.`);
        continue;
      }

      const lastPoll = lastPollTimes[groupId];
      const novas = messages.filter(m => m.timestamp * 1000 > lastPoll);
      lastPollTimes[groupId] = now;

      if (novas.length === 0) continue;

      pushEvent('info', `[Polling] ${novas.length} msg nova(s) em "${chat.name || groupId}" — verificando regras...`);
      for (const msg of novas) {
        await processMessageAgainstRules(msg, chat);
      }
      // Tenta disparar imediatamente após processar o lote
      processQueue();
    } catch (err) {
      pushEvent('error', `[Polling] Erro no grupo ${groupId}.`, { error: err.message });
    }
  }
}

// Envia o próximo item da fila.
// force=true bypassa janela de horário e intervalo mínimo (usado no envio manual).
async function processQueue(force = false) {
  if ((!runningSchedule && !force) || processingQueue || messageQueue.length === 0 || status !== 'conectado') return;

  const inWindow = isWithinScheduleWindow(config.settings);
  if (inWindow !== wasInWindow) {
    wasInWindow = inWindow;
    pushEvent('info', inWindow
      ? `[Agenda] Entrou na janela ${config.settings.scheduleStartTime}–${config.settings.scheduleEndTime}. Fila: ${messageQueue.length} msg.`
      : `[Agenda] Fora da janela ${config.settings.scheduleStartTime}–${config.settings.scheduleEndTime}. Fila aguardando: ${messageQueue.length} msg.`
    );
  }
  if (!force && !inWindow) return;

  const minMs = Math.max(0, config.settings.scheduleDispatchIntervalMinutes || 0) * 60 * 1000;
  if (!force && minMs > 0 && Date.now() - lastDispatchTime < minMs) return;

  processingQueue = true;
  let item;
  try {
    item = messageQueue.shift();
    if (!item) return;
    const targetIds = getTargetIds(item.rule);
    let sent = false;
    for (const targetId of targetIds) {
      try {
        const targetChat = await client.getChatById(targetId);
        if (item.media) {
          await targetChat.sendMessage(item.media, { caption: item.outgoing });
        } else {
          await targetChat.sendMessage(item.outgoing);
        }
        pushEvent('forward', 'Mensagem enviada via agenda.', {
          ruleName: item.rule.name, sourceName: item.sourceName,
          targetName: targetChat.name || targetId,
          reason: item.reason, affiliateSwapped: item.affiliateSwapped,
          hasMedia: !!item.media, queueRemaining: messageQueue.length,
          preview: item.outgoing.slice(0, 400)
        });
        sent = true;
      } catch (err) {
        pushEvent('error', `[Agenda] Erro ao enviar para ${targetId}.`, { error: err.message });
      }
    }
    if (sent) {
      lastDispatchTime = Date.now();
      recordForward(item.rule, item.platforms || []);
    }
    io.emit('queue', { count: messageQueue.length, lastDispatchTime });
  } catch (err) {
    if (item) messageQueue.unshift(item);
    io.emit('queue', { count: messageQueue.length, lastDispatchTime });
    pushEvent('error', '[Agenda] Erro crítico na fila. Item devolvido.', { error: err.message });
  } finally {
    processingQueue = false;
  }
}

function startScheduleTimer() {
  if (scheduleTimer) { clearInterval(scheduleTimer); scheduleTimer = null; }
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  wasInWindow = null;

  if (!config.settings.scheduleEnabled) {
    runningSchedule = false;
    io.emit('scheduleState', { running: false });
    pushEvent('info', '[Agenda] Desativada.');
    return;
  }

  const checkMs = Math.max(1, config.settings.scheduleCheckIntervalMinutes || 1) * 60 * 1000;
  pollTimer = setInterval(pollSourceGroups, checkMs);
  scheduleTimer = setInterval(processQueue, 30 * 1000);

  pushEvent('info',
    `[Agenda] Configurada — Polling: ${config.settings.scheduleCheckIntervalMinutes} min | ` +
    `Intervalo envio: ${config.settings.scheduleDispatchIntervalMinutes} min | ` +
    `Janela: ${config.settings.scheduleStartTime}–${config.settings.scheduleEndTime}` +
    (runningSchedule ? ' | RODANDO' : ' | PAUSADO')
  );

  if (runningSchedule) pollSourceGroups();
  io.emit('scheduleState', { running: runningSchedule });
}

/**
 * Retorna o texto compactado (espaços múltiplos → espaço único, sem bordas).
 * Usado para gerar hashes de deduplicação consistentes.
 */
function compactText(v) {
  return String(v || '').replace(/\s+/g, ' ').trim();
}

/**
 * Normaliza quebras de linha (remove \r) e remove espaços nas bordas.
 * Usado para padronizar o corpo das mensagens recebidas.
 */
function normalizeText(v) {
  return String(v || '').replace(/\r/g, '').trim();
}

/**
 * Gera um hash base64 a partir de um array de partes.
 * Usado para identificar mensagens duplicadas dentro da janela de tempo.
 */
function makeHash(parts) {
  return Buffer.from(parts.join('|')).toString('base64').slice(0, 120);
}

/** Remove hashes expirados do mapa de deduplicação com base na janela configurada. */
function cleanupHashes() {
  const now = Date.now();
  const ttl = (config.settings.avoidDuplicatesWindowSec || 120) * 1000;
  for (const [k, ts] of recentHashes.entries()) {
    if (now - ts > ttl) recentHashes.delete(k);
  }
}

// ─── Substituição de link de afiliado ─────────────────────────────────────────

// Regex para detectar URLs do Mercado Livre, variantes e links curtos meli.la
const ML_URL_RE = /https?:\/\/(?:(?:www\.)?(?:mercadolivre\.com\.br|mercadolibre\.com|mliv\.com\.br)|meli\.la)[^\s"'<>\])]*/gi;

// Detecta especificamente links curtos meli.la (de outros afiliados)
const MELI_SHORT_RE = /^https?:\/\/meli\.la\//i;

// Regex para detectar URLs da Amazon
const AMAZON_URL_RE = /https?:\/\/(?:(?:www\.)?amazon\.com\.br|amzn\.to|a\.co)[^\s"'<>\])]*/gi;
const AMAZON_SHORT_RE = /^https?:\/\/(?:amzn\.to|a\.co)\//i;

// Regex para detectar URLs da Shopee
const SHOPEE_URL_RE = /https?:\/\/(?:(?:www\.)?shopee\.com\.br|shope\.ee|s\.shopee\.com\.br)[^\s"'<>\])]*/gi;
const SHOPEE_SHORT_RE = /^https?:\/\/(?:shope\.ee|s\.shopee\.com\.br)\//i;

// Parâmetros de rastreamento que devem ser removidos junto com os matt_*
const NOISE_PARAMS = ['reco_backend','reco_client','reco_item_pos','source','reco_backend_type','reco_id','tracking_id','c_id','c_uid'];

/**
 * Substitui os parâmetros de afiliado em links do Mercado Livre pelo ID do usuário.
 * Remove parâmetros matt_* e de rastreamento existentes, e injeta os do usuário.
 * Só executa se affiliateEnabled = true e affiliateMattD2id estiver preenchido.
 *
 * @param {string} text - Corpo da mensagem que pode conter links ML
 * @param {Object} settings - Configurações com affiliateMattD2id, affiliateMattTracingId
 * @returns {string} Texto com links substituídos (ou original se afiliado desativado)
 */
function replaceAffiliateLink(text, settings) {
  if (!settings.affiliateEnabled || !settings.affiliateMattD2id) return text;
  return text.replace(new RegExp(ML_URL_RE.source, 'gi'), (url) => {
    // Links meli.la são curtos — não é possível injetar params sem resolver o redirect
    if (MELI_SHORT_RE.test(url)) return url;
    try {
      const parsed = new URL(url);
      for (const key of [...parsed.searchParams.keys()]) {
        if (key.startsWith('matt_') || NOISE_PARAMS.includes(key)) parsed.searchParams.delete(key);
      }
      parsed.searchParams.set('matt_d2id', settings.affiliateMattD2id);
      if (settings.affiliateMattTracingId) parsed.searchParams.set('matt_tracing_id', settings.affiliateMattTracingId);
      parsed.searchParams.set('matt_event_ts', Date.now().toString());
      return parsed.toString();
    } catch {
      return url;
    }
  });
}

// ─── API do Mercado Livre — geração de links curtos meli.la ──────────────────

/**
 * Extrai o título do produto de um texto de mensagem de promoção.
 * Retorna a primeira linha longa sem emoji, preço ou markdown.
 * @param {string} text
 * @returns {string|null}
 */
function extractProductTitle(text) {
  const lines = text.split('\n').map(l => l.replace(/[*~_]/g, '').trim()).filter(l => l.length > 0);
  for (const line of lines) {
    if (/[\u{1F000}-\u{1FFFF}\u{2600}-\u{27FF}]/u.test(line)) continue;
    if (/^(de:|por:|r\$|https?:|compre|promoç)/i.test(line)) continue;
    if (line.length < 10) continue;
    return line;
  }
  return null;
}

/**
 * Busca um produto no Mercado Livre pela API pública de busca.
 * Retorna o permalink (URL com MLB-xxx) do primeiro resultado, ou null.
 * @param {string} query - Título ou palavras-chave do produto
 * @returns {Promise<string|null>}
 */
function searchMeliProduct(query) {
  return new Promise((resolve) => {
    try {
      const encoded = encodeURIComponent(query.slice(0, 120));
      const req = https.request({
        hostname: 'api.mercadolibre.com',
        path: `/sites/MLB/search?q=${encoded}&limit=3`,
        method: 'GET',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'application/json'
        }
      }, (res) => {
        let data = '';
        res.on('data', chunk => { data += chunk; });
        res.on('end', () => {
          try {
            const json = JSON.parse(data);
            const first = json.results?.[0];
            const permalink = first?.permalink;
            if (permalink && /MLB-?\d+/i.test(permalink)) {
              pushEvent('info', '[Busca ML] Produto encontrado por título.', {
                query: query.slice(0, 80),
                titulo: first.title?.slice(0, 80),
                permalink: permalink.slice(0, 120)
              });
              resolve(permalink);
            } else {
              resolve(null);
            }
          } catch { resolve(null); }
        });
        res.on('error', () => resolve(null));
      });
      req.setTimeout(8000, () => { req.destroy(); resolve(null); });
      req.on('error', () => resolve(null));
      req.end();
    } catch { resolve(null); }
  });
}

/**
 * Faz uma requisição HTTPS POST e retorna { status, body }.
 * @param {string} url - URL completa do endpoint
 * @param {Object} headers - Headers adicionais
 * @param {string} bodyStr - Corpo serializado (JSON string)
 * @returns {Promise<{status: number, body: any}>}
 */
function httpPost(url, headers, bodyStr) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const req = https.request({
      hostname: parsed.hostname,
      path: parsed.pathname + (parsed.search || ''),
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(bodyStr),
        ...headers
      }
    }, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
}

/**
 * Usa o browser Puppeteer já inicializado (Chrome do WhatsApp) para abrir a página social,
 * executar o JavaScript do React app e extrair a URL do produto do DOM renderizado.
 * Esta é a abordagem mais confiável pois executa o mesmo código que o browser do usuário.
 * @param {string} socialUrl - URL social do ML (com ou sem forceInApp)
 * @returns {Promise<string|null>} URL do produto (MLB-xxx) ou null
 */
async function getProductUrlFromBrowser(socialUrl) {
  if (typeof client === 'undefined' || !client.pupBrowser || status !== 'conectado') return null;
  let page = null;
  try {
    page = await client.pupBrowser.newPage();
    await page.setDefaultNavigationTimeout(20000);

    const u = new URL(socialUrl);
    u.searchParams.delete('forceInApp');
    const targetUrl = u.toString();

    pushEvent('info', '[Browser] Abrindo página social no Chrome para extrair produto...', { url: targetUrl.slice(0, 120) });

    await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });

    // Aguarda até 10s para links de produto aparecerem no DOM (React carrega os produtos)
    await page.waitForFunction(
      () => !!document.querySelector('a[href*="MLB"]'),
      { timeout: 10000 }
    ).catch(() => {});

    const productUrl = await page.evaluate(() => {
      const links = document.querySelectorAll('a[href]');
      for (const link of links) {
        const href = link.href || '';
        if (/MLB-?\d+/i.test(href) && /mercadolivre\.com\.br/.test(href)) return href;
      }
      return null;
    });

    if (productUrl) {
      pushEvent('info', '[Browser] Produto extraído via DOM renderizado.', { produto: productUrl.slice(0, 120) });
    } else {
      const pageTitle = await page.title().catch(() => '');
      pushEvent('warn', '[Browser] Nenhum link MLB encontrado no DOM.', { pageTitle });
    }
    return productUrl;
  } catch (err) {
    pushEvent('warn', '[Browser] Erro ao extrair produto:', { error: err.message?.slice(0, 120) });
    return null;
  } finally {
    if (page) await page.close().catch(() => {});
  }
}

/**
 * Faz GET com User-Agent de bot de preview (Facebook) e retorna a URL do og:url.
 * O ML serve OG meta tags com a URL do produto quando acessado por crawlers sociais.
 * Retorna a URL do produto (MLB-xxx) ou null se não encontrar.
 * @param {string} url - URL social do ML com parâmetros ref etc.
 * @returns {Promise<string|null>}
 */
function resolveWithBotUA(url) {
  return new Promise((resolve) => {
    try {
      const parsed = new URL(url);
      const mod = parsed.protocol === 'https:' ? https : http;
      const req = mod.request({
        hostname: parsed.hostname,
        path: parsed.pathname + (parsed.search || ''),
        method: 'GET',
        headers: {
          'User-Agent': 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'pt-BR,pt;q=0.9'
        }
      }, (res) => {
        const location = res.headers.location;
        if ([301, 302, 303, 307, 308].includes(res.statusCode) && location) {
          res.resume();
          let next = location;
          if (location.startsWith('//')) next = `${parsed.protocol}${location}`;
          else if (!location.startsWith('http')) next = `${parsed.protocol}//${parsed.hostname}${location}`;
          resolve(/MLB-?\d+/i.test(next) ? next : null);
          return;
        }
        let body = '';
        let done = false;
        const finish = () => {
          if (done) return;
          done = true;
          const PRODUCT_RE = /https?:\/\/(?:produto\.mercadolivre\.com\.br|www\.mercadolivre\.com\.br)\/[^\s"'<>]+MLB-?\d+[^\s"'<>]*/i;
          const ogUrl =
            body.match(/property=["']og:url["'][^>]*content=["']([^"']+)["']/i)?.[1] ||
            body.match(/content=["']([^"']+)["'][^>]*property=["']og:url["']/i)?.[1];
          const found = (ogUrl && /MLB-?\d+/i.test(ogUrl) ? ogUrl : null) ||
                        body.match(PRODUCT_RE)?.[0] || null;
          pushEvent('info', '[BotUA] Resposta do crawler:', {
            statusCode: res.statusCode,
            ogUrl: ogUrl?.slice(0, 200) || '(nenhum)',
            found: found?.slice(0, 200) || '(nenhum)',
            headPreview: body.slice(0, 600)
          });
          resolve(found);
        };
        res.setEncoding('utf8');
        res.on('data', chunk => { body += chunk; if (body.length > 80000) { res.destroy(); finish(); } });
        res.on('end', finish);
        res.on('close', finish);
        res.on('error', () => resolve(null));
      });
      req.setTimeout(10000, () => { req.destroy(); resolve(null); });
      req.on('error', () => resolve(null));
      req.end();
    } catch { resolve(null); }
  });
}

/**
 * Resolve um link curto meli.la seguindo o redirecionamento HTTP.
 * Retorna a URL final (ex: mercadolivre.com.br/...) ou a URL original se falhar.
 * @param {string} url - Link curto meli.la
 * @returns {Promise<string>}
 */
function resolveShortLink(url, useGet = false) {
  return new Promise((resolve) => {
    try {
      const parsed = new URL(url);
      const mod = parsed.protocol === 'https:' ? https : http;
      const isSocialUrl = /\/social\//i.test(parsed.pathname);
      const req = mod.request({
        hostname: parsed.hostname,
        path: parsed.pathname + (parsed.search || ''),
        method: useGet ? 'GET' : 'HEAD',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          'Accept': isSocialUrl ? 'application/json, text/html,application/xhtml+xml,*/*;q=0.8' : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'pt-BR,pt;q=0.9',
          ...(isSocialUrl ? { 'X-Requested-With': 'XMLHttpRequest' } : {})
        }
      }, (res) => {
        const location = res.headers.location;
        if ([301, 302, 303, 307, 308].includes(res.statusCode) && location) {
          if (useGet) res.resume();
          let next = location;
          if (location.startsWith('//')) next = `${parsed.protocol}${location}`;
          else if (!location.startsWith('http')) next = `${parsed.protocol}//${parsed.hostname}${location}`;
          resolve(next);
          return;
        }

        if (!useGet) { resolve(url); return; }

        // GET com 200: lê até 300KB do HTML para encontrar URL do produto
        let body = '';
        let resolved = false;
        const processBody = () => {
          if (resolved) return;
          resolved = true;
          const PRODUCT_RE = /https?:\/\/(?:produto\.mercadolivre\.com\.br|www\.mercadolivre\.com\.br)\/[^\s"'<>]+MLB-?\d+[^\s"'<>]*/i;
          // Verifica se a resposta é JSON (ML pode retornar JSON para social URLs)
          const contentType = res.headers['content-type'] || '';
          let jsonBody = null;
          if (contentType.includes('application/json')) {
            try { jsonBody = JSON.parse(body); } catch {}
          }

          const attempts = [
            // Se recebemos JSON, procura por permalink/url com MLB-
            jsonBody && (() => {
              const s = JSON.stringify(jsonBody);
              return s.match(/"permalink"\s*:\s*"(https?:[^"]+MLB-\d+[^"]*)"/)?.[1] ||
                     s.match(/"url"\s*:\s*"(https?:[^"]+MLB-\d+[^"]*)"/)?.[1] ||
                     s.match(PRODUCT_RE)?.[0];
            })(),
            // Meta tags padrão
            body.match(/property=["']og:url["'][^>]*content=["']([^"']+)["']/i)?.[1],
            body.match(/content=["']([^"']+)["'][^>]*property=["']og:url["']/i)?.[1],
            body.match(/rel=["']canonical["'][^>]*href=["']([^"']+)["']/i)?.[1],
            // __NEXT_DATA__ (Next.js) — busca o bloco JSON completo
            (() => {
              const nd = body.match(/__NEXT_DATA__[^>]*>([\s\S]{1,50000}?)<\/script>/i)?.[1];
              if (!nd) return null;
              return nd.match(/"permalink"\s*:\s*"(https?:[^"]+MLB-\d+[^"]*)"/)?.[1] ||
                     nd.match(/"url"\s*:\s*"(https?:[^"]+MLB-\d+[^"]*)"/)?.[1] ||
                     nd.match(PRODUCT_RE)?.[0];
            })(),
            // window.__INITIAL_STATE__ / __PRELOADED_STATE__ (outros frameworks React)
            (() => {
              const ws = body.match(/window\.__(?:INITIAL|PRELOADED)_STATE__\s*=\s*([^<]{1,50000})/i)?.[1];
              if (!ws) return null;
              return ws.match(/"permalink"\s*:\s*"(https?:[^"]+MLB-\d+[^"]*)"/)?.[1] ||
                     ws.match(PRODUCT_RE)?.[0];
            })(),
            // React state JSON — permalink, itemUrl, productUrl, url
            body.match(/"permalink"\s*:\s*"(https?:\\?\/\\?\/[^"\\]+MLB-?\d+[^"\\]*)"/i)?.[1]?.replace(/\\\//g, '/'),
            body.match(/"itemUrl"\s*:\s*"(https?:\\?\/\\?\/[^"\\]+MLB-?\d+[^"\\]*)"/i)?.[1]?.replace(/\\\//g, '/'),
            body.match(/"productUrl"\s*:\s*"(https?:\\?\/\\?\/[^"\\]+MLB-?\d+[^"\\]*)"/i)?.[1]?.replace(/\\\//g, '/'),
            body.match(/"url"\s*:\s*"(https?:\\?\/\\?\/(?:www|produto)\.mercadolivre\.com\.br[^"\\]*MLB-?\d+[^"\\]*)"/i)?.[1]?.replace(/\\\//g, '/'),
            // href em links HTML
            body.match(/href=["'](https?:\/\/(?:www|produto)\.mercadolivre\.com\.br\/[^"']*MLB-\d+[^"']*)["']/i)?.[1],
            // JSON-LD schema.org
            body.match(/"@type"\s*:\s*"Product"[^}]*"url"\s*:\s*"([^"]+MLB-\d+[^"]+)"/i)?.[1],
            // window.location ou url: "..."
            body.match(/(?:window\.location(?:\.href)?\s*=|url\s*:\s*)["']([^"']+MLB-\d+[^"']*)["']/i)?.[1],
            // Qualquer URL de produto ML no HTML (fallback)
            body.match(PRODUCT_RE)?.[0],
          ];
          const found = attempts.find(u => u && /MLB-?\d+/i.test(u));
          if (!found && /\/social\//.test(url)) {
            const mid = Math.floor(body.length / 2);
            pushEvent('warn', '[Resolve HTML] Produto não encontrado. Preview HTML:', {
              statusCode: res.statusCode,
              contentType: contentType.slice(0, 60),
              totalBytes: body.length,
              head: body.slice(0, 800),
              mid: body.slice(mid, mid + 800),
              tail: body.slice(-3000)
            });
          }
          resolve(found || url);
        };
        res.setEncoding('utf8');
        res.on('data', chunk => { body += chunk; if (body.length > 300000) { res.destroy(); processBody(); } });
        res.on('end', processBody);
        res.on('close', processBody);
        res.on('error', processBody);
      });
      req.setTimeout(10000, () => { req.destroy(); resolve(url); });
      req.on('error', () => resolve(url));
      req.end();
    } catch {
      resolve(url);
    }
  });
}

/**
 * Segue todos os redirecionamentos de uma URL até chegar na URL final (máx 5 saltos).
 * Usa GET para meli.la (responde 200 a HEAD mas 302 a GET) e HEAD para demais URLs.
 * @param {string} url - URL inicial (ex: https://meli.la/xxxx)
 * @returns {Promise<string>} URL final ou URL original se falhar
 */
async function resolveUrlFully(url, hops = 0) {
  if (hops >= 5) return url;
  const useGet = MELI_SHORT_RE.test(url) || /\/social\//i.test(url);

  // Para URLs sociais: tenta extrair a URL do produto por diferentes métodos.
  if (/\/social\//i.test(url)) {
    // Limpa params matt_* de uma URL de produto antes de retornar
    const cleanProductUrl = (u) => {
      try {
        const parsed = new URL(u);
        for (const k of [...parsed.searchParams.keys()]) {
          if (k.startsWith('matt_') || NOISE_PARAMS.includes(k)) parsed.searchParams.delete(k);
        }
        return parsed.toString();
      } catch { return u; }
    };

    // Tentativa A: Puppeteer (Chrome já rodando) — executa o React app e lê o DOM renderizado
    const productUrlBrowser = await getProductUrlFromBrowser(url);
    if (productUrlBrowser && /MLB-?\d+/i.test(productUrlBrowser)) return cleanProductUrl(productUrlBrowser);

    // Tentativa B: BotUA com forceInApp
    const productUrlA = await resolveWithBotUA(url);
    if (productUrlA && /MLB-?\d+/i.test(productUrlA)) return cleanProductUrl(productUrlA);

    // Tentativa C: BotUA sem forceInApp
    try {
      const u = new URL(url);
      u.searchParams.delete('forceInApp');
      const productUrlB = await resolveWithBotUA(u.toString());
      if (productUrlB && /MLB-?\d+/i.test(productUrlB)) return cleanProductUrl(productUrlB);
    } catch {}
  }

  // Fallback: resolve via redirect HTTP normal
  let resolveTarget = url;
  if (/\/social\//i.test(url)) {
    try {
      const u = new URL(url);
      u.searchParams.delete('forceInApp');
      resolveTarget = u.toString();
    } catch {}
  }

  const next = await resolveShortLink(resolveTarget, useGet);
  if (next === resolveTarget || next === url) return url;
  return resolveUrlFully(next, hops + 1);
}

/**
 * Aplica os parâmetros de afiliado do usuário em uma URL social do ML resolvida.
 * Substitui matt_word e matt_tool pelo afiliado do usuário.
 * @param {string} resolvedUrl - URL completa do ML (produto ou social)
 * @param {Object} settings - Settings com affiliateApiTag e affiliateMattTool
 * @returns {string|null} URL modificada ou null se não for aplicável
 */
function applyAffiliateToResolvedUrl(resolvedUrl, settings) {
  try {
    const parsed = new URL(resolvedUrl);
    const isSocial = parsed.pathname.startsWith('/social/');
    const isProduct = parsed.hostname.includes('mercadolivre') || parsed.hostname.includes('mercadolibre');

    if (!isProduct) return null;

    if (isSocial) {
      // Substitui o path /social/OUTRO_AFILIADO pelo do usuário
      if (settings.affiliateSocialId) parsed.pathname = `/social/${settings.affiliateSocialId}`;
      // Troca os parâmetros do afiliado original pelos do usuário
      if (settings.affiliateApiTag) parsed.searchParams.set('matt_word', settings.affiliateApiTag);
      if (settings.affiliateMattTool) parsed.searchParams.set('matt_tool', settings.affiliateMattTool);
      // Remove o ref: ele é criptografado para o perfil original e causa /lists no perfil do usuário
      parsed.searchParams.delete('ref');
    } else if (settings.affiliateMattD2id) {
      // Link de produto direto: injeta matt_* params
      for (const key of [...parsed.searchParams.keys()]) {
        if (key.startsWith('matt_') || NOISE_PARAMS.includes(key)) parsed.searchParams.delete(key);
      }
      parsed.searchParams.set('matt_d2id', settings.affiliateMattD2id);
      if (settings.affiliateMattTracingId) parsed.searchParams.set('matt_tracing_id', settings.affiliateMattTracingId);
      parsed.searchParams.set('matt_event_ts', Date.now().toString());
    } else {
      return null;
    }
    return parsed.toString();
  } catch {
    return null;
  }
}

/**
 * Chama a API de afiliados do Mercado Livre para gerar um link curto meli.la.
 * Autentica via cookie ssid extraído do token base64 configurado.
 * Retorna null em caso de erro ou resposta inesperada.
 *
 * @param {string} originalUrl - URL original do Mercado Livre
 * @param {Object} settings - Settings com affiliateApiToken e affiliateApiTag
 * @returns {Promise<string|null>} URL curta (https://meli.la/...) ou null
 */
async function generateMeliShortLink(originalUrl, settings) {
  if (!settings.affiliateApiToken || !settings.affiliateApiTag) return null;
  try {
    const decoded = Buffer.from(settings.affiliateApiToken, 'base64').toString('utf8');
    const ssidMatch = decoded.match(/ssid=(.+)/);
    const ssid = ssidMatch ? ssidMatch[1].trim() : decoded.trim();

    const bodyStr = JSON.stringify({ urls: [originalUrl], tag: settings.affiliateApiTag });
    const result = await httpPost('https://www.mercadolivre.com.br/affiliate-program/api/v2/affiliates/createLink', {
      'Cookie': `ssid=${ssid}`,
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Origin': 'https://www.mercadolivre.com.br',
      'Referer': 'https://www.mercadolivre.com.br/affiliate-program/links'
    }, bodyStr);

    const bodyPreview = typeof result.body === 'object'
      ? JSON.stringify(result.body).slice(0, 500)
      : String(result.body).slice(0, 500);
    pushEvent('info', `[API ML] status=${result.status}`, { url: originalUrl.slice(0, 80), resposta: bodyPreview });

    if ((result.status === 200 || result.status === 201) && typeof result.body === 'object') {
      const urlObj = result.body.urls?.[0];
      const short = urlObj?.short_url;
      if (short) {
        // Auto-detecta o ID do perfil social a partir do long_url
        const longUrl = urlObj?.long_url;
        if (longUrl && !config.settings.affiliateSocialId) {
          try {
            const m = longUrl.match(/\/social\/([^?/]+)/);
            if (m) {
              config.settings.affiliateSocialId = m[1];
              saveConfig();
              pushEvent('info', `[Afiliado] ID social detectado automaticamente: ${m[1]}`);
            }
          } catch {}
        }
        return String(short);
      }
    }
    return null;
  } catch (err) {
    pushEvent('warn', '[API ML] Erro na chamada', { error: err.message, url: originalUrl.slice(0, 80) });
    return null;
  }
}

// Aplica tag de afiliado Amazon em uma URL de produto.
// Resolve links curtos (amzn.to) antes de injetar o parâmetro tag.
async function applyAmazonAffiliate(url, settings) {
  if (!settings.amazonTag) return url;
  let productUrl = url;
  if (AMAZON_SHORT_RE.test(url)) {
    const resolved = await resolveShortLink(url, false).catch(() => null);
    if (resolved && resolved !== url) productUrl = resolved;
  }
  try {
    const parsed = new URL(productUrl);
    parsed.searchParams.set('tag', settings.amazonTag);
    ['linkCode', 'linkId', 'ref', 'ref_'].forEach(k => parsed.searchParams.delete(k));
    return parsed.toString();
  } catch { return url; }
}

// Aplica parâmetros de afiliado Shopee em uma URL de produto.
// Resolve links curtos (shope.ee) antes de injetar os parâmetros.
async function applyShopeeAffiliate(url, settings) {
  if (!settings.shopeeAffiliateId) return url;
  let productUrl = url;
  if (SHOPEE_SHORT_RE.test(url)) {
    const resolved = await resolveShortLink(url, false).catch(() => null);
    if (resolved && resolved !== url) productUrl = resolved;
  }
  try {
    const parsed = new URL(productUrl);
    parsed.searchParams.set('utm_source', 'affiliates');
    parsed.searchParams.set('utm_medium', 'deep_link');
    parsed.searchParams.set('utm_campaign', `AFF_${settings.shopeeAffiliateId}`);
    if (settings.shopeeSubSource) parsed.searchParams.set('utm_content', settings.shopeeSubSource);
    return parsed.toString();
  } catch { return url; }
}

// Substitui todos os links Amazon em um texto com a tag de afiliado configurada.
async function replaceAmazonAffiliateLinkAsync(text, settings) {
  if (!settings.amazonEnabled || !settings.amazonTag) return { text, swapped: false };
  const urls = [...new Set((text.match(new RegExp(AMAZON_URL_RE.source, 'gi')) || []))];
  if (urls.length === 0) return { text, swapped: false };
  let resultText = text;
  let swapped = false;
  for (const url of urls) {
    const replaced = await applyAmazonAffiliate(url, settings);
    if (replaced !== url) {
      resultText = resultText.split(url).join(replaced);
      swapped = true;
      pushEvent('info', '[Amazon] Link de afiliado aplicado.', { original: url.slice(0, 80), resultado: replaced.slice(0, 120) });
    }
  }
  return { text: resultText, swapped };
}

// Substitui todos os links Shopee em um texto com os parâmetros de afiliado configurados.
async function replaceShopeeAffiliateLinkAsync(text, settings) {
  if (!settings.shopeeEnabled || !settings.shopeeAffiliateId) return { text, swapped: false };
  const urls = [...new Set((text.match(new RegExp(SHOPEE_URL_RE.source, 'gi')) || []))];
  if (urls.length === 0) return { text, swapped: false };
  let resultText = text;
  let swapped = false;
  for (const url of urls) {
    const replaced = await applyShopeeAffiliate(url, settings);
    if (replaced !== url) {
      resultText = resultText.split(url).join(replaced);
      swapped = true;
      pushEvent('info', '[Shopee] Link de afiliado aplicado.', { original: url.slice(0, 80), resultado: replaced.slice(0, 120) });
    }
  }
  return { text: resultText, swapped };
}

/**
 * Versão assíncrona da substituição de links de afiliado.
 * Tenta gerar links meli.la via API do ML (se token+tag configurados).
 * Se a API falhar por qualquer URL, cai no modo matt_* como fallback.
 *
 * @param {string} text - Texto da mensagem
 * @param {Object} settings - Configurações de afiliado
 * @returns {Promise<{text: string, swapped: boolean, mode: 'api'|'matt'|'none'}>}
 */
async function replaceAffiliateLinkAsync(text, settings) {
  if (!settings.affiliateEnabled) return { text, swapped: false, mode: 'none' };

  const useApi = !!(settings.affiliateApiToken && settings.affiliateApiTag);
  const urls = [...new Set((text.match(new RegExp(ML_URL_RE.source, 'gi')) || []))];

  if (urls.length === 0) return { text, swapped: false, mode: useApi ? 'api' : 'matt' };

  let resultText = text;
  let swapped = false;

  if (useApi) {
    for (const url of urls) {
      let urlForApi = url;

      if (MELI_SHORT_RE.test(url)) {
        // Tentativa 1: passa o meli.la direto para a API v2 (ela pode resolver internamente)
        const shortUrlDirect = await generateMeliShortLink(url, settings);
        if (shortUrlDirect) {
          resultText = resultText.split(url).join(shortUrlDirect);
          swapped = true;
          pushEvent('info', '[Afiliado] meli.la aceito direto pela API.', { original: url.slice(0, 80), resultado: shortUrlDirect });
          continue;
        }

        // Tentativa 2: resolve o meli.la e passa a URL resultante para a API
        const resolved = await resolveUrlFully(url);
        if (resolved !== url) urlForApi = resolved;
        pushEvent('info', '[Afiliado] meli.la resolvido para API.', { original: url.slice(0, 80), resolvido: urlForApi.slice(0, 120) });
      }

      const shortUrl = await generateMeliShortLink(urlForApi, settings);
      if (shortUrl) {
        resultText = resultText.split(url).join(shortUrl);
        swapped = true;
        continue;
      }

      // API falhou com URL social → tenta encontrar a URL do produto pelo título da mensagem
      if (/\/social\//i.test(urlForApi)) {
        const title = extractProductTitle(resultText);
        if (title) {
          const productUrl = await searchMeliProduct(title);
          if (productUrl) {
            const shortBySearch = await generateMeliShortLink(productUrl, settings);
            if (shortBySearch) {
              resultText = resultText.split(url).join(shortBySearch);
              swapped = true;
              pushEvent('success', '[Afiliado] Link gerado via busca de título.', {
                titulo: title.slice(0, 80), produto: productUrl.slice(0, 120), short: shortBySearch
              });
              continue;
            }
          }
        }
      }

      // Fallback final — aplica parâmetros de afiliado diretamente na URL resolvida (sem ref)
      const affiliateUrl = applyAffiliateToResolvedUrl(urlForApi, settings);
      if (affiliateUrl && affiliateUrl !== urlForApi) {
        resultText = resultText.split(url).join(affiliateUrl);
        swapped = true;
        pushEvent('info', '[Afiliado] Fallback: parâmetros trocados na URL resolvida.', {
          original: url.slice(0, 80), resultado: affiliateUrl.slice(0, 120)
        });
        continue;
      }

      if (settings.affiliateMattD2id) {
        const mattUrl = replaceAffiliateLink(urlForApi, settings);
        if (mattUrl !== urlForApi) {
          resultText = resultText.split(url).join(mattUrl);
          swapped = true;
          continue;
        }
      }
      pushEvent('warn', '[Afiliado] Não foi possível trocar link de afiliado.', { url: url.slice(0, 80) });
    }
    return { text: resultText, swapped, mode: 'api' };
  }

  const replaced = replaceAffiliateLink(text, settings);
  return { text: replaced, swapped: replaced !== text, mode: 'matt' };
}

// Processa afiliados de todas as plataformas ativas em um texto.
// Retorna o texto com todos os links substituídos e indicador de swap por plataforma.
async function replaceAllAffiliateLinks(text, settings, platforms) {
  const activePlatforms = Array.isArray(platforms) && platforms.length > 0 ? platforms : ['ml'];
  let resultText = text;
  const swappedBy = {};

  if (activePlatforms.includes('ml')) {
    const ml = await replaceAffiliateLinkAsync(resultText, settings);
    resultText = ml.text;
    swappedBy.ml = ml.swapped;
  }
  if (activePlatforms.includes('amazon')) {
    const amz = await replaceAmazonAffiliateLinkAsync(resultText, settings);
    resultText = amz.text;
    swappedBy.amazon = amz.swapped;
  }
  if (activePlatforms.includes('shopee')) {
    const shp = await replaceShopeeAffiliateLinkAsync(resultText, settings);
    resultText = shp.text;
    swappedBy.shopee = shp.swapped;
  }

  const swapped = Object.values(swappedBy).some(Boolean);
  return { text: resultText, swapped, swappedBy };
}

// ─── Cliente WhatsApp (whatsapp-web.js + Puppeteer) ───────────────────────────

function findChromePath() {
  const candidates = process.platform === 'win32'
    ? [
        'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
        'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
        (process.env.LOCALAPPDATA || '') + '\\Google\\Chrome\\Application\\chrome.exe'
      ]
    : [
        '/usr/bin/google-chrome-stable',
        '/usr/bin/google-chrome',
        '/usr/bin/chromium-browser',
        '/usr/bin/chromium',
        '/snap/bin/chromium'
      ];
  return candidates.find(p => { try { return fs.existsSync(p); } catch { return false; } });
}

const chromePath = findChromePath();
if (chromePath) console.log(`[Chrome] Usando: ${chromePath}`);
else console.log('[Chrome] Nenhum executável encontrado — usando Chromium embutido do Puppeteer.');

const client = new Client({
  authStrategy: new LocalAuth({ dataPath: SESSION_DIR }),
  puppeteer: {
    headless: true,
    timeout: 60000,
    ...(chromePath ? { executablePath: chromePath } : {}),
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--no-first-run',
      '--no-default-browser-check',
      '--disable-default-apps',
      '--mute-audio',
      '--window-size=1280,720',
      '--disable-background-networking',
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding',
      '--disable-extensions',
      '--disable-sync',
      '--disable-translate',
      '--metrics-recording-only',
      '--safebrowsing-disable-auto-update'
    ]
  }
});

// ─── Express + Socket.io ──────────────────────────────────────────────────────

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ─── Autenticação ─────────────────────────────────────────────────────────────

const AUTH_USER = 'admin';
const AUTH_PASS_HASH = crypto.createHash('sha256').update('monkey@10').digest('hex');
const AUTH_SESSIONS = new Map(); // token -> expiresAt
const SESSION_TTL = 24 * 60 * 60 * 1000; // 24 horas

function isValidToken(token) {
  if (!token) return false;
  const exp = AUTH_SESSIONS.get(token);
  if (!exp) return false;
  if (Date.now() > exp) { AUTH_SESSIONS.delete(token); return false; }
  return true;
}

function requireAuth(req, res, next) {
  if (isValidToken(req.headers['x-auth-token'])) return next();
  res.status(401).json({ error: 'Não autenticado.' });
}

app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body || {};
  const hash = crypto.createHash('sha256').update(password || '').digest('hex');
  if (username !== AUTH_USER || hash !== AUTH_PASS_HASH) {
    return res.status(401).json({ error: 'Usuário ou senha incorretos.' });
  }
  const token = crypto.randomBytes(32).toString('hex');
  AUTH_SESSIONS.set(token, Date.now() + SESSION_TTL);
  res.json({ ok: true, token });
});

app.post('/api/auth/logout', (req, res) => {
  AUTH_SESSIONS.delete(req.headers['x-auth-token']);
  res.json({ ok: true });
});

app.get('/api/auth/check', (req, res) => {
  res.json({ authenticated: isValidToken(req.headers['x-auth-token']) });
});

// Protege todas as demais rotas /api/*
app.use('/api', requireAuth);

// ─── Eventos do cliente WhatsApp ──────────────────────────────────────────────

/**
 * Dispara quando o WhatsApp Web gera um novo QR Code para autenticação.
 * Converte o QR para imagem base64 e transmite para todos os clientes do painel.
 */
client.on('qr', async (qr) => {
  latestQr = await qrcode.toDataURL(qr);
  status = 'aguardando_qr';
  io.emit('status', getStatus());
  pushEvent('info', 'QR Code gerado. Escaneie com o WhatsApp.');
});

/**
 * Dispara quando o WhatsApp está totalmente conectado e pronto para uso.
 * Carrega grupos com até 3 tentativas espaçadas (getChats pode falhar logo após ready).
 */
client.on('ready', async () => {
  status = 'conectado';
  latestQr = null;
  lastPollTimes = {};
  reconnectAttempts = 0;
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  try {
    me = client.info?.wid?._serialized || null;
  } catch {}
  io.emit('status', getStatus());
  pushEvent('success', 'WhatsApp conectado com sucesso.');

  // 1ª carga: 2s após ready (rápida, pega grupos ativos)
  setTimeout(async () => {
    if (status !== 'conectado') return; // abortou antes de carregar
    try {
      cachedGroups = await loadGroups();
      io.emit('groups', cachedGroups);
      pushEvent('info', `${cachedGroups.length} grupo(s) carregado(s).`);
      // Após carregar grupos, faz varredura imediata para registrar baseline de mensagens
      if (config.settings.scheduleEnabled) pollSourceGroups();
    } catch (err) {
      if (status === 'conectado') {
        pushEvent('warn', 'Primeira carga de grupos falhou, aguardando 2ª tentativa...', { error: err.message });
      }
    }
  }, 2000);

  // 2ª carga: 8s após ready (merge — pega grupos que o WhatsApp carregou atrasado)
  setTimeout(async () => {
    if (status !== 'conectado') return;
    try {
      const merged = await loadGroups(true);
      if (merged.length >= cachedGroups.length) {
        cachedGroups = merged;
        io.emit('groups', cachedGroups);
        if (cachedGroups.length > 0) {
          pushEvent('info', `Grupos atualizados: ${cachedGroups.length} no total.`);
        }
      }
    } catch { /* silencioso — a 1ª carga já resolveu */ }
  }, 8000);

  // 3ª carga: 20s após ready (última tentativa para grupos lentos)
  setTimeout(async () => {
    if (status !== 'conectado' || cachedGroups.length > 0) return;
    try {
      cachedGroups = await loadGroups();
      if (cachedGroups.length > 0) {
        io.emit('groups', cachedGroups);
        pushEvent('info', `Grupos carregados na 3ª tentativa: ${cachedGroups.length}.`);
      }
    } catch { /* silencioso */ }
  }, 20000);
});

/** Dispara durante o processo de autenticação (antes de ready). */
client.on('authenticated', () => {
  status = 'autenticado';
  io.emit('status', getStatus());
  pushEvent('success', 'Sessão autenticada.');
});

/** Dispara quando a autenticação falha (ex: sessão inválida). */
client.on('auth_failure', (msg) => {
  status = 'falha_auth';
  io.emit('status', getStatus());
  pushEvent('error', 'Falha de autenticação.', { msg });
});

/** Dispara quando o WhatsApp desconecta (logout, perda de internet, etc.). */
client.on('disconnected', (reason) => {
  status = 'desconectado';
  cachedGroups = [];
  io.emit('status', getStatus());
  io.emit('groups', []);
  pushEvent('error', 'WhatsApp desconectado.', { reason });
  if (reason !== 'LOGOUT') scheduleReconnect();
});

// Listener em tempo real — complementa o polling periódico capturando mensagens ao vivo.
client.on('message_create', async (message) => {
  try {
    const chat = await message.getChat();
    if (!chat?.isGroup && !chat?.isChannel) return;
    await processMessageAgainstRules(message, chat);
  } catch (err) {
    pushEvent('error', 'Erro ao processar mensagem.', { error: err.message });
  }
});

// ─── Funções auxiliares de API ────────────────────────────────────────────────

/**
 * Retorna o objeto de status atual para enviar ao frontend via API ou Socket.io.
 */
function getStatus() {
  return {
    status,
    qr: latestQr,
    me,
    settings: config.settings,
    rulesCount: config.rules.length,
    scheduleRunning: runningSchedule
  };
}

/**
 * Busca todos os grupos do WhatsApp.
 * Se merge=true, combina com cachedGroups para não perder grupos já carregados.
 * @param {boolean} merge - Se true, une com o cache atual antes de retornar
 * @returns {Promise<Array<{id: string, name: string}>>}
 */
async function loadGroups(merge = false) {
  if (status !== 'conectado') throw new Error('Cliente não está conectado.');
  const chats = await client.getChats();
  const fresh = chats
    .filter(c => c.isGroup || c.isChannel)
    .map(c => ({
      id: c.id._serialized,
      name: c.name || c.id.user || c.id._serialized,
      type: c.isChannel ? 'channel' : 'group'
    }));

  const base = merge ? [...cachedGroups, ...fresh] : fresh;
  const byId = new Map(base.map(g => [g.id, g]));
  return [...byId.values()].sort((a, b) => {
    if (a.type !== b.type) return a.type === 'group' ? -1 : 1; // grupos antes de canais
    return a.name.localeCompare(b.name, 'pt-BR');
  });
}

// ─── Rotas da API REST ────────────────────────────────────────────────────────

/** Retorna o status atual da conexão WhatsApp, QR code e configurações. */
app.get('/api/status', (req, res) => res.json(getStatus()));

/** Retorna os últimos 200 eventos de log. */
app.get('/api/events', (req, res) => res.json(lastEvents));

/** Retorna a configuração completa (settings + regras). */
app.get('/api/config', (req, res) => res.json(config));

/**
 * Lista todos os grupos do WhatsApp ordenados por nome.
 * Usa o cache se disponível, caso contrário busca do cliente.
 */
app.get('/api/groups', async (req, res) => {
  try {
    if (cachedGroups.length > 0) {
      return res.json(cachedGroups);
    }
    cachedGroups = await loadGroups();
    res.json(cachedGroups);
  } catch (err) {
    res.status(500).json({ error: 'Não foi possível listar grupos. Verifique se o WhatsApp está conectado.', details: err.message });
  }
});

/**
 * Força recarregamento da lista de grupos (ignora cache).
 * Emite o evento 'groups' via Socket.io para atualizar todos os clientes.
 */
app.post('/api/groups/refresh', async (req, res) => {
  try {
    cachedGroups = await loadGroups();
    io.emit('groups', cachedGroups);
    res.json(cachedGroups);
  } catch (err) {
    res.status(500).json({ error: 'Não foi possível atualizar grupos.', details: err.message });
  }
});

/**
 * Salva as configurações gerais do sistema.
 * Aceita: simulationMode, ignoreOwnMessages, avoidDuplicatesWindowSec,
 *         affiliateEnabled, affiliateMattD2id, affiliateMattTracingId, affiliateApiToken,
 *         affiliateApiTag, affiliateMattTool
 */
app.post('/api/settings', (req, res) => {
  const incoming = req.body || {};
  config.settings = {
    ...config.settings,
    ignoreOwnMessages: !!incoming.ignoreOwnMessages,
    simulationMode: !!incoming.simulationMode,
    avoidDuplicatesWindowSec: Math.max(10, Number(incoming.avoidDuplicatesWindowSec || 120)),
    affiliateEnabled: !!incoming.affiliateEnabled,
    affiliateMattD2id: String(incoming.affiliateMattD2id || '').trim(),
    affiliateMattTracingId: String(incoming.affiliateMattTracingId || '').trim(),
    affiliateApiToken: String(incoming.affiliateApiToken || '').trim(),
    affiliateApiTag: String(incoming.affiliateApiTag || '').trim(),
    affiliateMattTool: String(incoming.affiliateMattTool || '').trim(),
    affiliateSocialId: String(incoming.affiliateSocialId || '').trim(),
    scheduleEnabled: !!incoming.scheduleEnabled,
    scheduleStartTime: String(incoming.scheduleStartTime || '08:00').trim(),
    scheduleEndTime: String(incoming.scheduleEndTime || '22:00').trim(),
    scheduleCheckIntervalMinutes: Math.max(1, Number(incoming.scheduleCheckIntervalMinutes || 1)),
    scheduleDispatchIntervalMinutes: Math.max(0, Number(incoming.scheduleDispatchIntervalMinutes || 5)),
    amazonEnabled: !!incoming.amazonEnabled,
    amazonTag: String(incoming.amazonTag || '').trim(),
    shopeeEnabled: !!incoming.shopeeEnabled,
    shopeeAffiliateId: String(incoming.shopeeAffiliateId || '').trim(),
    shopeeSubSource: String(incoming.shopeeSubSource || '').trim()
  };
  saveConfig();
  startScheduleTimer();
  io.emit('status', getStatus());
  io.emit('queue', { count: messageQueue.length, lastDispatchTime });
  res.json({ ok: true, settings: config.settings });
});

app.post('/api/schedule/start', (req, res) => {
  if (!config.settings.scheduleEnabled) return res.status(400).json({ error: 'Agenda desativada nas configurações.' });
  runningSchedule = true;
  io.emit('scheduleState', { running: true });
  pushEvent('success', '[Agenda] Iniciada manualmente. Buscando promoções...');
  pollSourceGroups();
  res.json({ ok: true, running: true });
});

app.post('/api/schedule/pause', (req, res) => {
  runningSchedule = false;
  io.emit('scheduleState', { running: false });
  pushEvent('warn', '[Agenda] Pausada manualmente.');
  res.json({ ok: true, running: false });
});

app.get('/api/schedule/state', (req, res) => {
  res.json({ running: runningSchedule, scheduleEnabled: config.settings.scheduleEnabled });
});

app.get('/api/queue', (req, res) => {
  const dispatchIntervalMs = Math.max(0, config.settings.scheduleDispatchIntervalMinutes || 0) * 60 * 1000;
  res.json({
    count: messageQueue.length,
    lastDispatchTime,
    dispatchIntervalMs,
    inWindow: isWithinScheduleWindow(config.settings),
    scheduleEnabled: config.settings.scheduleEnabled,
    scheduleStart: config.settings.scheduleStartTime || '',
    scheduleEnd: config.settings.scheduleEndTime || '',
    items: messageQueue.map(i => ({ rule: i.rule.name, sourceName: i.sourceName, addedAt: i.addedAt, preview: i.outgoing.slice(0, 200) }))
  });
});

app.delete('/api/queue', (req, res) => {
  const count = messageQueue.length;
  messageQueue = [];
  io.emit('queue', { count: 0, lastDispatchTime });
  pushEvent('info', `[Agenda] Fila limpa manualmente. ${count} mensagens removidas.`);
  res.json({ ok: true, removed: count });
});

app.post('/api/queue/process', async (req, res) => {
  if (status !== 'conectado') return res.status(400).json({ error: 'WhatsApp não conectado.' });
  if (messageQueue.length === 0) return res.json({ ok: true, msg: 'Fila vazia.' });
  await processQueue(true);
  res.json({ ok: true, queueRemaining: messageQueue.length });
});

app.post('/api/queue/poll', async (req, res) => {
  if (status !== 'conectado') return res.status(400).json({ error: 'WhatsApp não conectado.' });
  await pollSourceGroups();
  res.json({ ok: true, queueCount: messageQueue.length });
});

/** Retorna a última promoção capturada por plataforma para cada regra. */
app.get('/api/rules/last-by-platform', (req, res) => {
  const result = {};
  for (const rule of config.rules) {
    const entry = lastByPlatform[rule.id] || {};
    result[rule.id] = {};
    for (const plat of ['ml', 'amazon', 'shopee']) {
      result[rule.id][plat] = entry[plat]
        ? { at: entry[plat].at, preview: entry[plat].outgoing.slice(0, 120) }
        : null;
    }
  }
  res.json(result);
});

/**
 * Cria uma nova regra de encaminhamento.
 * Campos obrigatórios: name, sourceGroupId, targetGroupId, matchType, pattern
 * Campos opcionais: flags, prefix, suffix, active
 */
app.post('/api/rules', (req, res) => {
  const rule = req.body || {};
  const targetGroupIds = Array.isArray(rule.targetGroupIds) && rule.targetGroupIds.length > 0
    ? rule.targetGroupIds.filter(id => id && typeof id === 'string')
    : rule.targetGroupId ? [String(rule.targetGroupId)] : [];
  if (!rule.name || !rule.sourceGroupId || targetGroupIds.length === 0 || !rule.matchType || !rule.pattern) {
    return res.status(400).json({ error: 'Campos obrigatórios ausentes: name, sourceGroupId, targetGroupIds, matchType, pattern' });
  }
  const item = {
    id: Date.now().toString(),
    name: String(rule.name),
    sourceGroupId: String(rule.sourceGroupId),
    targetGroupIds,
    matchType: String(rule.matchType),
    pattern: String(rule.pattern),
    flags: String(rule.flags || 'i'),
    prefix: String(rule.prefix || ''),
    suffix: String(rule.suffix || ''),
    active: rule.active !== false,
    platforms: Array.isArray(rule.platforms) && rule.platforms.length > 0
      ? rule.platforms.filter(p => ['ml', 'amazon', 'shopee'].includes(p))
      : ['ml']
  };
  config.rules.push(item);
  saveConfig();
  res.json({ ok: true, item });
});

/** Retorna estatísticas de envio dos últimos 30 dias. */
app.get('/api/stats', (req, res) => {
  const today = new Date().toISOString().slice(0, 10);
  const last7 = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(); d.setDate(d.getDate() - i);
    const key = d.toISOString().slice(0, 10);
    last7.push({ date: key, ...(stats.daily[key] || { total: 0, ml: 0, amazon: 0, shopee: 0 }) });
  }
  res.json({
    total: stats.total,
    today: stats.daily[today] || { total: 0, ml: 0, amazon: 0, shopee: 0 },
    last7,
    byRule: Object.entries(stats.byRule).map(([id, s]) => ({ id, ...s }))
      .sort((a, b) => b.total - a.total)
  });
});

/**
 * Atualiza uma regra existente pelo ID.
 * Permite alterar qualquer campo da regra sem recriar.
 */
app.put('/api/rules/:id', (req, res) => {
  const idx = config.rules.findIndex(r => r.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Regra não encontrada' });
  config.rules[idx] = { ...config.rules[idx], ...req.body, id: config.rules[idx].id };
  saveConfig();
  res.json({ ok: true, item: config.rules[idx] });
});

/**
 * Remove uma regra pelo ID.
 */
app.delete('/api/rules/:id', (req, res) => {
  config.rules = config.rules.filter(r => r.id !== req.params.id);
  delete lastByPlatform[req.params.id];
  saveConfig();
  res.json({ ok: true });
});

/**
 * Envia uma mensagem de teste para o grupo de destino de uma regra.
 * Ignora o modo simulação — sempre envia de verdade para confirmar que o grupo funciona.
 */
app.post('/api/rules/:id/test-send', async (req, res) => {
  const rule = config.rules.find(r => r.id === req.params.id);
  if (!rule) return res.status(404).json({ error: 'Regra não encontrada' });
  try {
    const now = new Date().toLocaleString('pt-BR');
    for (const targetId of getTargetIds(rule)) {
      const targetChat = await client.getChatById(targetId);
      await targetChat.sendMessage(`✅ *Teste WPP Promoções*\nRegra: _${rule.name}_\nEnviado em: ${now}`);
      pushEvent('success', 'Mensagem de teste enviada.', { ruleName: rule.name, targetName: targetChat.name });
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * Busca a última mensagem do grupo de origem da regra e força o processamento.
 * Útil para re-encaminhar a promoção mais recente sem esperar por uma nova mensagem.
 * Respeita o modo simulação.
 */
app.post('/api/rules/:id/replay-last', async (req, res) => {
  const rule = config.rules.find(r => r.id === req.params.id);
  if (!rule) return res.status(404).json({ error: 'Regra não encontrada' });
  try {
    const sourceChat = await client.getChatById(rule.sourceGroupId);
    const messages = await sourceChat.fetchMessages({ limit: 20 });
    // Pega a última mensagem com corpo de texto (ignora mensagens de sistema)
    const lastMsg = messages.reverse().find(m => (m.body || '').trim().length > 0);
    if (!lastMsg) return res.status(404).json({ error: 'Nenhuma mensagem de texto encontrada no grupo de origem.' });

    const body = normalizeText(lastMsg.body);
    const { text: processedBody, swapped: affiliateSwapped } = await replaceAffiliateLinkAsync(body, config.settings);
    const outgoing = (rule.prefix ? `${rule.prefix}\n` : '') + processedBody + (rule.suffix ? `\n${rule.suffix}` : '');

    if (config.settings.simulationMode) {
      pushEvent('simulate', 'Reenvio forçado em modo simulação.', {
        ruleName: rule.name,
        affiliateSwapped,
        hasMedia: lastMsg.hasMedia,
        preview: outgoing.slice(0, 400)
      });
      return res.json({ ok: true, simulated: true, preview: outgoing.slice(0, 400) });
    }

    let media = null;
    if (lastMsg.hasMedia) {
      try { media = await lastMsg.downloadMedia(); } catch {}
    }

    for (const targetId of getTargetIds(rule)) {
      const targetChat = await client.getChatById(targetId);
      if (media) {
        await targetChat.sendMessage(media, { caption: outgoing });
      } else {
        await targetChat.sendMessage(outgoing);
      }
      pushEvent('forward', 'Última mensagem reenviada manualmente.', {
        ruleName: rule.name, targetName: targetChat.name,
        affiliateSwapped, hasMedia: !!media, preview: outgoing.slice(0, 400)
      });
    }
    res.json({ ok: true, simulated: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * Busca a última mensagem com link da plataforma especificada no grupo de origem,
 * aplica os links de afiliado e envia imediatamente para o grupo de destino da regra.
 * Ignora fila e agenda — envio direto ao clicar.
 */
app.post('/api/rules/:id/replay-platform/:platform', async (req, res) => {
  const rule = config.rules.find(r => r.id === req.params.id);
  if (!rule) return res.status(404).json({ error: 'Regra não encontrada' });
  const platform = req.params.platform;
  if (!['ml', 'amazon', 'shopee'].includes(platform)) return res.status(400).json({ error: 'Plataforma inválida' });

  if (status !== 'conectado') return res.status(400).json({ error: 'WhatsApp não está conectado.' });

  try {
    const sourceChat = await client.getChatById(rule.sourceGroupId);
    const messages = await sourceChat.fetchMessages({ limit: 50 });

    // Regex para a plataforma selecionada (sem flag global para evitar problema de lastIndex)
    const urlPatterns = {
      ml:     /https?:\/\/(?:(?:www\.)?(?:mercadolivre\.com\.br|mercadolibre\.com|mliv\.com\.br)|meli\.la)[^\s"'<>\]))]*/i,
      amazon: /https?:\/\/(?:(?:www\.)?amazon\.com\.br|amzn\.to|a\.co)[^\s"'<>\]))]*/i,
      shopee: /https?:\/\/(?:(?:www\.)?shopee\.com\.br|shope\.ee|s\.shopee\.com\.br)[^\s"'<>\]))]*/i
    };
    const urlRe = urlPatterns[platform];

    // Encontra a mensagem mais recente com link da plataforma
    const lastMsg = [...messages].reverse().find(m => urlRe.test(normalizeText(m.body || '')));
    if (!lastMsg) {
      const PLAT_NAME = { ml: 'Mercado Livre', amazon: 'Amazon', shopee: 'Shopee' };
      return res.status(404).json({ error: `Nenhuma promoção de ${PLAT_NAME[platform]} encontrada nas últimas 50 mensagens do grupo de origem.` });
    }

    const body = normalizeText(lastMsg.body);
    const { text: processedBody, swapped: affiliateSwapped } = await replaceAllAffiliateLinks(body, config.settings, [platform]);
    const outgoing = (rule.prefix ? `${rule.prefix}\n` : '') + processedBody + (rule.suffix ? `\n${rule.suffix}` : '');

    if (config.settings.simulationMode) {
      pushEvent('simulate', `Reenvio manual [${platform.toUpperCase()}] em modo simulação.`, {
        ruleName: rule.name, affiliateSwapped, preview: outgoing.slice(0, 400)
      });
      return res.json({ ok: true, simulated: true, preview: outgoing.slice(0, 400) });
    }

    let media = null;
    if (lastMsg.hasMedia) {
      try { media = await lastMsg.downloadMedia(); } catch {}
    }

    for (const targetId of getTargetIds(rule)) {
      const targetChat = await client.getChatById(targetId);
      if (media) {
        await targetChat.sendMessage(media, { caption: outgoing });
      } else {
        await targetChat.sendMessage(outgoing);
      }
      pushEvent('forward', `Última promoção [${platform.toUpperCase()}] reenviada manualmente.`, {
        ruleName: rule.name, targetName: targetChat.name,
        affiliateSwapped, hasMedia: !!media, preview: outgoing.slice(0, 400)
      });
    }
    res.json({ ok: true, simulated: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * Reinicia a sessão do WhatsApp sem apagar os dados de autenticação.
 * Útil quando a conexão trava sem desconectar formalmente.
 */
app.post('/api/restart', async (req, res) => {
  try {
    latestQr = null;
    cachedGroups = [];
    await client.destroy();
    setTimeout(() => client.initialize(), 1500);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * Desconecta o WhatsApp e apaga todos os dados de sessão.
 * Na próxima inicialização, um novo QR Code será gerado.
 */
app.post('/api/logout', async (req, res) => {
  try { await client.logout(); } catch {}
  try { fs.rmSync(SESSION_DIR, { recursive: true, force: true }); } catch {}
  latestQr = null;
  cachedGroups = [];
  status = 'resetado';
  pushEvent('warn', 'Sessão removida. Reinicie o servidor para gerar novo QR Code.');
  res.json({ ok: true });
});

// ─── Socket.io — conexão de novos clientes ────────────────────────────────────

/**
 * Quando um novo browser abre o painel, envia imediatamente o status atual.
 * Se o WhatsApp estiver conectado, envia grupos em cache ou tenta carregar na hora.
 */
io.use((socket, next) => {
  if (isValidToken(socket.handshake.auth?.token)) return next();
  next(new Error('Não autenticado.'));
});

io.on('connection', async (socket) => {
  socket.emit('status', getStatus());
  if (status === 'conectado') {
    if (cachedGroups.length > 0) {
      socket.emit('groups', cachedGroups);
    } else {
      // Cache vazio mas WhatsApp conectado: tenta carregar para este cliente
      try {
        cachedGroups = await loadGroups();
        socket.emit('groups', cachedGroups);
      } catch {
        // Se falhar, o cliente pode usar o botão "↺ Grupos"
      }
    }
  }
});

// ─── Inicialização ────────────────────────────────────────────────────────────

stats = loadStats();

server.listen(PORT, () => {
  console.log(`\n✅ Painel em http://localhost:${PORT}\n`);
  pushEvent('info', `Servidor iniciado na porta ${PORT}.`);
  startScheduleTimer();
});

// Captura erros de inicialização do Puppeteer/Chrome e os exibe nos logs do painel
process.on('unhandledRejection', (reason) => {
  const msg = reason instanceof Error ? reason.message : String(reason);
  console.error('[unhandledRejection]', msg);
  pushEvent('error', 'Erro interno não tratado.', { error: msg });
  if (msg.toLowerCase().includes('chrome') || msg.toLowerCase().includes('puppeteer') || msg.toLowerCase().includes('browser')) {
    status = 'falha_auth';
    io.emit('status', getStatus());
  }
});

client.initialize().catch((err) => {
  console.error('[client.initialize] falhou:', err.message);
  status = 'falha_auth';
  pushEvent('error', 'Falha ao inicializar o Chrome/WhatsApp.', { error: err.message });
  io.emit('status', getStatus());
});
