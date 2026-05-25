# Squad Memory: wpp-afiliados

## Estilo de Escrita

## Design Visual

## Estrutura de Conteúdo

## Proibições Explícitas

## Técnico (específico do squad)

- Stack: Node.js + Express + whatsapp-web.js + Socket.io + Vanilla JS. Porta 3080. Config em data/config.json.
- API de afiliados ML: POST https://www.mercadolivre.com.br/affiliate-program/api/v2/affiliates/createLink — body: `{ urls: [...], tag: "..." }` — Cookie: ssid=... — resposta: `urls[0].short_url`
- A API ML rejeita URLs sociais (error_code 111) e links meli.la curtos (error_code 100). Só aceita URLs de produto com MLB-.
- Links meli.la de outros afiliados resolvem via GET (não HEAD) para URLs do tipo `/social/{affiliateId}?...&forceInApp=true&ref=ENCRYPTED`.
- O parâmetro `ref` é criptografado e específico do perfil do afiliado original — copiar o ref para outro perfil causa redirect para /lists.
- Solução: extrair URL do produto (MLB-xxx) da página social para usar na API. Porém a página é um React SPA (recommendations-landings-fe) sem produto no HTML estático.
- `resolveWithBotUA()` — tenta extrair og:url com facebookexternalhit User-Agent. ML não serve OG tags de produto para URLs sociais (só HTML estático de SPA).
- `getProductUrlFromBrowser(socialUrl)` — solução definitiva: usa `client.pupBrowser.newPage()` para abrir a página social no Chrome já ativo do WhatsApp, aguarda o React renderizar (waitForFunction `a[href*="MLB"]`), extrai a URL do produto do DOM. Requer `status === 'conectado'`.
- `searchMeliProduct(query)` — busca produto na API pública `api.mercadolibre.com/sites/MLB/search?q=...`. Fallback quando browser não disponível.
- `extractProductTitle(text)` — extrai título do produto do texto da mensagem (primeira linha sem emoji, preço ou URL).
- Fallback final (`applyAffiliateToResolvedUrl`): troca o social ID e params, remove o ref. Link vai para o perfil root do afiliado (não produto específico) — só ativa se todas as tentativas anteriores falharem.
- Chrome headless: apenas flags seguras — `--no-sandbox`, `--disable-setuid-sandbox`, `--disable-dev-shm-usage`, `--disable-gpu`, `--no-first-run`, `--no-default-browser-check`, `--disable-default-apps`, `--mute-audio`, `--window-size=1280,720`.
- Três tentativas de carga de grupos com guards `status !== 'conectado'` para evitar crash "Target closed".
- `GET /api/queue` retorna `{ count, lastDispatchTime, dispatchIntervalMs, inWindow, scheduleEnabled, items[] }`. Socket `queue` emite `{ count, lastDispatchTime }`.
- Painel de fila com timers: `#queuePanel` aparece quando há itens na fila. `renderQueueTimers()` calcula `nextDispatch = max(lastDispatchTime + intervalMs, now)` e estima horário de cada item pela posição (item N = nextDispatch + N * intervalMs). Countdown atualiza a cada 1s via `setInterval`.
- Regras com padrões de teste ("teste1"…"teste4") nunca capturavam promoções reais. Padrão correto: `matchType: "regex"`, pattern `meli\.la|mercadolivre\.com\.br|mercadolibre\.com|amazon\.com\.br|amzn\.to|a\.co|shopee\.com\.br|shope\.ee`.
