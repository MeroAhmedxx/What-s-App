async function loadTranslations(lang) {
  try {
    const res = await fetch(`/static/lang/${lang}.json`, {cache: 'no-store'});
    if (!res.ok) return {};
    return await res.json();
  } catch (e) {
    return {};
  }
}

function walkTextNodes(node, callback) {
  const walker = document.createTreeWalker(node, NodeFilter.SHOW_TEXT, {
    acceptNode(n) {
      if (!n.nodeValue || !n.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
      const p = n.parentElement;
      if (!p) return NodeFilter.FILTER_REJECT;
      if (['SCRIPT', 'STYLE', 'TEXTAREA'].includes(p.tagName)) return NodeFilter.FILTER_REJECT;
      if (p.closest('[data-i18n-skip="1"]')) return NodeFilter.FILTER_REJECT;
      return NodeFilter.FILTER_ACCEPT;
    }
  });
  let current;
  while ((current = walker.nextNode())) callback(current);
}

function translateKey(key, dict) {
  return dict[key] || key;
}

function applyExplicitTranslations(dict) {
  document.querySelectorAll('[data-i18n]').forEach((el) => {
    const key = el.getAttribute('data-i18n');
    if (!key) return;
    if (!el.dataset.i18nOriginal) el.dataset.i18nOriginal = el.textContent;
    el.textContent = translateKey(key, dict);
  });

  document.querySelectorAll('[data-i18n-placeholder]').forEach((el) => {
    const key = el.getAttribute('data-i18n-placeholder');
    if (!key) return;
    el.setAttribute('placeholder', translateKey(key, dict));
  });

  document.querySelectorAll('[data-i18n-title]').forEach((el) => {
    const key = el.getAttribute('data-i18n-title');
    if (!key) return;
    el.setAttribute('title', translateKey(key, dict));
  });

  document.querySelectorAll('[data-i18n-value]').forEach((el) => {
    const key = el.getAttribute('data-i18n-value');
    if (!key) return;
    el.setAttribute('value', translateKey(key, dict));
  });
}

function applyFallbackTranslations(dict) {
  walkTextNodes(document.body, (n) => {
    const original = (n.__origText || n.nodeValue || '').trim();
    if (!original) return;
    if (!n.__origText) n.__origText = original;
    if (dict[original]) {
      n.nodeValue = n.nodeValue.replace(original, dict[original]);
    } else if (original && n.nodeValue.trim() !== original && n.nodeValue.includes(original)) {
      n.nodeValue = n.nodeValue.replace(n.nodeValue.trim(), original);
    }
  });

  document.querySelectorAll('input[placeholder], textarea[placeholder]').forEach((el) => {
    const ph = el.getAttribute('placeholder');
    const original = el.dataset.origPlaceholder || ph;
    if (!el.dataset.origPlaceholder) el.dataset.origPlaceholder = original;
    el.setAttribute('placeholder', translateKey(original, dict));
  });

  document.querySelectorAll('button, option').forEach((el) => {
    const txt = (el.dataset.origText || el.textContent || '').trim();
    if (!txt) return;
    if (!el.dataset.origText) el.dataset.origText = txt;
    el.textContent = translateKey(txt, dict);
  });
}

function applyTranslations(dict, lang) {
  document.documentElement.lang = lang;
  document.documentElement.dir = lang === 'ar' ? 'rtl' : 'ltr';
  document.body.classList.toggle('rtl', lang === 'ar');
  applyExplicitTranslations(dict);
  applyFallbackTranslations(dict);
  const sel = document.getElementById('lang-switcher');
  if (sel) sel.value = lang;
}

async function setLang(lang) {
  localStorage.setItem('altahhan_lang', lang);
  const dict = await loadTranslations(lang);
  applyTranslations(dict, lang);
}

async function initLang() {
  const lang = localStorage.getItem('altahhan_lang') || 'en';
  await setLang(lang);
}

window.addEventListener('DOMContentLoaded', initLang);
