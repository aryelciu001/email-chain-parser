export async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export function escHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

export function buildDocItem(doc) {
  const item = document.createElement('div');
  item.className = 'doc-item';
  const content = (doc.content || '').trim();
  item.innerHTML = `
    <div class="doc-subject">
      ${escHtml(doc.subject || '(no subject)')}
      ${doc.duplicate ? '<span class="duplicate-badge">duplicate</span>' : ''}
    </div>
    <div class="doc-meta">
      ${doc.from      ? `<span>From: ${escHtml(doc.from)}</span>` : ''}
      ${doc.to        ? `<span>To: ${escHtml(doc.to)}</span>` : ''}
      ${doc.date      ? `<span>${escHtml(doc.date)}</span>` : ''}
      ${doc.doc_id    ? `<span>doc: ${escHtml(doc.doc_id)}</span>` : ''}
      ${doc.thread_id ? `<span>thread: <code>${escHtml(doc.thread_id)}</code></span>` : ''}
      ${doc.canon_order != null ? `<span>#${doc.canon_order}</span>` : ''}
    </div>
    ${content ? `<div class="doc-body">${escHtml(content)}</div>
      <button class="toggle-body">Show more</button>` : ''}
  `;
  item.querySelector('.toggle-body')?.addEventListener('click', function () {
    const bodyEl = this.previousElementSibling;
    this.textContent = bodyEl.classList.toggle('expanded') ? 'Show less' : 'Show more';
  });
  return item;
}
