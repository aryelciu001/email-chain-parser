import { fetchJSON, escHtml, buildDocItem } from '../utils.js';

export async function renderDocs(main) {
  main.innerHTML = `
    <h2>Docs</h2>
    <p class="subtitle" id="subtitle">Loading…</p>
    <div id="docs-container"></div>
  `;
  const subtitle = main.querySelector('#subtitle');
  const container = main.querySelector('#docs-container');
  try {
    const data = await fetchJSON('/api/docs');
    const docs = data.docs || [];
    subtitle.textContent = `${docs.length} doc${docs.length !== 1 ? 's' : ''}`;
    if (!docs.length) {
      container.innerHTML = '<div class="error">No documents found.</div>';
      return;
    }
    docs.forEach(({ doc_id, count }) => container.appendChild(buildDocCard(doc_id, count)));
  } catch (e) {
    subtitle.textContent = '';
    container.innerHTML = `<div class="error">Failed to load docs: ${e.message}</div>`;
  }
}

function buildDocCard(doc_id, count) {
  const card = document.createElement('div');
  card.className = 'thread-card';
  card.innerHTML = `
    <div class="thread-header">
      <div class="thread-title">
        <span class="thread-subject">${escHtml(doc_id)}</span>
      </div>
      <div class="thread-meta">
        <span class="badge">${count} email${count !== 1 ? 's' : ''}</span>
        <span class="chevron">▼</span>
      </div>
    </div>
    <div class="thread-docs"></div>
  `;

  const header = card.querySelector('.thread-header');
  const emails = card.querySelector('.thread-docs');
  let loaded = false;

  header.addEventListener('click', async () => {
    const wasOpen = card.classList.contains('open');
    card.classList.toggle('open');
    if (!wasOpen && !loaded) {
      loaded = true;
      emails.innerHTML = `<div class="loading">Loading…</div>`;
      try {
        const data = await fetchJSON(`/api/emails?doc_id=${encodeURIComponent(doc_id)}`);
        emails.innerHTML = '';
        (data.emails || []).forEach(email => emails.appendChild(buildDocItem(email)));
        if (!data.emails?.length) emails.innerHTML = '<div class="error">No emails found.</div>';
      } catch (e) {
        emails.innerHTML = `<div class="error">Failed: ${e.message}</div>`;
      }
    }
  });

  return card;
}
