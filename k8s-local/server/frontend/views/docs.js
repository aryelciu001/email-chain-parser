import { fetchJSON, escHtml } from '../utils.js';

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
    docs.forEach(({ doc_id, count }) => {
      const item = document.createElement('div');
      item.className = 'doc-item';
      item.innerHTML = `
        <div class="doc-subject">${escHtml(doc_id)}</div>
        <div class="doc-meta"><span>${count} email${count !== 1 ? 's' : ''}</span></div>
      `;
      container.appendChild(item);
    });
  } catch (e) {
    subtitle.textContent = '';
    container.innerHTML = `<div class="error">Failed to load docs: ${e.message}</div>`;
  }
}
