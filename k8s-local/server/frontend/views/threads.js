import { fetchJSON, escHtml, buildDocItem } from '../utils.js';

export async function renderThreads(main) {
  main.innerHTML = `
    <h2>Threads</h2>
    <p class="subtitle" id="subtitle">Loading…</p>
    <div id="threads-container"></div>
  `;
  const subtitle = main.querySelector('#subtitle');
  const container = main.querySelector('#threads-container');
  try {
    const data = await fetchJSON('/api/threads');
    const threads = data.threads || [];
    subtitle.textContent = `${threads.length} thread${threads.length !== 1 ? 's' : ''}`;
    threads.forEach(t => container.appendChild(buildThreadCard(t)));
  } catch (e) {
    subtitle.textContent = '';
    container.innerHTML = `<div class="error">Failed to load threads: ${e.message}</div>`;
  }
}

function buildThreadCard({ thread_id, count, subject }) {
  const card = document.createElement('div');
  card.className = 'thread-card';
  card.innerHTML = `
    <div class="thread-header">
      <div class="thread-title">
        <span class="thread-id">${escHtml(thread_id)}</span>
        ${subject ? `<span class="thread-subject">${escHtml(subject)}</span>` : ''}
      </div>
      <div class="thread-meta">
        <span class="badge">${count} doc${count !== 1 ? 's' : ''}</span>
        <span class="chevron">▼</span>
      </div>
    </div>
    <div class="thread-docs"></div>
  `;

  const header = card.querySelector('.thread-header');
  const docs = card.querySelector('.thread-docs');
  let loaded = false;

  header.addEventListener('click', async () => {
    const wasOpen = card.classList.contains('open');
    card.classList.toggle('open');
    if (!wasOpen && !loaded) {
      loaded = true;
      docs.innerHTML = `<div class="loading">Loading…</div>`;
      try {
        const data = await fetchJSON(`/api/threads?thread_id=${encodeURIComponent(thread_id)}`);
        docs.innerHTML = '';
        (data.docs || []).forEach(doc => docs.appendChild(buildDocItem(doc)));
        if (!data.docs?.length) docs.innerHTML = '<div class="error">No documents found.</div>';
      } catch (e) {
        docs.innerHTML = `<div class="error">Failed: ${e.message}</div>`;
      }
    }
  });

  return card;
}
