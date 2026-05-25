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
        <span class="badge">${count} email${count !== 1 ? 's' : ''}</span>
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
        const { roots, children, leaderDups } = buildEmailTree(data.docs || []);
        if (!roots.length) {
          docs.innerHTML = '<div class="error">No documents found.</div>';
        } else {
          renderTree(docs, roots, children, leaderDups, 0);
        }
      } catch (e) {
        docs.innerHTML = `<div class="error">Failed: ${e.message}</div>`;
      }
    }
  });

  return card;
}

// ── Tree building ──────────────────────────────────────────────────────────

function isLeader(doc) {
  return !doc.duplicate && !doc.id?.endsWith('m');
}

function buildEmailTree(docs) {
  const byId = new Map(docs.map(d => [d.id, d]));

  // Step 1+2: build dup -> leader mapping
  const dupToLeader = new Map();

  // 'm'-suffix near-dups: strip trailing 'm's until hitting a leader id
  for (const doc of docs) {
    if (doc.id?.endsWith('m')) {
      let leaderId = doc.id;
      while (leaderId.endsWith('m')) {
        leaderId = leaderId.slice(0, -1);
        if (byId.has(leaderId) && isLeader(byId.get(leaderId))) break;
      }
      dupToLeader.set(doc.id, leaderId);
    }
  }

  // exact dups (duplicate=true): find leader in same canon_order group
  const byCanon = new Map();
  for (const doc of docs) {
    const o = doc.canon_order ?? 0;
    if (!byCanon.has(o)) byCanon.set(o, []);
    byCanon.get(o).push(doc);
  }
  for (const group of byCanon.values()) {
    const leader = group.find(isLeader);
    if (leader) {
      for (const doc of group) {
        if (!isLeader(doc) && !dupToLeader.has(doc.id)) {
          dupToLeader.set(doc.id, leader.id);
        }
      }
    }
  }

  // Step 3: transitively resolve a parent_id to a leader id (or null)
  function resolveParent(parentId) {
    if (!parentId) return null;
    const visited = new Set();
    let cur = parentId;
    while (cur && dupToLeader.has(cur)) {
      if (visited.has(cur)) return null; // cycle guard
      visited.add(cur);
      cur = dupToLeader.get(cur);
    }
    return cur ?? null;
  }

  // Step 4: build tree from leaders only
  const leaders = docs.filter(isLeader);
  const childrenMap = new Map(); // leader id -> [child leaders]
  const roots = [];

  for (const doc of leaders) {
    const resolvedParent = resolveParent(doc.parent_id);
    if (!resolvedParent) {
      roots.push(doc);
    } else {
      if (!childrenMap.has(resolvedParent)) childrenMap.set(resolvedParent, []);
      childrenMap.get(resolvedParent).push(doc);
    }
  }

  // Collect dup doc_ids per leader for "also in:" display
  const leaderDups = new Map(); // leader id -> [doc_ids of dups]
  for (const [dupId, leaderId] of dupToLeader) {
    const dupDoc = byId.get(dupId);
    if (!dupDoc) continue;
    if (!leaderDups.has(leaderId)) leaderDups.set(leaderId, []);
    leaderDups.get(leaderId).push(dupDoc.doc_id);
  }

  return { roots, children: childrenMap, leaderDups };
}

// ── Tree rendering ─────────────────────────────────────────────────────────

function renderTree(container, nodes, childrenMap, leaderDups, depth) {
  for (const doc of nodes) {
    const wrap = document.createElement('div');
    wrap.className = depth === 0 ? 'tree-root' : 'tree-node';

    const nodeEl = document.createElement('div');
    nodeEl.className = 'tree-node-inner';
    nodeEl.appendChild(buildDocItem(doc));

    const others = leaderDups.get(doc.id) || [];
    if (others.length) {
      const list = document.createElement('div');
      list.className = 'dup-list';
      list.innerHTML = `<span class="dup-label">also in:</span> `
        + others.map(id => `<span class="dup-doc-id">${escHtml(id)}</span>`).join('');
      nodeEl.appendChild(list);
    }

    wrap.appendChild(nodeEl);

    const kids = childrenMap.get(doc.id) || [];
    if (kids.length) {
      const childContainer = document.createElement('div');
      childContainer.className = 'tree-children';
      renderTree(childContainer, kids, childrenMap, leaderDups, depth + 1);
      wrap.appendChild(childContainer);

      nodeEl.classList.add('tree-collapsible');
      nodeEl.addEventListener('click', e => {
        // don't interfere with the "Show more/less" button inside doc-item
        if (e.target.closest('.toggle-body')) return;
        const collapsed = childContainer.classList.toggle('tree-collapsed');
        nodeEl.classList.toggle('tree-is-collapsed', collapsed);
      });
    }

    container.appendChild(wrap);
  }
}
