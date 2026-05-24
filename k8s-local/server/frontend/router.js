import { renderThreads } from './views/threads.js';
import { renderDocs }   from './views/docs.js';

const routes = {
  '/threads': renderThreads,
  '/docs':    renderDocs,
};

const main = document.getElementById('main');

function navigate(path, push = true) {
  if (push && location.pathname !== path) history.pushState(null, '', path);
  document.querySelectorAll('nav a').forEach(a => {
    a.classList.toggle('active', a.getAttribute('href') === path);
  });
  (routes[path] ?? renderThreads)(main);
}

document.querySelectorAll('nav a').forEach(a => {
  a.addEventListener('click', e => {
    e.preventDefault();
    navigate(a.getAttribute('href'));
  });
});

window.addEventListener('popstate', () => navigate(location.pathname, false));

const initial = location.pathname === '/' ? '/threads' : location.pathname;
navigate(initial, initial !== location.pathname);
