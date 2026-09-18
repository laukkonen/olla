import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { mount, unmount } from 'svelte';
import { pollScheduler } from '../lib/poll-scheduler';
import QueuePanel from './QueuePanel.svelte';

describe('QueuePanel', () => {
  let component: ReturnType<typeof mount>;

  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({
      timestamp: new Date().toISOString(), waiting: 1, oldest_wait_ms: 1200,
      by_class: { interactive: 1 }, by_model: { 'llama3:8b': 1 }, items: [{
        position: 1, model: 'llama3:8b', class: 'interactive', queued_at: new Date().toISOString(),
        waited_ms: 1200, requested_context: 4096,
      }],
    }), { status: 200, headers: { 'Content-Type': 'application/json' } })));
    pollScheduler.start();
  });

  afterEach(() => {
    if (component) unmount(component);
    pollScheduler.stop();
    vi.unstubAllGlobals();
  });

  it('polls independently and renders queue records', async () => {
    component = mount(QueuePanel, { target: document.body });
    await vi.waitFor(() => expect(document.body.textContent).toContain('llama3:8b'));
    expect(pollScheduler.isActive('queue')).toBe(true);
    expect(document.body.textContent).toContain('interactive');
  });
});
