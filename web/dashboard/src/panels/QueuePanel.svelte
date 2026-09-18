<script lang="ts">
  import { queue } from '../lib/stores/queue.svelte';
  import StatusBanner from '../components/StatusBanner.svelte';
  import { fmtAgo, fmtDuration, fmtMs, fmtUntil } from '../lib/format';
  import { getNow as liveNow } from '../lib/clock.svelte';

  const loading = $derived(queue.status === 'loading');
  const data = $derived(queue.data);
  const items = $derived(data?.items ?? []);
  const now = $derived(liveNow());

  $effect(() => {
    queue.start();
    return () => queue.stop();
  });

  function entries(values: Record<string, number> | undefined): [string, number][] {
    return Object.entries(values ?? {}).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  }
</script>

<div id="panel-queue" class="panel is-active" role="tabpanel" aria-labelledby="tab-queue" tabindex="0">
  <StatusBanner store={queue} />
  <div class="panel-data" data-state={queue.status === 'error' || queue.status === 'stale' ? queue.status : null}>
    <div class="section-head">
      <h2>Admission queue</h2>
      <span class="section-note">read-only · refreshes every 3s</span>
    </div>
    {#if loading}
      <div class="table-scroll"><div class="scroll-hint">loading...</div>
        {#each Array(4) as _}<div class="skeleton row-skel" style="margin:6px 10px"></div>{/each}
      </div>
    {:else}
      <div class="tile-grid queue-summary">
        <div class="tile"><span class="label">Waiting</span><span class="value">{data?.waiting ?? 0}</span><span class="sub">requests in admission</span></div>
        <div class="tile"><span class="label">Oldest wait</span><span class="value">{fmtMs(data?.oldest_wait_ms ?? 0)}</span><span class="sub">current snapshot</span></div>
        <div class="tile"><span class="label">Classes</span><span class="value">{entries(data?.by_class).length}</span><span class="sub">active queue bands</span></div>
        <div class="tile"><span class="label">Models</span><span class="value">{entries(data?.by_model).length}</span><span class="sub">requested models</span></div>
      </div>

      {#if data?.waiting}
        <div class="queue-breakdown">
          {#each entries(data?.by_class) as [name, count]}<span class="pill">{name} <strong>{count}</strong></span>{/each}
          {#each entries(data?.by_model) as [name, count]}<span class="pill pill-muted">{name} <strong>{count}</strong></span>{/each}
        </div>
      {/if}

      <div class="table-scroll queue-table">
        <table>
          <thead><tr><th>Pos</th><th>Model</th><th>Class</th><th>Waited</th><th>Queued</th><th>Context</th><th>Deadline</th></tr></thead>
          <tbody>
            {#each items as item (item.request_id ?? `${item.position}-${item.queued_at}`)}
              <tr>
                <td>{item.position}</td>
                <td><strong>{item.model || 'unknown'}</strong></td>
                <td>{item.class}</td>
                <td>{fmtMs(item.waited_ms)}</td>
                <td>{fmtAgo(item.queued_at, now) || '—'}</td>
                <td>{item.requested_context ? item.requested_context.toLocaleString('en-AU') : 'default'}</td>
                <td>{item.deadline ? `${fmtUntil(item.deadline, now) || 'now'}${item.timeout_ms ? ` · ${fmtDuration(item.timeout_ms)} timeout` : ''}` : (item.timeout_ms ? `${fmtDuration(item.timeout_ms)} timeout` : '—')}</td>
              </tr>
            {:else}
              <tr><td colspan="7" class="empty-cell">No requests are waiting.</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>
</div>
