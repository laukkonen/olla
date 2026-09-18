<script lang="ts">
  import { requests } from '../lib/stores/requests.svelte';
  import type { RequestRecord } from '../lib/types';
  import StatusBanner from '../components/StatusBanner.svelte';
  import { fmtAgo, fmtBytes, fmtMs } from '../lib/format';
  import { getNow as liveNow } from '../lib/clock.svelte';

  let filter = $state('');
  const data = $derived(requests.data);
  const now = $derived(liveNow());
  const rows = $derived((data?.requests ?? []).filter((row) => {
    const needle = filter.trim().toLowerCase();
    return !needle || [row.request_id, row.model, row.endpoint, row.backend, row.path, row.user_agent]
      .filter(Boolean).some((value) => value!.toLowerCase().includes(needle));
  }));

  $effect(() => {
    requests.start();
    return () => requests.stop();
  });

  function detail(row: RequestRecord): string {
    return [row.remote_addr, row.user_agent, row.routing_decision, row.routing_reason,
      row.admission_class && `admission:${row.admission_class}`, row.sticky_session && `sticky:${row.sticky_session}`]
      .filter(Boolean).join(' · ');
  }
</script>

<div id="panel-requests" class="panel is-active" role="tabpanel" aria-labelledby="tab-requests" tabindex="0">
  <StatusBanner store={requests} />
  <div class="panel-data" data-state={requests.status === 'error' || requests.status === 'stale' ? requests.status : null}>
    <div class="section-head">
      <h2>Recent requests</h2>
      <span class="section-note">last {data?.capacity ?? 200} · refreshes every 3s</span>
    </div>
    <div class="request-toolbar">
      <label>Filter <input aria-label="Filter requests" bind:value={filter} placeholder="model, endpoint, path, client..." /></label>
      <span class="section-note">{rows.length} shown</span>
    </div>
    <p class="section-note request-limitations">Network-level client IP and user-agent only. Docker NAT may hide the originating container. History is bounded to memory and is cleared on restart.</p>
    <div class="table-scroll request-table">
      <table>
        <thead><tr><th>When</th><th>Status</th><th>Method / path</th><th>Model</th><th>Endpoint / backend</th><th>Duration</th><th>Traffic</th><th>Origin / routing</th></tr></thead>
        <tbody>
          {#each rows as row (row.request_id + row.timestamp)}
            <tr>
              <td title={row.timestamp}>{fmtAgo(row.timestamp, now) || 'now'}</td>
              <td><strong class:request-failed={row.status >= 400}>{row.status}</strong><br /><span class="section-note">{row.state}</span></td>
              <td><strong>{row.method}</strong> <code>{row.path}</code><br /><span class="section-note">{row.request_id}</span></td>
              <td>{row.model || 'unknown'}</td>
              <td>{row.endpoint || 'not selected'}{#if row.backend}<br /><span class="section-note">{row.backend}</span>{/if}</td>
              <td>{fmtMs(row.duration_ms)}</td>
              <td>{fmtBytes(row.request_bytes)} → {fmtBytes(row.response_bytes)}</td>
              <td title={detail(row)}>{row.client_ip || row.remote_addr || 'unknown'}<br /><span class="section-note">{row.routing_strategy || 'routing unavailable'}</span></td>
            </tr>
          {:else}
            <tr><td colspan="8" class="empty-cell">No proxy requests captured yet.</td></tr>
          {/each}
        </tbody>
      </table>
    </div>
  </div>
</div>
