import type { QueueStatusResponse } from '../types';
import { createPollStore } from './poll-store.svelte';

const QUEUE_INTERVAL_MS = 3000;

export const queue = createPollStore<QueueStatusResponse>({
  name: 'queue',
  url: '/internal/status/queue',
  intervalMs: QUEUE_INTERVAL_MS,
});
