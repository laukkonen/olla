import type { RequestHistoryResponse } from '../types';
import { createPollStore } from './poll-store.svelte';

export const requests = createPollStore<RequestHistoryResponse>({
  name: 'requests',
  url: '/internal/ui/api/requests',
  intervalMs: 3000,
});
