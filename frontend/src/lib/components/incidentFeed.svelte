<script lang="ts">
  import { onMount } from 'svelte';

  type Incident = {
    incident_type: string;
    city: string;
    country: string;
    region: string;
    severity: string;
    event_count: number;
    max_value: number;
    average_value: number;
    detected_at: string;
  };

  let incidents = $state<Incident[]>([]);
  let loading = $state(true);
  let error = $state('');
  let timer: number | undefined;

  async function loadIncidents() {
    try {
      const response = await fetch(
        'http://127.0.0.1:8000/api/incidents/live'
      );

      if (!response.ok) {
        throw new Error(
          `Live API returned HTTP ${response.status}`
        );
      }

      incidents = await response.json();
      error = '';
    } catch (err) {
      console.error('Incident feed error:', err);

      error =
        err instanceof Error
          ? err.message
          : 'Unable to load incidents';
    } finally {
      loading = false;
    }
  }

  function severityClass(severity: string) {
    return severity.toLowerCase();
  }

  function formatTime(timestamp: string) {
    return new Date(timestamp).toLocaleTimeString();
  }

  onMount(() => {
    loadIncidents();

    timer = window.setInterval(
      loadIncidents,
      5000
    );

    return () => {
      if (timer) {
        window.clearInterval(timer);
      }
    };
  });
</script>

<div class="feed">

  <div class="feed-header">
    <div>
      <span class="eyebrow">
        INTELLIGENCE STREAM
      </span>

      <h2>Live Incident Feed</h2>
    </div>

    <span class="status">
      ● LIVE
    </span>
  </div>

  {#if loading}

    <div class="message">
      Loading intelligence stream...
    </div>

  {:else if error}

    <div class="error">
      {error}
    </div>

  {:else}

    <div class="table">

      <div class="table-header">
        <span>TIME</span>
        <span>LOCATION</span>
        <span>INCIDENT</span>
        <span>SEVERITY</span>
        <span>EVENTS</span>
        <span>PEAK</span>
      </div>

      {#each incidents.slice(0, 20) as incident}

        <div class="row">

          <span class="time">
            {formatTime(incident.detected_at)}
          </span>

          <span>
            <strong>{incident.city}</strong>
            <small>{incident.country}</small>
          </span>

          <span class="type">
            {incident.incident_type.replaceAll('_', ' ')}
          </span>

          <span>
            <span
              class="severity {severityClass(incident.severity)}"
            >
              {incident.severity}
            </span>
          </span>

          <span>
            {incident.event_count}
          </span>

          <span>
            {Number(incident.max_value).toFixed(2)}
          </span>

        </div>

      {/each}

    </div>

  {/if}

</div>

<style>
  .feed {
    margin-top: 34px;
  }

  .feed-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-end;

    margin-bottom: 14px;
  }

  .eyebrow {
    font-size: 10px;
    letter-spacing: 0.16em;
    color: #60a5fa;
  }

  h2 {
    margin: 6px 0 0;
    font-size: 20px;
    font-weight: 500;
  }

  .status {
    font-size: 10px;
    letter-spacing: 0.15em;
    color: #22c55e;
  }

  .table {
    border: 1px solid #202a36;
    background: #0b1119;
  }

  .table-header,
  .row {
    display: grid;

    grid-template-columns:
      120px
      1.2fr
      1.5fr
      130px
      100px
      100px;

    gap: 14px;
    align-items: center;
  }

  .table-header {
    padding: 13px 18px;

    border-bottom: 1px solid #202a36;

    font-size: 9px;
    letter-spacing: 0.15em;
    color: #64748b;
  }

  .row {
    padding: 14px 18px;

    border-bottom: 1px solid #17202b;

    font-size: 12px;
  }

  .row:last-child {
    border-bottom: 0;
  }

  .row:hover {
    background: #101923;
  }

  .row strong {
    display: block;
    font-size: 12px;
    font-weight: 500;
  }

  .row small {
    display: block;
    margin-top: 3px;
    color: #64748b;
  }

  .time {
    color: #94a3b8;
    font-family: monospace;
  }

  .type {
    color: #cbd5e1;
  }

  .severity {
    display: inline-block;

    padding: 4px 7px;

    border-radius: 3px;

    font-size: 9px;
    letter-spacing: 0.08em;
  }

  .critical {
    color: #f87171;
    background: rgba(239, 68, 68, 0.1);
    border: 1px solid rgba(239, 68, 68, 0.3);
  }

  .high {
    color: #fbbf24;
    background: rgba(245, 158, 11, 0.1);
    border: 1px solid rgba(245, 158, 11, 0.3);
  }

  .medium {
    color: #38bdf8;
    background: rgba(56, 189, 248, 0.1);
    border: 1px solid rgba(56, 189, 248, 0.3);
  }

  .message {
    color: #94a3b8;
  }

  .error {
    color: #ef4444;
  }
</style>