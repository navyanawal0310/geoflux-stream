<script lang="ts">
  import { onMount } from 'svelte';
  import IncidentMap from '$lib/components/IncidentMap.svelte';
  import IncidentFeed from '$lib/components/IncidentFeed.svelte';
  import IncidentAnalytics
    from '$lib/components/IncidentAnalytics.svelte';
  import PipelineHealth
    from '$lib/components/PipelineHealth.svelte';
  type IncidentSummary = {
    total_incidents: number;
    affected_cities: number;
    critical_incidents: number;
    high_incidents: number;
  };

  let summary = $state<IncidentSummary | null>(null);
  let loading = $state(true);
  let error = $state('');

  async function loadSummary() {
    try {
      if (!summary) {
        loading = true;
      }
      error = '';

      console.log('Requesting GEOFlux summary...');

      const response = await fetch(
        'http://127.0.0.1:8000/api/incidents/summary'
      );

      if (!response.ok) {
        throw new Error(
          `API returned HTTP ${response.status}`
        );
      }

      const data: IncidentSummary =
        await response.json();

      console.log(
        'GEOFlux summary received:',
        data
      );

      summary = data;
    } catch (err) {
      console.error(
        'GEOFlux frontend error:',
        err
      );

      error =
        err instanceof Error
          ? err.message
          : 'Unknown error';
    } finally {
      loading = false;
    }
  }

  onMount(() => {
    loadSummary();

    const interval = window.setInterval(
      loadSummary,
      5000
    );

    return () => {
      window.clearInterval(interval);
    };
  });
</script>


<svelte:head>
  <title>GEOFlux</title>
</svelte:head>


<main>

  <header>
    <div>
      <h1>GEOFLUX</h1>

      <p>
        REAL-TIME GLOBAL EVENT INTELLIGENCE
      </p>
    </div>

    {#if !loading && !error}
      <div class="stream-status">
        <span class="dot"></span>
        STREAM ACTIVE
      </div>
    {/if}
  </header>


  {#if !summary && loading}

    <div class="message">
      Connecting to intelligence stream...
    </div>

  {:else if !summary && error}

    <div class="error">
      <strong>
        API CONNECTION FAILED
      </strong>

      <span>
        {error}
      </span>

      <button onclick={loadSummary}>
        RETRY
      </button>
    </div>

  {:else if summary}

    <section class="kpis">

      <article>
        <span>
          TOTAL INCIDENTS
        </span>

        <strong>
          {summary.total_incidents.toLocaleString()}
        </strong>
      </article>


      <article>
        <span>
          AFFECTED CITIES
        </span>

        <strong>
          {summary.affected_cities}
        </strong>
      </article>


      <article>
        <span>
          CRITICAL INCIDENTS
        </span>

        <strong>
          {summary.critical_incidents.toLocaleString()}
        </strong>
      </article>


      <article>
        <span>
          HIGH INCIDENTS
        </span>

        <strong>
          {summary.high_incidents.toLocaleString()}
        </strong>
      </article>

    </section>


    <!-- MAP BELONGS HERE — OUTSIDE SCRIPT -->

    <div class="map-header">

      <div>
        <span>
          GLOBAL INCIDENT MONITOR
        </span>

        <h2>
          Live Event Intelligence
        </h2>
      </div>

      <span class="live">
        ● LIVE
      </span>

    </div>


    <div class="map-panel">
      <IncidentMap />
    </div>
    <IncidentFeed />
    <IncidentAnalytics />
    <PipelineHealth />
  {/if}

</main>


<style>

  :global(*) {
    box-sizing: border-box;
  }


  :global(body) {
    margin: 0;

    min-height: 100vh;

    background: #080c12;

    color: #e8edf5;

    font-family:
      Inter,
      system-ui,
      sans-serif;
  }


  main {
    padding: 42px;
  }


  header {
    display: flex;

    align-items: center;

    justify-content: space-between;
  }


  h1 {
    margin: 0;

    font-size: 28px;

    letter-spacing: 0.34em;
  }


  header p {
    margin-top: 14px;

    font-size: 11px;

    letter-spacing: 0.12em;

    color: #8290a3;
  }


  .stream-status {
    display: flex;

    align-items: center;

    gap: 9px;

    font-size: 11px;

    letter-spacing: 0.12em;

    color: #8fa0b6;
  }


  .dot {
    width: 8px;

    height: 8px;

    border-radius: 50%;

    background: #22c55e;

    box-shadow:
      0 0 10px #22c55e;
  }


  .message {
    margin-top: 30px;

    color: #8290a3;
  }


  .error {
    display: flex;

    flex-direction: column;

    align-items: flex-start;

    gap: 10px;

    margin-top: 30px;

    color: #ef4444;
  }


  button {
    margin-top: 8px;

    padding: 8px 14px;

    border:
      1px solid #334155;

    background: #111827;

    color: #e8edf5;

    cursor: pointer;
  }


  .kpis {
    display: grid;

    grid-template-columns:
      repeat(
        4,
        minmax(0, 1fr)
      );

    gap: 12px;

    margin-top: 42px;
  }


  article {
    padding: 22px;

    background: #0d141e;

    border:
      1px solid #202a36;
  }


  article span {
    display: block;

    font-size: 10px;

    letter-spacing: 0.14em;

    color: #76859a;
  }


  article strong {
    display: block;

    margin-top: 12px;

    font-size: 32px;
  }


  .map-header {
    margin-top: 40px;

    display: flex;

    align-items: flex-end;

    justify-content: space-between;
  }


  .map-header > div > span {
    color: #76859a;

    font-size: 10px;

    letter-spacing: 0.14em;
  }


  .map-header h2 {
    margin: 6px 0 0;

    font-size: 20px;

    font-weight: 500;
  }


  .live {
    color: #22c55e;

    font-size: 10px;

    letter-spacing: 0.15em;
  }


  .map-panel {
    margin-top: 14px;

    border:
      1px solid #202a36;

    overflow: hidden;
  }


  @media (
    max-width: 900px
  ) {

    .kpis {
      grid-template-columns:
        repeat(2, 1fr);
    }

  }

</style>