<script lang="ts">
  let {
    city,
    onClose
  }: {
    city: string;
    onClose: () => void;
  } = $props();

  let incidents = $state<any[]>([]);
  let loading = $state(true);
  let error = $state('');

  async function loadCity(currentCity: string) {
    loading = true;
    error = '';

    try {
      const response = await fetch(
        `http://127.0.0.1:8000/api/incidents/city/${encodeURIComponent(currentCity)}`
      );

      if (!response.ok) {
        throw new Error(
          `City API returned HTTP ${response.status}`
        );
      }

      const data = await response.json();

      incidents = data.incidents ?? [];
    } catch (err) {
      console.error(
        'City intelligence error:',
        err
      );

      error =
        err instanceof Error
          ? err.message
          : 'Unable to load city data';
    } finally {
      loading = false;
    }
  }

  $effect(() => {
    loadCity(city);
  });
</script>


<aside class="panel">

  <div class="header">

    <div>
      <span>
        CITY INTELLIGENCE
      </span>

      <h2>
        {city}
      </h2>
    </div>

    <button
      onclick={onClose}
      aria-label="Close city intelligence"
    >
      ×
    </button>

  </div>


  {#if loading}

    <div class="message">
      Loading city intelligence...
    </div>

  {:else if error}

    <div class="error">
      {error}
    </div>

  {:else}

    <div class="summary">

      <div>
        <span>
          INCIDENTS
        </span>

        <strong>
          {incidents.length}
        </strong>
      </div>


      <div>
        <span>
          CRITICAL
        </span>

        <strong>
          {
            incidents.filter(
              (incident) =>
                incident.severity === 'CRITICAL'
            ).length
          }
        </strong>
      </div>


      <div>
        <span>
          HIGH
        </span>

        <strong>
          {
            incidents.filter(
              (incident) =>
                incident.severity === 'HIGH'
            ).length
          }
        </strong>
      </div>

    </div>


    <div class="feed">

      {#each incidents as incident}

        <div class="incident">

          <div class="incident-header">

            <strong>
              {
                incident.incident_type
                  ?.replaceAll('_', ' ')
              }
            </strong>

            <span
              class:critical={
                incident.severity ===
                'CRITICAL'
              }
              class:high={
                incident.severity ===
                'HIGH'
              }
            >
              {incident.severity}
            </span>

          </div>


          <div class="meta">

            <span>
              {incident.sensor_type}
            </span>

            <span>
              events:
              {incident.event_count}
            </span>

            <span>
              max:
              {
                Number(
                  incident.max_value
                ).toFixed(2)
              }
            </span>

          </div>


          <small>
            {
              new Date(
                incident.detected_at
              ).toLocaleString()
            }
          </small>

        </div>

      {/each}


      {#if incidents.length === 0}

        <div class="message">
          No recent incidents found.
        </div>

      {/if}

    </div>

  {/if}

</aside>


<style>
  .panel {
    position: fixed;

    top: 0;
    right: 0;

    z-index: 1000;

    width: min(420px, 100vw);
    height: 100vh;

    overflow-y: auto;

    padding: 24px;

    background: #0b1119;

    border-left:
      1px solid #263241;

    box-shadow:
      -10px 0 40px
      rgba(0, 0, 0, 0.4);
  }


  .header {
    display: flex;

    justify-content:
      space-between;

    align-items:
      flex-start;
  }


  .header span {
    color: #60a5fa;

    font-size: 9px;

    letter-spacing:
      0.15em;
  }


  h2 {
    margin:
      6px 0 0;

    font-size: 24px;
  }


  button {
    border: 0;

    background:
      transparent;

    color:
      #94a3b8;

    font-size:
      28px;

    cursor:
      pointer;
  }


  button:hover {
    color:
      #ffffff;
  }


  .summary {
    display: grid;

    grid-template-columns:
      repeat(3, 1fr);

    gap: 8px;

    margin-top: 28px;
  }


  .summary div {
    padding: 12px;

    background:
      #0d141e;

    border:
      1px solid #202a36;
  }


  .summary span {
    display: block;

    color:
      #64748b;

    font-size: 8px;

    letter-spacing:
      0.12em;
  }


  .summary strong {
    display: block;

    margin-top: 7px;

    font-size: 20px;
  }


  .feed {
    margin-top: 24px;
  }


  .incident {
    padding: 14px 0;

    border-bottom:
      1px solid #202a36;
  }


  .incident-header {
    display: flex;

    justify-content:
      space-between;

    gap: 10px;
  }


  .incident-header strong {
    font-size: 12px;
  }


  .incident-header span {
    font-size: 9px;
  }


  .critical {
    color: #ef4444;
  }


  .high {
    color: #f59e0b;
  }


  .meta {
    display: flex;

    flex-wrap: wrap;

    gap: 12px;

    margin-top: 8px;

    color: #94a3b8;

    font-size: 10px;
  }


  small {
    display: block;

    margin-top: 8px;

    color: #475569;
  }


  .message {
    margin-top: 24px;

    color: #94a3b8;
  }


  .error {
    margin-top: 24px;

    color: #ef4444;
  }
</style>