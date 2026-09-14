<script lang="ts">
  import { onMount } from 'svelte';

  let throughput = $state<any>(null);
  let kafka = $state<any>(null);

  let timer: number | undefined;


  async function loadMetrics() {

    try {

      const [
        throughputResponse,
        kafkaResponse
      ] = await Promise.all([
        fetch(
          'http://127.0.0.1:8000/api/system/throughput'
        ),

        fetch(
          'http://127.0.0.1:8000/api/system/kafka-lag'
        )
      ]);


      if (
        !throughputResponse.ok ||
        !kafkaResponse.ok
      ) {
        throw new Error(
          'Stream metrics request failed'
        );
      }


      throughput =
        await throughputResponse.json();

      kafka =
        await kafkaResponse.json();


    } catch (error) {

      console.error(
        'Stream metrics error:',
        error
      );

    }

  }


  function totalKafkaLag() {

    if (!kafka?.consumer_groups) {
      return 0;
    }


    return kafka.consumer_groups
      .reduce(
        (
          total: number,
          group: any
        ) =>
          total +
          Number(
            group.total_lag ?? 0
          ),

        0
      );

  }


  onMount(() => {

    loadMetrics();


    timer =
      window.setInterval(
        loadMetrics,
        5000
      );


    return () => {

      if (timer) {
        window.clearInterval(timer);
      }

    };

  });
</script>


<section class="metrics">

  <div class="header">

    <div>

      <span class="eyebrow">
        STREAM OBSERVABILITY
      </span>

      <h2>
        Runtime Metrics
      </h2>

    </div>

    <span class="live">
      ● LIVE
    </span>

  </div>


  <div class="metric-grid">


    <article>

      <span class="label">
        INCIDENT THROUGHPUT
      </span>

      <strong>
        {
          throughput
            ?.events_per_second
            ?? '—'
        }
      </strong>

      <small>
        incidents / sec
      </small>

    </article>


    <article>

      <span class="label">
        LAST 60 SECONDS
      </span>

      <strong>
        {
          throughput
            ?.events_last_60_seconds
            ?? '—'
        }
      </strong>

      <small>
        processed incidents
      </small>

    </article>


    <article>

      <span class="label">
        KAFKA CONSUMER LAG
      </span>

      <strong>
        {totalKafkaLag()}
      </strong>

      <small>
        messages behind
      </small>

    </article>


    <article>

      <span class="label">
        CONSUMER GROUPS
      </span>

      <strong>
        {
          kafka
            ?.consumer_groups
            ?.length
            ?? '—'
        }
      </strong>

      <small>
        monitored groups
      </small>

    </article>


  </div>


  {#if kafka?.consumer_groups}

    <div class="groups">

      {#each
        kafka.consumer_groups
        as group
      }

        <div class="group">

          <div>

            <span class="group-name">
              {group.group_id}
            </span>

            <small>
              {group.topic}
            </small>

          </div>


          <div class="lag">

            <span>
              LAG
            </span>

            <strong>
              {
                group.total_lag
                ?? 'UNKNOWN'
              }
            </strong>

          </div>

        </div>

      {/each}

    </div>

  {/if}

</section>


<style>
  .metrics {
    margin-top: 34px;
  }


  .header {
    display: flex;

    justify-content:
      space-between;

    align-items:
      flex-end;

    margin-bottom: 14px;
  }


  .eyebrow {
    color: #60a5fa;

    font-size: 10px;

    letter-spacing:
      0.15em;
  }


  h2 {
    margin:
      6px 0 0;

    font-size: 20px;

    font-weight: 500;
  }


  .live {
    color: #22c55e;

    font-size: 10px;

    letter-spacing:
      0.15em;
  }


  .metric-grid {
    display: grid;

    grid-template-columns:
      repeat(
        4,
        minmax(0, 1fr)
      );

    gap: 12px;
  }


  article {
    padding: 18px;

    background:
      #0b1119;

    border:
      1px solid #202a36;
  }


  .label {
    display: block;

    color: #64748b;

    font-size: 9px;

    letter-spacing:
      0.14em;
  }


  article strong {
    display: block;

    margin-top: 10px;

    font-size: 28px;

    font-weight: 600;
  }


  article small {
    display: block;

    margin-top: 5px;

    color: #64748b;
  }


  .groups {
    display: grid;

    grid-template-columns:
      repeat(
        2,
        minmax(0, 1fr)
      );

    gap: 12px;

    margin-top: 12px;
  }


  .group {
    display: flex;

    justify-content:
      space-between;

    align-items:
      center;

    padding: 14px 16px;

    background:
      #0b1119;

    border:
      1px solid #202a36;
  }


  .group-name {
    display: block;

    font-family:
      monospace;

    color:
      #cbd5e1;
  }


  .group small {
    display: block;

    margin-top: 4px;

    color:
      #64748b;
  }


  .lag {
    text-align: right;
  }


  .lag span {
    display: block;

    color:
      #64748b;

    font-size: 9px;

    letter-spacing:
      0.12em;
  }


  .lag strong {
    display: block;

    margin-top: 4px;

    color:
      #22c55e;
  }


  @media (
    max-width: 900px
  ) {

    .metric-grid {
      grid-template-columns:
        repeat(2, 1fr);
    }


    .groups {
      grid-template-columns:
        1fr;
    }

  }
</style>