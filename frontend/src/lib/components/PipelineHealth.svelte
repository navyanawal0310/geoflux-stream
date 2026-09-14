<script lang="ts">
  import { onMount } from 'svelte';

  let health = $state<any>(null);
  let timer: number | undefined;

  async function loadHealth() {
    try {
      const response = await fetch(
        'http://127.0.0.1:8000/api/system/health'
      );

      health =
        await response.json();

    } catch (error) {

      console.error(
        'Pipeline health error:',
        error
      );

    }
  }


  onMount(() => {

    loadHealth();

    timer =
      window.setInterval(
        loadHealth,
        5000
      );


    return () => {

      if (timer) {
        window.clearInterval(
          timer
        );
      }

    };

  });


  function statusClass(
    status: string
  ) {

    return status === 'UP'
      ? 'up'
      : 'down';

  }
</script>


<section class="pipeline">

  <div class="header">

    <div>

      <span>
        SYSTEM OBSERVABILITY
      </span>

      <h2>
        Pipeline Health
      </h2>

    </div>

    <span class="monitoring">
      ● MONITORING
    </span>

  </div>


  {#if health}

    <div class="flow">


      <div class="service">

        <span class="name">
          SENSOR NETWORK
        </span>

        <strong>
          Simulator
        </strong>

        <span class="status up">
          ● ACTIVE
        </span>

      </div>


      <div class="arrow">
        →
      </div>


      <div class="service">

        <span class="name">
          MESSAGE BUS
        </span>

        <strong>
          Apache Kafka
        </strong>

        <span
          class="status {statusClass(
            health.kafka?.status
          )}"
        >
          ● {health.kafka?.status}
        </span>

      </div>


      <div class="arrow">
        →
      </div>


      <div class="service">

        <span class="name">
          STREAM PROCESSING
        </span>

        <strong>
          Apache Flink
        </strong>

        <span
          class="status {statusClass(
            health.flink?.status
          )}"
        >
          ● {health.flink?.status}
        </span>

        <small>

          {
            health.flink
              ?.jobs_running ?? 0
          }
          jobs

        </small>

      </div>


      <div class="arrow">
        →
      </div>


      <div class="service">

        <span class="name">
          ANALYTICAL STORE
        </span>

        <strong>
          ClickHouse
        </strong>

        <span
          class="status {statusClass(
            health.clickhouse?.status
          )}"
        >
          ● {health.clickhouse?.status}
        </span>

      </div>


      <div class="arrow">
        →
      </div>


      <div class="service">

        <span class="name">
          API
        </span>

        <strong>
          FastAPI
        </strong>

        <span class="status up">
          ● UP
        </span>

      </div>


    </div>

  {/if}

</section>


<style>

  .pipeline {
    margin-top: 34px;
  }


  .header {
    display: flex;

    justify-content:
      space-between;

    align-items:
      flex-end;

    margin-bottom:
      14px;
  }


  .header > div > span {
    color: #60a5fa;

    font-size: 10px;

    letter-spacing:
      0.15em;
  }


  h2 {
    margin:
      6px 0 0;

    font-size:
      20px;

    font-weight:
      500;
  }


  .monitoring {
    color:
      #22c55e;

    font-size:
      10px;

    letter-spacing:
      0.15em;
  }


  .flow {
    display: grid;

    grid-template-columns:
      1fr auto
      1fr auto
      1fr auto
      1fr auto
      1fr;

    align-items:
      center;

    gap: 10px;

    padding: 20px;

    background:
      #0b1119;

    border:
      1px solid #202a36;
  }


  .service {
    min-height:
      110px;

    display: flex;

    flex-direction:
      column;

    justify-content:
      center;

    padding: 16px;

    background:
      #0d141e;

    border:
      1px solid #202a36;
  }


  .name {
    font-size:
      9px;

    letter-spacing:
      0.12em;

    color:
      #64748b;
  }


  strong {
    margin-top:
      8px;

    font-size:
      14px;
  }


  .status {
    margin-top:
      10px;

    font-size:
      10px;

    letter-spacing:
      0.08em;
  }


  .up {
    color:
      #22c55e;
  }


  .down {
    color:
      #ef4444;
  }


  small {
    margin-top:
      5px;

    color:
      #64748b;
  }


  .arrow {
    color:
      #475569;

    font-size:
      20px;
  }

</style>