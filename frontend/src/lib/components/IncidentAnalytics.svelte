<script lang="ts">
  import { onMount } from 'svelte';

  let typeChartEl: HTMLDivElement;
  let timelineChartEl: HTMLDivElement;

  let typeChart: any;
  let timelineChart: any;

  let timer: number | undefined;

  async function loadAnalytics() {
    const [typeRes, timelineRes] =
      await Promise.all([
        fetch(
          'http://127.0.0.1:8000/api/incidents/by-type'
        ),
        fetch(
          'http://127.0.0.1:8000/api/incidents/timeline'
        )
      ]);

    if (!typeRes.ok || !timelineRes.ok) {
      throw new Error(
        'Analytics API request failed'
      );
    }

    const byType =
      await typeRes.json();

    const timeline =
      await timelineRes.json();

    typeChart.setOption({
      tooltip: {
        trigger: 'axis'
      },

      grid: {
        left: 160,
        right: 30,
        top: 25,
        bottom: 25
      },

      xAxis: {
        type: 'value',

        axisLabel: {
          color: '#64748b'
        },

        splitLine: {
          lineStyle: {
            color: '#18212c'
          }
        }
      },

      yAxis: {
        type: 'category',

        data:
          byType.map(
            (row: any) =>
              row.incident_type.replaceAll(
                '_',
                ' '
              )
          ),

        axisLabel: {
          color: '#94a3b8'
        }
      },

      series: [
        {
          type: 'bar',

          data:
            byType.map(
              (row: any) =>
                Number(
                  row.incident_count
                )
            ),

          barWidth: 16
        }
      ]
    });


    timelineChart.setOption({
      tooltip: {
        trigger: 'axis'
      },

      grid: {
        left: 50,
        right: 25,
        top: 25,
        bottom: 40
      },

      xAxis: {
        type: 'category',

        data:
          timeline.map(
            (row: any) =>
              new Date(
                row.timestamp
              ).toLocaleTimeString(
                [],
                {
                  hour: '2-digit',
                  minute: '2-digit'
                }
              )
          ),

        axisLabel: {
          color: '#64748b'
        },

        axisLine: {
          lineStyle: {
            color: '#263241'
          }
        }
      },

      yAxis: {
        type: 'value',

        axisLabel: {
          color: '#64748b'
        },

        splitLine: {
          lineStyle: {
            color: '#18212c'
          }
        }
      },

      series: [
        {
          type: 'line',

          smooth: true,

          showSymbol: false,

          data:
            timeline.map(
              (row: any) =>
                Number(
                  row.incident_count
                )
            )
        }
      ]
    });
  }


  onMount(() => {
    let destroyed = false;

    async function start() {
      const echarts =
        await import('echarts');

      if (destroyed) {
        return;
      }

      typeChart =
        echarts.init(typeChartEl);

      timelineChart =
        echarts.init(
          timelineChartEl
        );

      await loadAnalytics();

      timer =
        window.setInterval(
          loadAnalytics,
          5000
        );

      const resize = () => {
        typeChart?.resize();
        timelineChart?.resize();
      };

      window.addEventListener(
        'resize',
        resize
      );

      return resize;
    }

    let resizeHandler:
      (() => void) | undefined;

    start().then(
      handler => {
        resizeHandler = handler;
      }
    );

    return () => {
      destroyed = true;

      if (timer) {
        window.clearInterval(timer);
      }

      if (resizeHandler) {
        window.removeEventListener(
          'resize',
          resizeHandler
        );
      }

      typeChart?.dispose();
      timelineChart?.dispose();
    };
  });
</script>


<section class="analytics">

  <div class="analytics-header">
    <div>
      <span>
        INCIDENT ANALYTICS
      </span>

      <h2>
        Streaming Intelligence
      </h2>
    </div>

    <span class="live">
      ● LIVE
    </span>
  </div>


  <div class="analytics-grid">

    <article>
      <div class="panel-title">
        INCIDENT DISTRIBUTION
      </div>

      <div
        bind:this={typeChartEl}
        class="chart"
      ></div>
    </article>


    <article>
      <div class="panel-title">
        INCIDENT ACTIVITY — 60 MIN
      </div>

      <div
        bind:this={timelineChartEl}
        class="chart"
      ></div>
    </article>

  </div>

</section>


<style>
  .analytics {
    margin-top: 34px;
  }

  .analytics-header {
    display: flex;

    justify-content:
      space-between;

    align-items:
      flex-end;

    margin-bottom: 14px;
  }

  .analytics-header span {
    font-size: 10px;

    letter-spacing:
      0.15em;

    color: #60a5fa;
  }

  h2 {
    margin: 6px 0 0;

    font-size: 20px;

    font-weight: 500;
  }

  .live {
    color: #22c55e !important;
  }

  .analytics-grid {
    display: grid;

    grid-template-columns:
      1fr 1fr;

    gap: 14px;
  }

  article {
    background:
      #0b1119;

    border:
      1px solid #202a36;
  }

  .panel-title {
    padding:
      13px 16px;

    border-bottom:
      1px solid #202a36;

    color: #64748b;

    font-size: 9px;

    letter-spacing:
      0.15em;
  }

  .chart {
    width: 100%;

    height: 320px;
  }

  @media (
    max-width: 1000px
  ) {
    .analytics-grid {
      grid-template-columns:
        1fr;
    }
  }
</style>