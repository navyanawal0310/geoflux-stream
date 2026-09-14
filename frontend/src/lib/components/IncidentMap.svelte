<script lang="ts">
  import { onMount } from 'svelte';

  let {
    onCitySelect
  }: {
    onCitySelect?: (city: string) => void;
  } = $props();

  let mapContainer: HTMLDivElement;
  onMount(() => {
    let map: any = null;
    let refreshTimer: number | undefined;
    let destroyed = false;

    async function startMap() {
      const maplibregl = await import('maplibre-gl');

      if (destroyed) {
        return;
      }

      map = new maplibregl.Map({
        container: mapContainer,

        style: {
          version: 8,

          sources: {
            osm: {
              type: 'raster',

              tiles: [
                'https://tile.openstreetmap.org/{z}/{x}/{y}.png'
              ],

              tileSize: 256,

              attribution:
                '© OpenStreetMap contributors'
            }
          },

          layers: [
            {
              id: 'osm-basemap',
              type: 'raster',
              source: 'osm',

              paint: {
                'raster-saturation': -0.65,
                'raster-contrast': 0.15,
                'raster-brightness-max': 0.72
              }
            }
          ]
        },

        center: [20, 18],
        zoom: 1.35,
        minZoom: 1,

        attributionControl: true
      });

      (window as any).__geofluxMap = map;

      map.addControl(
        new maplibregl.NavigationControl({
          showCompass: false
        }),
        'top-right'
      );


      async function loadIncidents() {
        try {
          const response = await fetch(
            'http://127.0.0.1:8000/api/incidents/map'
          );

          if (!response.ok) {
            throw new Error(
              `Map API returned HTTP ${response.status}`
            );
          }

          const incidents =
            await response.json();

          console.log(
            'MAP API RECORDS:',
            incidents.length,
            incidents
          );


          const geojson = {
            type: 'FeatureCollection',

            features: incidents.map(
              (incident: any) => ({
                type: 'Feature',

                geometry: {
                  type: 'Point',

                  coordinates: [
                    Number(incident.longitude),
                    Number(incident.latitude)
                  ]
                },

                properties: {
                  city:
                    incident.city,

                  country:
                    incident.country,

                  region:
                    incident.region,

                  incident_count:
                    Number(
                      incident.incident_count
                    ),

                  critical_count:
                    Number(
                      incident.critical_count
                    ),

                  high_count:
                    Number(
                      incident.high_count
                    ),

                  latest_incident_type:
                    incident.latest_incident_type,

                  latest_severity:
                    incident.latest_severity,

                  latest_incident:
                    incident.latest_incident
                }
              })
            )
          };


          const source =
            map.getSource(
              'geoflux-incidents'
            );


          if (!source) {
            return geojson;
          }


          source.setData(geojson);

          console.log(
            'GEOJSON SENT TO MAP:',
            geojson.features.length,
            geojson
          );


          console.log(
            `GEOFlux map updated: ${incidents.length} cities`
          );

          return geojson;

        } catch (error) {
          console.error(
            'GEOFlux incident map error:',
            error
          );
        }
      }


      /*
       * Everything involving the GeoJSON source
       * happens only after MapLibre finishes loading.
       */
      map.once('load', async () => {
        console.log(
          'GEOFlux geographic basemap loaded'
        );

        map.addSource(
          'geoflux-incidents',
          {
            type: 'geojson',
            data: {
              type: 'FeatureCollection',
              features: []
            }
          }
        );

        /*
         * Soft glow behind each point.
         */
        map.addLayer({
          id: 'incident-glow',

          type: 'circle',

          source: 'geoflux-incidents',

          paint: {
            'circle-radius': 14,

            'circle-color': [
              'match',
              ['get', 'latest_severity'],

              'CRITICAL',
              '#ef4444',

              'HIGH',
              '#f59e0b',

              'MEDIUM',
              '#38bdf8',

              '#94a3b8'
            ],

            'circle-opacity': 0.25,

            'circle-blur': 0.7
          }
        });


        /*
         * Main visible incident point.
         */
        map.addLayer({
          id: 'incident-points',

          type: 'circle',

          source: 'geoflux-incidents',

          paint: {
            'circle-radius': 7,

            'circle-color': [
              'match',
              ['get', 'latest_severity'],

              'CRITICAL',
              '#ef4444',

              'HIGH',
              '#f59e0b',

              'MEDIUM',
              '#38bdf8',

              '#94a3b8'
            ],

            'circle-stroke-color':
              '#ffffff',

            'circle-stroke-width':
              1.5,

            'circle-opacity':
              1
          }
        });

        /*
         * Map interaction.
         * We query a small screen-space box around the cursor instead of
         * adding an invisible circle layer above the visible incident dots.
         * This preserves the original rendering while making points easier
         * to hover and click.
         */
        const findIncidentNearPoint = (point: any) => {
          const padding = 12;

          const box = [
            [point.x - padding, point.y - padding],
            [point.x + padding, point.y + padding]
          ];

          return map.queryRenderedFeatures(
            box,
            { layers: ['incident-points'] }
          )?.[0];
        };


        map.on(
          'mousemove',
          (event: any) => {
            const feature =
              findIncidentNearPoint(event.point);

            map.getCanvas().style.cursor =
              feature ? 'pointer' : '';
          }
        );


        map.on(
          'click',
          (event: any) => {
            const feature =
              findIncidentNearPoint(event.point);

            if (!feature) {
              return;
            }

            const p =
              feature.properties;

            console.log(
              'CITY SELECTED:',
              p?.city
            );

            if (p?.city) {
              onCitySelect?.(
                String(p.city)
              );
            }

            new maplibregl.Popup({
              offset: 16,
              closeButton: false
            })
              .setLngLat(
                event.lngLat
              )
              .setHTML(`
                <div class="geoflux-popup">

                  <div class="popup-city">
                    ${p.city}
                  </div>

                  <div class="popup-country">
                    ${p.country} · ${p.region}
                  </div>

                  <div class="popup-divider"></div>

                  <div class="popup-incident">
                    ${p.latest_incident_type}
                  </div>

                  <div class="popup-grid">

                    <span>Severity</span>
                    <strong>
                      ${p.latest_severity}
                    </strong>

                    <span>Total</span>
                    <strong>
                      ${p.incident_count}
                    </strong>

                    <span>Critical</span>
                    <strong>
                      ${p.critical_count}
                    </strong>

                    <span>High</span>
                    <strong>
                      ${p.high_count}
                    </strong>

                  </div>

                </div>
              `)
              .addTo(map);
          }
        );


        /*
         * Initial data load.
         */
        await loadIncidents();


        /*
         * Subsequent updates only replace GeoJSON data.
         * The map itself is NOT recreated.
         */
        refreshTimer =
          window.setInterval(
            loadIncidents,
            5000
          );
      });


      map.on(
        'error',
        (event: any) => {
          console.error(
            'MapLibre error:',
            event.error
          );
        }
      );
    }


    startMap();


    /*
     * Svelte cleanup.
     */
    return () => {
      destroyed = true;

      if (refreshTimer) {
        window.clearInterval(
          refreshTimer
        );
      }

      if (map) {
        map.remove();
      }
    };
  });
</script>


<svelte:head>
  <link
    href="https://unpkg.com/maplibre-gl@5/dist/maplibre-gl.css"
    rel="stylesheet"
  />
</svelte:head>


<div class="map-wrapper">

  <div class="legend">

    <div>
      <span
        class="legend-dot critical"
      ></span>

      Critical
    </div>

    <div>
      <span
        class="legend-dot high"
      ></span>

      High
    </div>

    <div>
      <span
        class="legend-dot medium"
      ></span>

      Medium
    </div>

  </div>


  <div
    bind:this={mapContainer}
    class="map"
  ></div>

</div>


<style>
  .map-wrapper {
    position: relative;
    width: 100%;
  }


  .map {
    width: 100%;
    height: 560px;

    background:
      #080c12;
  }


  .legend {
    position: absolute;

    z-index: 10;

    left: 16px;
    bottom: 16px;

    display: flex;

    gap: 16px;

    padding:
      9px 12px;

    background:
      rgba(
        8,
        12,
        18,
        0.88
      );

    border:
      1px solid #263241;

    backdrop-filter:
      blur(8px);

    font-size:
      10px;

    letter-spacing:
      0.08em;

    text-transform:
      uppercase;

    color:
      #94a3b8;
  }


  .legend > div {
    display: flex;

    align-items: center;

    gap: 6px;
  }


  .legend-dot {
    display: block;

    width: 7px;
    height: 7px;

    border-radius: 50%;
  }


  .legend-dot.critical {
    background:
      #ef4444;

    box-shadow:
      0 0 8px #ef4444;
  }


  .legend-dot.high {
    background:
      #f59e0b;

    box-shadow:
      0 0 8px #f59e0b;
  }


  .legend-dot.medium {
    background:
      #38bdf8;

    box-shadow:
      0 0 8px #38bdf8;
  }


  :global(
    .maplibregl-popup-content
  ) {
    min-width:
      190px;

    padding:
      14px;

    background:
      #0b1119;

    color:
      #e8edf5;

    border:
      1px solid #334155;

    border-radius:
      3px;

    box-shadow:
      0 8px 30px
      rgba(
        0,
        0,
        0,
        0.45
      );

    font-family:
      Inter,
      system-ui,
      sans-serif;
  }


  :global(
    .maplibregl-popup-tip
  ) {
    border-top-color:
      #0b1119 !important;
  }


  :global(
    .popup-city
  ) {
    font-size:
      15px;

    font-weight:
      600;
  }


  :global(
    .popup-country
  ) {
    margin-top:
      3px;

    color:
      #64748b;

    font-size:
      11px;
  }


  :global(
    .popup-divider
  ) {
    margin:
      10px 0;

    border-top:
      1px solid #263241;
  }


  :global(
    .popup-incident
  ) {
    margin-bottom:
      10px;

    color:
      #cbd5e1;

    font-size:
      11px;

    letter-spacing:
      0.04em;
  }


  :global(
    .popup-grid
  ) {
    display: grid;

    grid-template-columns:
      1fr auto;

    gap:
      6px 16px;

    font-size:
      11px;
  }


  :global(
    .popup-grid span
  ) {
    color:
      #64748b;
  }


  :global(
    .popup-grid strong
  ) {
    font-weight:
      500;

    text-align:
      right;
  }
</style>