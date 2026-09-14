import type {
  IncidentSummary,
  MapIncident,
  Incident,
  IncidentTypeStat,
  TimelinePoint
} from '$lib/types';


const API_BASE = 'http://127.0.0.1:8000/api';


async function request<T>(path: string): Promise<T> {

  const response = await fetch(
    `${API_BASE}${path}`
  );

  if (!response.ok) {
    throw new Error(
      `GEOFlux API error: ${response.status}`
    );
  }

  return response.json();
}


export const geoflux = {

  summary():
    Promise<IncidentSummary> {

    return request(
      '/incidents/summary'
    );
  },


  map():
    Promise<MapIncident[]> {

    return request(
      '/incidents/map'
    );
  },


  live():
    Promise<Incident[]> {

    return request(
      '/incidents/live'
    );
  },


  byType():
    Promise<IncidentTypeStat[]> {

    return request(
      '/incidents/by-type'
    );
  },


  timeline():
    Promise<TimelinePoint[]> {

    return request(
      '/incidents/timeline'
    );
  }

};