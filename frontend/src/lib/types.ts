export interface IncidentSummary {
  total_incidents: number;
  affected_cities: number;
  critical_incidents: number;
  high_incidents: number;
}

export interface MapIncident {
  city: string;
  country: string;
  region: string;

  latitude: number;
  longitude: number;

  incident_count: number;

  critical_count: number;
  high_count: number;

  latest_incident: string;
  latest_incident_type: string;
  latest_severity: string;
}

export interface Incident {
  incident_type: string;

  city: string;
  country: string;
  region: string;

  latitude: number;
  longitude: number;

  sensor_type: string;
  severity: string;

  event_count: number;

  max_value: number;
  min_value: number;
  average_value: number;

  window_start: string;
  window_end: string;

  detected_at: string;
}

export interface IncidentTypeStat {
  incident_type: string;
  incident_count: number;
}

export interface TimelinePoint {
  timestamp: string;
  incident_count: number;
}