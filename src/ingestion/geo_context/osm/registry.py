# src/ingestion/geo_context/osm/registry.py

from src.ingestion.geo_context.osm.parser import (
    parse_osm_pois,
    parse_osm_roads,
    parse_osm_transit_stops,
    parse_osm_railways,
    parse_osm_administrative_boundaries,
)

from src.ingestion.geo_context.osm.query_builder import (
    build_pois_query_by_bbox,
    build_roads_query_by_bbox,
    build_transit_stops_query_by_bbox,
    build_railways_query_by_bbox,
    build_administrative_boundaries_query_by_bbox,
)


OSM_ENTITY_REGISTRY = {
    "pois": {
        "entity_type": "pois",
        "query_builder": build_pois_query_by_bbox,
        "parser": parse_osm_pois,
    },
    "roads": {
        "entity_type": "roads",
        "query_builder": build_roads_query_by_bbox,
        "parser": parse_osm_roads,
    },
    "transit_stops": {
        "entity_type": "transit_stops",
        "query_builder": build_transit_stops_query_by_bbox,
        "parser": parse_osm_transit_stops,
    },
    "railways": {
        "entity_type": "railways",
        "query_builder": build_railways_query_by_bbox,
        "parser": parse_osm_railways,
    },
    "administrative_boundaries": {
        "entity_type": "administrative_boundaries",
        "query_builder": build_administrative_boundaries_query_by_bbox,
        "parser": parse_osm_administrative_boundaries,
    },
}