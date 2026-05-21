# src/ingestion/geo_context/osm/parser.py

from typing import Any, Dict, List


def _get_lat_lon(element: Dict[str, Any]):
    if "lat" in element and "lon" in element:
        return element.get("lat"), element.get("lon")

    center = element.get("center") or {}
    return center.get("lat"), center.get("lon")


def _geometry_linestring(element: Dict[str, Any]):
    geometry = element.get("geometry")

    if not geometry:
        return None

    return {
        "type": "LineString",
        "coordinates": [
            [point.get("lon"), point.get("lat")]
            for point in geometry
            if point.get("lat") is not None and point.get("lon") is not None
        ],
    }


def _geometry_polygon_from_relation(element: Dict[str, Any]):
    members = element.get("members") or []

    rings = []

    for member in members:
        geometry = member.get("geometry")

        if not geometry:
            continue

        ring = [
            [point.get("lon"), point.get("lat")]
            for point in geometry
            if point.get("lat") is not None and point.get("lon") is not None
        ]

        if len(ring) >= 4:
            if ring[0] != ring[-1]:
                ring.append(ring[0])

            rings.append(ring)

    if not rings:
        return None

    return {
        "type": "MultiPolygon",
        "coordinates": [
            [ring]
            for ring in rings
        ],
    }


def _base_record(element: Dict[str, Any], entity_type: str) -> Dict[str, Any]:
    tags = element.get("tags") or {}
    lat, lon = _get_lat_lon(element)

    return {
        "osm_id": str(element.get("id")),
        "osm_type": element.get("type"),
        "entity_type": entity_type,
        "name": tags.get("name"),
        "name_en": tags.get("name:en"),
        "lat": lat,
        "lon": lon,
        "tags": tags,
    }


def parse_osm_pois(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []

    for element in elements:
        record = _base_record(element, "pois")
        tags = record["tags"]

        record.update(
            {
                "geometry_type": "Point",
                "geometry_geojson": {
                    "type": "Point",
                    "coordinates": [record["lon"], record["lat"]],
                } if record["lat"] is not None and record["lon"] is not None else None,
                "amenity": tags.get("amenity"),
                "shop": tags.get("shop"),
                "leisure": tags.get("leisure"),
            }
        )

        records.append(record)

    return records


def parse_osm_roads(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []

    for element in elements:
        record = _base_record(element, "roads")
        tags = record["tags"]

        record.update(
            {
                "geometry_type": "LineString",
                "geometry_geojson": _geometry_linestring(element),
                "highway": tags.get("highway"),
                "surface": tags.get("surface"),
                "lanes": tags.get("lanes"),
                "oneway": tags.get("oneway"),
                "maxspeed": tags.get("maxspeed"),
            }
        )

        records.append(record)

    return records


def parse_osm_transit_stops(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []

    for element in elements:
        record = _base_record(element, "transit_stops")
        tags = record["tags"]

        record.update(
            {
                "geometry_type": "Point",
                "geometry_geojson": {
                    "type": "Point",
                    "coordinates": [record["lon"], record["lat"]],
                } if record["lat"] is not None and record["lon"] is not None else None,
                "public_transport": tags.get("public_transport"),
                "highway": tags.get("highway"),
                "railway": tags.get("railway"),
                "amenity": tags.get("amenity"),
            }
        )

        records.append(record)

    return records


def parse_osm_railways(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []

    for element in elements:
        record = _base_record(element, "railways")
        tags = record["tags"]

        record.update(
            {
                "geometry_type": "LineString",
                "geometry_geojson": _geometry_linestring(element),
                "railway": tags.get("railway"),
                "route": tags.get("route"),
                "operator": tags.get("operator"),
            }
        )

        records.append(record)

    return records


def parse_osm_administrative_boundaries(elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []

    for element in elements:
        record = _base_record(element, "administrative_boundaries")
        tags = record["tags"]

        record.update(
            {
                "geometry_type": "MultiPolygon",
                "geometry_geojson": _geometry_polygon_from_relation(element),
                "boundary": tags.get("boundary"),
                "admin_level": tags.get("admin_level"),
            }
        )

        records.append(record)

    return records