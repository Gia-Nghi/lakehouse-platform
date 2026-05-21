# src/ingestion/geo_context/osm/query_builder.py


def build_pois_query_by_bbox(south, west, north, east) -> str:
    return f"""
[out:json][timeout:300];
(
  node["amenity"~"^(school|university|college|kindergarten|hospital|clinic|pharmacy|cafe|restaurant|fast_food|bank|atm|police|fire_station|post_office|marketplace|bus_station)$"]({south},{west},{north},{east});
  way["amenity"~"^(school|university|college|kindergarten|hospital|clinic|pharmacy|cafe|restaurant|fast_food|bank|atm|police|fire_station|post_office|marketplace|bus_station)$"]({south},{west},{north},{east});

  node["shop"~"^(supermarket|convenience|mall)$"]({south},{west},{north},{east});
  way["shop"~"^(supermarket|convenience|mall)$"]({south},{west},{north},{east});

  node["leisure"~"^(park|fitness_centre|sports_centre)$"]({south},{west},{north},{east});
  way["leisure"~"^(park|fitness_centre|sports_centre)$"]({south},{west},{north},{east});
);
out center tags;
""".strip()


def build_roads_query_by_bbox(south, west, north, east) -> str:
    return f"""
[out:json][timeout:300];
(
  way["highway"~"^(motorway|trunk|primary|secondary|tertiary|residential|service|living_street)$"]({south},{west},{north},{east});
);
out geom tags;
""".strip()


def build_transit_stops_query_by_bbox(south, west, north, east) -> str:
    return f"""
[out:json][timeout:300];
(
  node["highway"="bus_stop"]({south},{west},{north},{east});
  node["amenity"="bus_station"]({south},{west},{north},{east});
  node["public_transport"~"^(platform|station|stop_position)$"]({south},{west},{north},{east});
  node["railway"~"^(station|halt|tram_stop)$"]({south},{west},{north},{east});

  way["highway"="bus_stop"]({south},{west},{north},{east});
  way["amenity"="bus_station"]({south},{west},{north},{east});
  way["public_transport"~"^(platform|station|stop_position)$"]({south},{west},{north},{east});
);
out center tags;
""".strip()


def build_railways_query_by_bbox(south, west, north, east) -> str:
    return f"""
[out:json][timeout:300];
(
  way["railway"~"^(rail|subway|light_rail|tram|construction)$"]({south},{west},{north},{east});
);
out geom tags;
""".strip()


def build_administrative_boundaries_query_by_bbox(south, west, north, east) -> str:
    return f"""
[out:json][timeout:300];
(
  relation["boundary"="administrative"]["admin_level"~"^(8|10)$"]({south},{west},{north},{east});
);
out body geom tags;
""".strip()