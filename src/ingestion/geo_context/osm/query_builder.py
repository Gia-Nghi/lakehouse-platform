# pois
def build_pois_query_by_bbox(south: float, west: float, north: float, east: float) -> str:
    return f"""
[out:json][timeout:180];
(
  node["amenity"~"^(school|hospital|bank|marketplace)$"]({south},{west},{north},{east});
  way["amenity"~"^(school|hospital|bank|marketplace)$"]({south},{west},{north},{east});
  relation["amenity"~"^(school|hospital|bank|marketplace)$"]({south},{west},{north},{east});

  node["shop"="supermarket"]({south},{west},{north},{east});
  way["shop"="supermarket"]({south},{west},{north},{east});
  relation["shop"="supermarket"]({south},{west},{north},{east});
);
out center tags;
""".strip()

# roads
def build_roads_query_by_bbox(south: float, west: float, north: float, east: float) -> str:
    return f"""
[out:json][timeout:180];
(
  way["highway"~"^(primary|secondary|tertiary|residential)$"]({south},{west},{north},{east});
);
out center tags;
""".strip()

# transit stops
def build_transit_stops_query_by_bbox(south: float, west: float, north: float, east: float) -> str:
    return f"""
[out:json][timeout:180];
(
  node["highway"="bus_stop"]({south},{west},{north},{east});
  way["highway"="bus_stop"]({south},{west},{north},{east});
  relation["highway"="bus_stop"]({south},{west},{north},{east});

  node["amenity"="bus_station"]({south},{west},{north},{east});
  way["amenity"="bus_station"]({south},{west},{north},{east});
  relation["amenity"="bus_station"]({south},{west},{north},{east});

  node["public_transport"~"^(platform|station|stop_position)$"]({south},{west},{north},{east});
  way["public_transport"~"^(platform|station|stop_position)$"]({south},{west},{north},{east});
  relation["public_transport"~"^(platform|station|stop_position)$"]({south},{west},{north},{east});

  node["railway"~"^(station|halt|tram_stop)$"]({south},{west},{north},{east});
  way["railway"~"^(station|halt|tram_stop)$"]({south},{west},{north},{east});
  relation["railway"~"^(station|halt|tram_stop)$"]({south},{west},{north},{east});
);
out center tags;
""".strip()

# railways
def build_railways_query_by_bbox(south: float, west: float, north: float, east: float) -> str:
    return f"""
[out:json][timeout:180];
(
  way["railway"~"^(rail|subway|light_rail|tram)$"]({south},{west},{north},{east});
  relation["railway"~"^(rail|subway|light_rail|tram)$"]({south},{west},{north},{east});

  relation["route"~"^(train|subway|light_rail|tram)$"]({south},{west},{north},{east});
);
out center tags;
""".strip()