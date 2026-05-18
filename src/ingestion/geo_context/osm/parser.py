from typing import Dict, List


def parse_osm_gdf(gdf) -> List[Dict]:
    if gdf is None or gdf.empty:
        return []

    gdf = gdf.reset_index()

    records = []

    for _, row in gdf.iterrows():
        geometry = row.get("geometry")

        records.append(
            {
                "osm_id": str(row.get("osmid")),
                "element_type": str(row.get("element_type")),
                "name": row.get("name"),
                "amenity": row.get("amenity"),
                "shop": row.get("shop"),
                "public_transport": row.get("public_transport"),
                "lat": geometry.centroid.y if geometry is not None else None,
                "lon": geometry.centroid.x if geometry is not None else None,
                "geometry_wkt": geometry.wkt if geometry is not None else None,
            }
        )

    return records