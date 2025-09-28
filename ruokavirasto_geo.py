# simple map-data project for CV

# we are going to be reading geological data which is following the OGC WFS standard.

# let's import some libraries first
# geopandas is great for geographical data
import geopandas as gpd
# requests is great for making HTTP requests
import requests
# BytesIO is for saving the data to memory instead of harddrive (temp)
from io import BytesIO

import matplotlib.pyplot as plt

# contextily is a library with which we can draw maps. And I am curious to see the
# data in the geometry column visualized.
import contextily as ctx


url = "https://inspire.ruokavirasto-awsa.com/geoserver/wfs"
params = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetFeature",
    "typeNames": "inspire:LC.LandCoverSurfaces.LPISLandscapeFeature.2024",
    "outputFormat": "application/gml+xml; version=3.2",
    "count": "2000"
}

response = requests.get(url, params=params)

# this stuff was used for figuring out the correct outputFormat
print(response.status_code)
print(response.headers.get("Content-Type"))
print(response.text[:500])

gdf = gpd.read_file(BytesIO(response.content))

# I am really curious about the geometry column after printing the below line
print(gdf.head())

# contextily's background maps are in Web Mercator format (EPSG:3857) so the coordinates from "geometry"
# need to be converted to that.
gdf = gdf.set_crs(epsg=3067)

gdf_webmerc = gdf.to_crs(epsg=3857)

# add_basemap will retrieve the map. But zoom level is obviously something to think about.
# zoomlevels are:
# 0-4: global
# 5-8: country
# 9-12: city/state
# 13-16: street/local
# 17-19: detailed; buildings, real estate

# sounds neat but there is something crucial to consider.
# each zoom level is basically a set of map tiles.
# 0 zoom level is 1 map tile, and loading is fast. it's the map of the world
# 1 zoom level is 4 map tiles
# 2 zoom level is 16 map tiles
# and so on as 2^n * 2^n
# zoom level 13 would be 67 108 864 map tiles, and the below example would be really slow (tried it)

# ctx.add_basemap(ax, zoom=13, source=ctx.providers.OpenStreetMap.Mapnik)
#plt.show()

# so we gotta be smart, and only load the tiles close to the coordinates.

def auto_zoom(minx, miny, maxx, maxy, target_pixels=800):
    """
    Arvioi zoom-taso bounding boxin ja kuvan koon perusteella.
    target_pixels = kuinka leveäksi bbox halutaan pikseleinä
    """
    import math

    # Web Mercator's "tile extent"
    world_merc = 20037508.342789244 * 2
    bbox_width = maxx - minx
    # zoom = log2(tiles)
    zoom = math.log2(world_merc / bbox_width * (target_pixels / 256))

    # zoom can never be higher than 19
    if zoom > 19:
        zoom = 19
    return int(round(zoom))

def map_printer(gdf, row):
    geom = gdf.iloc[[row]]
    # convert to web Mercator
    geom_webmerc = geom.to_crs(epsg=3857)
    # boundaries. kind of a box
    minx, miny, maxx, maxy = geom_webmerc.total_bounds

    width = maxx - minx
    height = maxy - miny

    buffer = 4 * max(width, height)
    # Add buffer to the xy-axis so we get enough information around the polygon
    minx -= buffer
    miny -= buffer
    maxx += buffer
    maxy += buffer

    fig, ax = plt.subplots(figsize=(8, 8))

    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)

    zoom = auto_zoom(minx=minx, miny=miny, maxx=maxx, maxy=maxy, target_pixels=800)

    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=zoom)
    geom_webmerc.plot(ax=ax, alpha=0.8, edgecolor="k")


    plt.show()

map_printer(gdf, 12)






#print(response.headers["Content-Type"])
#print(response.text[:500])