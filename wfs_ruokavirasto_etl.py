# ETL project for Ruokavirasto Map-data.
# I am downloading stuff that is within 50km of the city center of Nokia (approximate)
# however, because the function chunk_reader asks for x and y (coordinates)
# any point on the globe will suffice. Just don't expect any results if the point is not
# in Finland

import geopandas as gpd
import pandas as pd
# requests is great for making HTTP requests
import requests
# BytesIO is for saving the data to memory instead of harddrive (temp)
from io import BytesIO
from wfs_ruokavirasto_table_check import layer_exists, get_wfs_typenames

import pyodbc
from sqlalchemy import create_engine

#import matplotlib.pyplot as plt
from shapely.geometry import Point

# contextily is a library with which we can draw maps. And I am curious to see the
# data in the geometry column visualized.
#import contextily as ctx


def wfs_chunk_reader(url, x, y, startindex=None, chunk_size=None):
    # let's give a default chunk_size if type is None or type is not int
    if chunk_size is None or not isinstance(chunk_size, int):
        chunk_size = 2000
    # same for startIndex on the WFS parameter
    if startindex is None or not isinstance(startindex, int):
        startindex = 0
    

    all_gdfs = []

    # starting year when the data was being collected in this form. no stuff before this afaik
    year = 2020
    typename = f"inspire:LC.LandCoverSurfaces.LPISLandscapeFeature.{year}"

    while year < 2025:
        print("loop number: " + str(year))
        startindex = 0

        if not layer_exists(url, typename):
            print(f"Layer {typename} ei löytynyt — hypätään yli.")
            continue

        while True:
            for version in ["2.0.0", "1.1.0"]:
                params = {
                    "service": "WFS",
                    "version": version,
                    "request": "GetFeature",
                    "typeNames": f"inspire:LC.LandCoverSurfaces.LPISLandscapeFeature.{year}",
                    "outputFormat": "application/gml+xml; version=3.2",
                    "startIndex": str(startindex),
                    "count": str(chunk_size)
                }

            response = requests.get(url, params)
            #
            print(response.status_code)
            print(response.headers.get("Content-Type"))
            print(response.text[:500])
            if not response.ok:
                print(f"HTTP error: {response.status_code}")
                break

            content_type = response.headers.get("Content-Type", "").lower()
            print(f"Content-Type: {content_type}")

            if not content_type.startswith("application/gml") and not content_type.startswith("text/xml"):
                print("⚠️ Response is not GML/XML. Printing first 500 chars:")
                print(response.text[:500])
                break

            #
            gdf = gpd.read_file(BytesIO(response.content))

            if gdf.empty:
                break
            if gdf.crs is None:
                gdf = gdf.set_crs(epsg=3067)
        
            gdf = gdf.to_crs(epsg=3067)

            filter_point = Point(x, y)

            gdf["distance_to_point"] = gdf.distance(filter_point)

            gdf = gdf[gdf["distance_to_point"] <= 1000000000]

            gdf["year_added"] = year

            all_gdfs.append(gdf)
            startindex += chunk_size
        
        year += 1

    username = "etl_user"
    password = "VahvaSalasana!2025"
    server = "localhost\\SQLEXPRESS"
    server = "ISINESKAPEDESK\\SIURONPARONISQL"
    database = "GeoDataETL_tests"
    engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    )


    if all_gdfs:
        gdf_to_sql = pd.concat(all_gdfs, ignore_index=True)
        gdf_to_sql['geometry'] = gdf.geometry.apply(lambda g: g.wkt)
        
        gdf_to_sql.to_sql(
            name='MaatalousmaisemaPiirre_raw',
            con=engine,
            schema='staging',
            if_exists='replace', # I'm frequently running this with small test data, so not using append
            index=False,
            chunksize=500
        )

        return gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True), crs=gdf.crs)
    else:
        return gpd.GeoDataFrame



print("GO AGAIN")
print("---")


url = "https://inspire.ruokavirasto-awsa.com/geoserver/wfs"

# nokia_point = Point(324769, 6820284)
x = 324769
y = 6820284
# unfortunately there was only 1 result withing 90km of this chosen point, so I changed the distance filter inside the function
# to 1000000000 meters


print(get_wfs_typenames(url))

gdf = wfs_chunk_reader(url=url, x=x, y=y, startindex=0, chunk_size=2000)

print(gdf.head())
print()
print(gdf.shape)
print()
print(gdf.info())
print()
print(gdf.crs)
print("---")