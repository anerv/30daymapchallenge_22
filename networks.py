#%%
import pyrosm
import pandas as pd
import geopandas as gpd

#%%
osm_fp = "denmark-latest.osm.pbf"

osm = pyrosm.OSM(osm_fp)

extra_attr = [
    "cycleway:left",
    "cycleway:right",
    "cycleway:both",
    "cycleway:width",
    "bicycle_road",
    "proposed",
    "construction",
]

#%%
# Get osm network edges
edges = osm.get_network(
    nodes=False, network_type="cycling", extra_attributes=extra_attr
)

#%%
# Filter out edges with irrelevant highway types
unused_highway_values = [
    "abandoned",
    "planned",
    "proposed",
    "construction",
    "disused",
    "elevator",
    "platform",
    "bus_stop",
    "step",
    "steps",
    "corridor",
    "raceway",
    "bus_guideway",
    "rest_area",
    "razed",
    "layby",
    "skyway",
    "su",
]

org_len = len(edges)
edges = edges.loc[~edges.highway.isin(unused_highway_values)]

new_len = len(edges)

print(f"{org_len - new_len} edges where removed")

#%%
def clean_col_names(df):

    """
    Remove upper-case letters and : from data with OSM tags
    Special characters like ':' can for example break with pd.query function

    Arguments:
        df (df/gdf): dataframe/geodataframe with OSM tag data

    Returns:
        df (df/gdf): the same dataframe with updated column names
    """

    df.columns = df.columns.str.lower()

    df_cols = df.columns.to_list()

    new_cols = [c.replace(":", "_") for c in df_cols]

    df.columns = new_cols

    return df


edges = clean_col_names(edges)

#%%
# Filter

bicycle_infrastructure_queries = {
    "A": "highway == 'cycleway'",
    "B": "cycleway in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing']",
    "C": "cycleway_left in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing']",
    "D": "cycleway_right in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing']",
    "E": "cycleway_both in ['lane','track','opposite_lane','opposite_track','shared_lane','designated','crossing']",
    # "F": "bicycle_street in ['yes','True']",
    "G": "bicycle_road in ['yes','True']",
}

edges["bicycle_infrastructure"] = None

for q in bicycle_infrastructure_queries.values():

    try:
        osm_filtered = edges.query(q)

    except Exception:
        print("Exception occured when quering with:", q)
        print("Please check if the columns used in the query are present in the data")

    edges.loc[osm_filtered.index, "bicycle_infrastructure"] = "yes"


bicycle_edges = edges.loc[edges.bicycle_infrastructure == "yes"].copy()

#%%
# Project
bicycle_edges.to_crs("EPSG:25832", inplace=True)
#%%
bicycle_edges = bicycle_edges[
    [
        "geometry",
        "highway",
        "cycleway",
        "cycleway_left",
        "cycleway_right",
        "cycleway_both",
        "bicycle_road",
    ]
]
#%%
# Export data
bicycle_edges.to_file("bicycle_edges.gpkg")
#%%
