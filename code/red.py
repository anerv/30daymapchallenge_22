#%%
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt

#%%
# Download first XML file
URL = "https://www.dst.dk/valg/Valg1968094/xml/fintal.xml"

response = requests.get(URL)

with open("../data/feed.xml", "wb") as file:
    file.write(response.content)

# Read file
with open("../data/feed.xml", "r") as f:
    data = f.read()

# Passing the stored data inside the beautifulsoup parser, storing the returned object
bs = BeautifulSoup(data, "html.parser")

#%%
# Getting all links to results
links = []

ops_afs = {}

for afs in bs.findAll("afstemningsomraade"):
    links.append(afs.get("filnavn"))
    ops_afs[afs.get("afstemningsomraade_id")] = afs.get("opstillingskreds_id")

print(f"{len(links)} links and ids for voting areas found!")

#%%
# Get name and ids of opstillingskredse
opstillingskreds = bs.findAll("Opstillingskreds")

ops_dict = {}

for ops in bs.findAll("opstillingskreds"):
    ops_name = ops.contents[0]
    ops_id = ops.get("opstillingskreds_id")

    ops_dict[ops_id] = ops_name

# #%%
# convert to dataframes
ops_df = pd.DataFrame.from_dict(
    ops_dict, orient="index", columns=["opstillingskreds_navn"]
)

ops_afs_df = pd.DataFrame.from_dict(
    ops_afs, orient="index", columns=["opstillingskreds_id"]
)

ops_df["opstillingskreds_id"] = ops_df.index

ops_afs_df["afs_id"] = ops_afs_df.index

ops_afs_joined = ops_afs_df.merge(ops_df, on="opstillingskreds_id", how="left")

ops_afs_joined.set_index("afs_id", inplace=True)

#%%
afs_names = {}
afs_results = {}
afs_stemmeberettigede = {}
afs_ops_ids = {}

for i, l in enumerate(links):

    print(i)
    response = requests.get(l)

    bs = BeautifulSoup(response.content, "html.parser")

    afs_id = bs.select("sted")[0].get("id")
    afs_names[afs_id] = bs.select("sted")[0].contents[0]
    afs_stemmeberettigede[afs_id] = int(bs.select("Stemmeberettigede")[0].contents[0])
    afs

    votes = bs.find("stemmer")
    parties = votes.find_all("parti")

    results = {}

    for p in parties:
        letter = p.get("bogstav")

        results[str(letter) + "_count"] = p.get("stemmerantal")
        results[str(letter) + "_pct"] = p.get("stemmerpct")

    afs_results[afs_id] = results

with open("../data/voting_areas_results.json", "w") as outfile:
    json.dump(afs_results, outfile)

print(f"Results for {len(afs_results)} voting areas downloaded!")
# %%
# convert to df
dataframes = []
for key, values in afs_results.items():
    df = pd.DataFrame.from_dict(afs_results[key], orient="index", columns=[key]).T
    dataframes.append(df)

voting_results_df = pd.concat(dataframes)

names_df = pd.DataFrame.from_dict(afs_names, orient="index", columns=["name"])
voters_df = pd.DataFrame.from_dict(
    afs_stemmeberettigede, orient="index", columns=["voters"]
)

# Merge
voting_results_df = voting_results_df.merge(names_df, left_index=True, right_index=True)
voting_results_df = voting_results_df.merge(
    voters_df, left_index=True, right_index=True
)
voting_results_df = voting_results_df.merge(
    ops_afs_joined, left_index=True, right_index=True
)

voting_results_df.to_csv("../data/voting_results.csv")

#%%
# Read geometries from DAGI
geometries = gpd.read_file("../data/afs_areas.gpkg")

useful_cols = [
    "objectid",
    "id_lokalid",
    "navn",
    "afstemning",
    "afstemni00",
    "kommunekod",
    "kommunelok",
    "opstilling",
    "opstilli00",
    "geometry",
]
geometries = geometries[useful_cols]

#%%
# Read pop and geo data
pop = pd.read_csv("../data/election_data2/Udregning.csv", sep=";")

# read data on voting areas
geo = pd.read_csv("../data/election_data2/geografi.csv", sep=";")

pop_geo = pop.merge(geo, left_on="GeoId", right_on="Valgsted Id")

assert len(pop_geo) == len(geo) == len(pop)

useful_cols = [
    "ValgstedKode",
    "TotalPersoner",
    "GeoId",
    "Kreds Nr_x",
    "KommuneNr",
    "Storkreds Nr",
    "Valgsted navn",
    "Kommune navn",
    "Kreds navn",
    "Storkreds navn",
    "Landsdels navn",
]

pop_geo = pop_geo[useful_cols]

#%%
# Join pop data on geometries
def remove_leading_zero(str):

    if str[0] == "0":

        return str[1:]

    else:
        return str


geometries["muni_id"] = geometries.kommunekod.astype(str)
geometries["muni_id"] = geometries["muni_id"].apply(lambda x: remove_leading_zero(x))

geometries["vote_id"] = "0" + geometries.afstemning.astype(str)

geometries["id"] = geometries.muni_id + geometries.vote_id
geometries["id"] = geometries["id"].astype(int)

geometries = geometries.merge(pop_geo, left_on="id", right_on="GeoId", how="inner")
#%%
# Fix truncated names in voting data
geo_names = geometries.navn.to_list()

voting_names = voting_results_df.name.to_list()

geo_errors = [x for x in geo_names if x not in voting_names]
voting_errors = [x for x in voting_names if x not in geo_names]

geo_errors.sort()
voting_errors.sort()

geo_errors.remove("Lille Næstved Skole - Karrebækvej")

assert len(geo_errors) == len(voting_errors)

for i, v in enumerate(voting_errors):
    ix = voting_results_df.loc[voting_results_df.name == v].index[0]

    voting_results_df.loc[ix, "name"] = geo_errors[i]
#%%
# Fix duplicate name
idx = voting_results_df.loc[voting_results_df.name == "Lille Næstved Skole - Karrebæk"][
    "voters"
].idxmax()
voting_results_df.loc[idx, "name"] = "Lille Næstved Skole - Karrebækvej"

#%%
# Fix new names
ix = geometries.loc[geometries["Kreds navn"] == "6. Utterslev"].index
geometries.loc[ix, "Kreds navn"] = "6. Bispebjerg"
#%%
#%%
# Make join cols
geometries["join_col"] = geometries["Kreds navn"] + "_" + geometries.navn

voting_results_df["join_col"] = (
    voting_results_df.opstillingskreds_navn + "_" + voting_results_df.name
)
#%%
# Merge election results with geometries with pop data
voting_results_with_geoms = voting_results_df.merge(
    geometries, left_on="join_col", right_on="join_col", how="inner"
)
voting_results = gpd.GeoDataFrame(voting_results_with_geoms, geometry="geometry")

# Check results
assert len(voting_results) == len(geometries) == len(voting_results_df)
assert len(voting_results.loc[voting_results.geometry.isna()]) == 0
#%%
voting_results.to_file("../data/voting.gpkg")
# %%
useful_cols = [
    "A_count",
    "A_pct",
    "B_count",
    "B_pct",
    "C_count",
    "C_pct",
    "D_count",
    "D_pct",
    "F_count",
    "F_pct",
    "I_count",
    "I_pct",
    "K_count",
    "K_pct",
    "M_count",
    "M_pct",
    "O_count",
    "O_pct",
    "Q_count",
    "Q_pct",
    "V_count",
    "V_pct",
    "Æ_count",
    "Æ_pct",
    "Ø_count",
    "Ø_pct",
    "Å_count",
    "Å_pct",
    "None_count",
    "None_pct",
    "name",
    "voters",
    "kommunekod",
    "geometry",
    "TotalPersoner",
    "Valgsted navn",
    "Kommune navn",
    "Kreds navn",
    "Storkreds navn",
    "Landsdels navn",
]

voting_results = voting_results[useful_cols]
#%%
numeric_cols = [
    "A_count",
    "A_pct",
    "B_count",
    "B_pct",
    "C_count",
    "C_pct",
    "D_count",
    "D_pct",
    "F_count",
    "F_pct",
    "I_count",
    "I_pct",
    "K_count",
    "K_pct",
    "M_count",
    "M_pct",
    "O_count",
    "O_pct",
    "Q_count",
    "Q_pct",
    "V_count",
    "V_pct",
    "Æ_count",
    "Æ_pct",
    "Ø_count",
    "Ø_pct",
    "Å_count",
    "Å_pct",
    "None_count",
    "None_pct",
    "voters",
    "TotalPersoner",
]

for n in numeric_cols:
    voting_results[n] = voting_results[n].astype(float)
#%%
# Red parties col
voting_results["red"] = (
    voting_results.A_pct
    + voting_results.Ø_pct
    + voting_results.F_pct
    + voting_results.Å_pct
    + voting_results.B_pct
    + voting_results.Q_pct
)

voting_results["blue"] = (
    voting_results.C_pct
    + voting_results.D_pct
    + voting_results.I_pct
    + voting_results.O_pct
    + voting_results.M_pct
    + voting_results.V_pct
    + voting_results.Æ_pct
    + voting_results.K_pct
)
voting_results["total"] = (
    voting_results.red + voting_results.blue + voting_results.None_pct
)
#%%
norm = mpl.colors.Normalize(vmin=-1, vmax=100)
voting_results.plot(column="red", cmap="Reds", legend=True, norm=norm)
# %%
voting_results.to_file("../data/voting.gpkg")
#%%
fig, ax = plt.subplots(figsize=(10, 10))
norm = mpl.colors.Normalize(vmin=-1, vmax=100)
voting_results.plot(ax=ax, column="red", cmap="Reds", legend=True, norm=norm)
ax.set_axis_off()
# %%
##### 
import numpy as np
from shapely.geometry import Point


def Random_Points_in_Polygon(polygon, number):
    points = []
    minx, miny, maxx, maxy = polygon.bounds
    while len(points) < number:
        pnt = Point(np.random.uniform(minx, maxx), np.random.uniform(miny, maxy))
        if polygon.contains(pnt):
            points.append(pnt)
    return points


def Random_Points_in_Bounds(polygon, number):

    gdf_poly = gpd.GeoDataFrame(geometry=[polygon])

    minx, miny, maxx, maxy = polygon.bounds
    x = np.random.uniform(minx, maxx, number)
    y = np.random.uniform(miny, maxy, number)

    df = pd.DataFrame()
    df["points"] = list(zip(x, y))
    df["points"] = df["points"].apply(Point)
    gdf_points = gpd.GeoDataFrame(df, geometry="points")

    sjoin = gpd.tools.sjoin(gdf_points, gdf_poly, predicate="within", how="left")

    return sjoin.geometry.to_list()


#%%
# TODO
# for each geometry in voting_results:
# get total number of votes and geom
# Generate points
# Save point to new point gdf with name/id of afs area
# polys = gpd.GeoDataFrame.from_file("../data/voting.gpkg")
# polys["id"] = polys.index
# polys["red_count"] = (
#     polys.A_count
#     + polys.Ø_count
#     + polys.F_count
#     + polys.Å_count
#     + polys.B_count
#     + polys.Q_count
# )

# polys["blue_count"] = (
#     polys.C_count
#     + polys.D_count
#     + polys.I_count
#     + polys.O_count
#     + polys.M_count
#     + polys.V_count
#     + polys.Æ_count
#     + polys.K_count
# )
# # %%

# all_red_points = []
# all_blue_points = []
# all_other_points = []

# for index, row in polys.iterrows():

#     print(index)
#     voter_count = int(row.voters)

#     poly = row.geometry

#     points = Random_Points_in_Polygon(poly, voter_count)

#     blue_count = int(row.blue_count)
#     red_count = int(row.red_count)
#     other_count = int(voter_count - blue_count - red_count)

#     red_points = Random_Points_in_Polygon(poly, red_count)
#     blue_points = Random_Points_in_Polygon(poly, blue_count)
#     other_points = Random_Points_in_Bounds(poly, other_count)

#     all_red_points.extend(red_points)
#     all_blue_points.extend(blue_points)
#     all_other_points.extend(other_points)

# # %%
# red_gdf = gpd.GeoDataFrame(geometry=all_red_points, crs=polys.crs)
# blue_gdf = gpd.GeoDataFrame(geometry=all_blue_points, crs=polys.crs)
# other_gdf = gpd.GeoDataFrame(geometry=all_other_points, crs=polys.crs)

# # %%
# red_gdf.to_file("../data/red.gpkg")
# blue_gdf.to_file("../data/blue.gpkg")
# other_gdf.to_file("../data/other.gpkg")
# %%
