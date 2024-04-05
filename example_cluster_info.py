import pprint

from syndiffix.synthesizer import Synthesizer

from cluster_info import *
from common_tasks import *
from tests.helpers import *

"""
Demonstrates usage of ClusterInfo to extract information about the
clusters in a synthetic dataset (the columns that constitute each cluster).
"""

pp = pprint.PrettyPrinter(indent=4)

# Make an example dataframe
df = get_generic_dataframe_big()

# Synthesize the dataframe
syn = Synthesizer(df)
df_syn = syn.sample()

# Create a ClusterInfo object and get the cluster information
ci = ClusterInfo(syn)
cluster_info = ci.get_cluster_info()
pp.pprint(cluster_info)
