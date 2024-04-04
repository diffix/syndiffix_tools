import pprint

from syndiffix.synthesizer import Synthesizer

from common_tasks import *
from tree_walker import *
from tests.helpers import *

"""
Demonstrates usage of TreeWalker to obtain information about a synthetic
dataset's internal forest and tree structure. Can be useful for various
syndiffix debugging and development purposes.
"""

pp = pprint.PrettyPrinter(indent=4)

# Make an example dataframe
df = get_generic_dataframe()

# Synthesize the dataframe
syn = Synthesizer(df[["str5", "datetime", "int10"]])
df_syn = syn.sample()

# Create a TreeWalker object
tw = TreeWalker(syn)

# Get a dictionary with all trees in the forest
forest_nodes = tw.get_forest_nodes()
pp.pprint(forest_nodes)

# Alternatively, iterate through the forest and trees yourself
for col_id, root in tw.forest_walker():
    # col_id is a tuple indicating which columns the tree is based on
    for node, parent in tw.tree_walker(root):
        # This returns a dict with information about the node
        ni = tw.node_info(node=node, parent=parent)
